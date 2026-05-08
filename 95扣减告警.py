from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import pandas as pd
from openpyxl import Workbook
import time
import json
import os

def connect_to_es(es_url, username=None, password=None):
    """
    连接到Elasticsearch服务器
    
    参数:
        es_url: Elasticsearch服务器的URL
        username: 用户名（可选）
        password: 密码（可选）
        
    返回:
        Elasticsearch客户端实例，连接失败则返回None
    """
    try:
        if username and password:
            es = Elasticsearch(es_url, basic_auth=(username, password))
        else:
            es = Elasticsearch(es_url)
        
        # 测试连接
        if es.ping():
            print("✅ 成功连接到Elasticsearch")
            return es
        else:
            print("❌ 无法连接到Elasticsearch")
            return None
    except Exception as e:
        print(f"❌ 连接Elasticsearch失败: {e}")
        return None

def _parse_index_date(index):
    return None, None

def query_index(es, index):
    """
    查询指定索引，按5分钟粒度聚合 app_upflow，取平均值最高的第15个点，返回时间戳和 app_upflow
    """
    query = {
        "size": 0,
        "aggs": {
            "by_5min": {
                "date_histogram": {
                    "field": "@timestamp",
                    "fixed_interval": "5m",
                    "min_doc_count": 1
                },
                "aggs": {
                    "avg_upflow": {"avg": {"field": "app_upflow"}},
                    "top_doc": {"top_hits": {"size": 1, "_source": ["srm_channel"]}}
                }
            }
        }
    }
    try:
        print(f"🔎 正在查询索引: {index}")
        response = es.search(index=index, body=query, request_timeout=90)
        buckets = response.get('aggregations', {}).get('by_5min', {}).get('buckets', [])
        if len(buckets) < 15:
            print(f"⚠️ 索引 {index} 中聚合后的数据点不足15个")
            return None
        # 使用客户端排序，避免管道聚合兼容性问题
        sorted_buckets = sorted(
            buckets,
            key=lambda b: (b.get('avg_upflow', {}) or {}).get('value', 0) or 0,
            reverse=True
        )
        if len(sorted_buckets) < 15:
            print(f"⚠️ 索引 {index} 中聚合后的数据点不足15个")
            return None
        fifteenth = sorted_buckets[14]
        timestamp_ms = fifteenth['key'] # ES聚合返回的是毫秒时间戳
        timestamp = datetime.fromtimestamp(timestamp_ms / 1000.0)
        app_upflow = fifteenth['avg_upflow']['value']
        srm_channel = fifteenth['top_doc']['hits']['hits'][0].get('_source', {}).get('srm_channel')
        
        return {
            "timestamp": timestamp,
            "app_upflow": app_upflow,
            "srm_channel": srm_channel
        }
    except Exception as e:
        print(f"❌ 查询索引 {index} 失败: {e}")
        return None

def query_index_channels(es, index):
    query = {
        "size": 0,
        "query": {"bool": {"must": [{"exists": {"field": "srm_channel"}}]}},
        "aggs": {
            "by_channel": {
                "terms": {"field": "srm_channel", "size": 10000},
                "aggs": {
                    "by_5min": {
                        "date_histogram": {
                            "field": "@timestamp",
                            "fixed_interval": "5m",
                            "min_doc_count": 1
                        },
                        "aggs": {
                            "avg_upflow": {"avg": {"field": "app_upflow"}}
                        }
                    }
                }
            }
        }
    }
    print(f"🔎 正在查询索引(全量按渠道): {index}")
    response = es.search(index=index, body=query, request_timeout=120)
    ch_buckets = response.get('aggregations', {}).get('by_channel', {}).get('buckets', [])
    result = {}
    for ch in ch_buckets:
        key = ch.get('key')
        hist = ch.get('by_5min', {}).get('buckets', [])
        if not hist:
            continue
        sorted_hist = sorted(
            hist,
            key=lambda b: (b.get('avg_upflow', {}) or {}).get('value', 0) or 0,
            reverse=True
        )
        n = len(sorted_hist)
        if n == 0:
            continue
        top_k = int(n * 0.05)
        if top_k >= n:
            continue
        selected = sorted_hist[top_k]
        ts = datetime.fromtimestamp(selected['key'] / 1000.0)
        val = (selected.get('avg_upflow', {}) or {}).get('value', 0)
        result[key] = {"timestamp": ts, "app_upflow": val}
    return result

def _in_window(ts):
    h = ts.hour
    m = ts.minute
    return (h > 18 and h <= 23) or (h == 18 and m >= 0)

def main():
    # 配置参数
    ES_URL = "http://e.es.kingdata.ksyun.com:9200"
    USERNAME = "readonly"
    PASSWORD = "re2)f1MaFsa"
    
    INDEX_TODAY = "eds_billing-20260203"
    #INDEX_YESTERDAY = "eds_billing-20251126"
    THRESHOLD = 10000000000
    
    print("🚀 开始窗口检查 eds_billing 数据")
    print(f"📅 查询索引: {INDEX_TODAY}")
    
    # 步骤1: 连接ES
    es = connect_to_es(ES_URL, USERNAME, PASSWORD)
    if not es:
        print("❌ 无法连接到ES，程序退出")
        return
    
    try:
        today_map = query_index_channels(es, INDEX_TODAY)
        if not today_map:
            print("❌ 今天数据查询失败或无有效渠道")
            return

        rows_detail = []
        rows_summary = []
        anomalies = []
        alerts_rows = []
        lowflow_rows = []
        for ch, td in sorted(today_map.items()):
            in_win_t = _in_window(td['timestamp'])
            threshold_met = (td['app_upflow'] is not None) and (td['app_upflow'] >= THRESHOLD)
            anomaly = (not in_win_t) and threshold_met

            rows_detail.append({
                "label": "today",
                "index": INDEX_TODAY,
                "srm_channel": ch,
                "timestamp": td['timestamp'],
                "time_hhmm": td['timestamp'].strftime('%H:%M'),
                "app_upflow": td['app_upflow']
            })
            rows_summary.append({
                "srm_channel": ch,
                "index_today": INDEX_TODAY,
                "time_today": td['timestamp'].strftime('%H:%M'),
                "app_upflow_today": td['app_upflow'],
                "threshold_met": threshold_met,
                "in_window_today": in_win_t,
                "anomaly": anomaly
            })
            if anomaly:
                anomalies.append(ch)
                alerts_rows.append({
                    "srm_channel": ch,
                    "index_today": INDEX_TODAY,
                    "time_today": td['timestamp'].strftime('%H:%M'),
                    "timestamp": td['timestamp'],
                    "app_upflow_today": td['app_upflow']
                })
            elif (not in_win_t) and (not threshold_met):
                lowflow_rows.append({
                    "srm_channel": ch,
                    "index_today": INDEX_TODAY,
                    "time_today": td['timestamp'].strftime('%H:%M'),
                    "timestamp": td['timestamp'],
                    "app_upflow_today": td['app_upflow']
                })

        df_detail = pd.DataFrame(rows_detail)
        df_summary = pd.DataFrame(rows_summary)

        filename = f"eds_billing_window_check_{INDEX_TODAY.split('-')[-1]}.xlsx"
        output_path = os.path.join(os.getcwd(), filename)
        if os.path.exists(output_path):
            ts_suffix = datetime.now().strftime('%H%M%S')
            output_path = os.path.join(os.getcwd(), f"eds_billing_window_check_{INDEX_TODAY.split('-')[-1]}_{ts_suffix}.xlsx")
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            pd.DataFrame(alerts_rows).to_excel(writer, sheet_name="alerts", index=False)
            pd.DataFrame(lowflow_rows).to_excel(writer, sheet_name="out_of_window_below_threshold", index=False)

        if anomalies:
            print(f"⚠️ 检测到异常渠道数量: {len(anomalies)}")
            print("异常渠道:", ", ".join(anomalies[:20]))
        else:
            print("✅ 无异常差异")
        print(f"📄 已生成Excel: {output_path}")
    except Exception as e:
        print(f"❌ 程序执行出错: {e}")
    
    finally:
        # 关闭ES连接
        try:
            es.transport.close()
            print("\n🔌 已关闭ES连接")
        except:
            pass

if __name__ == "__main__":
    main()

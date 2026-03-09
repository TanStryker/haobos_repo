import pandas as pd
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta, timezone
import math
import os

import requests

def get_billing_time_from_api(day_date):
    """
    从接口获取指定日期的计费时间点 (RealTime)
    """
    url = "http://100.83.3.236:10090/2019-03-01/statistics/GetP2PBillingData"
    headers = {
        "X-KSC-ACCOUNT-ID": "73400809",
        "Content-Type": "application/json",
        "X-action": "GetERNBillingData"
    }
    
    start_time_str = day_date.strftime("%Y-%m-%dT00:00+0800")
    end_time_str = (day_date + timedelta(days=1) - timedelta(seconds=1)).strftime("%Y-%m-%dT23:59+0800")
    
    body = {
        "StartTime": start_time_str,
        "EndTime": end_time_str,
        "DomainNames": "",
        "BillingMode": "peak95bw"
    }
    
    try:
        print(f"正在从接口获取 {day_date.strftime('%Y-%m-%d')} 的计费时间...")
        response = requests.post(url, headers=headers, json=body, timeout=10)
        response.raise_for_status() # 如果状态码不是 2xx，则抛出异常
        
        data = response.json()
        real_time_str = data.get("RealTime")
        
        if real_time_str:
            # 尝试解析多种可能的日期格式
            dt_obj = None
            # 格式1: '2026-03-05T22:30+0800' 无秒
            # 格式2: '2026-03-05T22:30:00+0800' (有秒)
            # 格式3: '2026-03-05 22:30:00' (旧格式)
            for fmt in ("%Y-%m-%dT%H:%M%z", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S"):
                try:
                    dt_obj = datetime.strptime(real_time_str, fmt)
                    break # 解析成功，跳出循环
                except ValueError:
                    continue # 格式不匹配，尝试下一个
            
            if dt_obj:
                return dt_obj.strftime("%H:%M")
            else:
                print(f"❌ 无法解析接口返回的日期格式: {real_time_str}")
                return None
        else:
            print(f"⚠️ 接口未返回 {day_date.strftime('%Y-%m-%d')} 的 RealTime")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"❌ 调用接口失败: {e}")
        return None
    except (ValueError, KeyError) as e:
        print(f"❌ 解析接口返回数据失败: {e}")
        return None


def connect_to_es(es_url, username=None, password=None):
    """
    连接到Elasticsearch服务器
    """
    try:
        if username and password:
            es = Elasticsearch(es_url, basic_auth=(username, password), request_timeout=120)
        else:
            es = Elasticsearch(es_url, request_timeout=120)
        
        if es.ping():
            print("✅ 成功连接到Elasticsearch")
            return es
        else:
            print("❌ 无法连接到Elasticsearch")
            return None
    except Exception as e:
        print(f"❌ 连接Elasticsearch失败: {e}")
        return None

def get_95_peak_for_day(es, index_pattern, channel, day_date, specified_time_str=None):
    """
    查询指定日期的95峰值，以及可选的指定时间点带宽，按isp字段细分
    day_date: datetime object (representing the start of the day 00:00:00)
    specified_time_str: string "HH:MM", optional
    """
    start_time = day_date
    end_time = day_date + timedelta(days=1) - timedelta(seconds=1) # 23:59:59
    
    # 转换为ISO格式字符串
    start_str = start_time.strftime("%Y-%m-%dT%H:%M:%S")
    end_str = end_time.strftime("%Y-%m-%dT%H:%M:%S")
    
    print(f"正在查询日期: {day_date.strftime('%Y-%m-%d')} (Channel: {channel})...")
    
    query = {
        "size": 0,
        "query": {
            "bool": {
                "must": [
                    {"term": {"srm_channel": channel}},
                    {"range": {
                        "@timestamp": {
                            "gte": start_str,
                            "lte": end_str,
                            "time_zone": "+08:00"
                        }
                    }}
                ]
            }
        },
        "aggs": {
            "by_isp": {
                "terms": {"field": "isp", "size": 10},
                "aggs": {
                    "by_5min": {
                        "date_histogram": {
                            "field": "@timestamp",
                            "fixed_interval": "5m",
                            "min_doc_count": 0,
                            "time_zone": "+08:00",
                            "extended_bounds": {
                                "min": start_str,
                                "max": end_str
                            }
                        },
                        "aggs": {
                            "total_up_flow": {"sum": {"field": "up_flow"}}
                        }
                    }
                }
            }
        }
    }
    
    try:
        resp = es.search(index=index_pattern, body=query, ignore_unavailable=True)
    except Exception as e:
        print(f"❌ 查询失败: {e}")
        return None

    isp_buckets = resp.get("aggregations", {}).get("by_isp", {}).get("buckets", [])
    if not isp_buckets:
        print(f"⚠️ 日期 {day_date.strftime('%Y-%m-%d')} 无数据")
        return None
        
    isp_results = []
    raw_data_points = [] # list of {date, timestamp, dimension, isp, bandwidth}
    
    # 用来存储整个渠道的总和点位
    channel_total_points = {} # timestamp -> sum_up_flow
    
    for isp_bucket in isp_buckets:
        isp_name = isp_bucket["key"]
        buckets = isp_bucket.get("by_5min", {}).get("buckets", [])
        
        # 处理数据
        data_points = []
        specified_bandwidth = None
        
        for bucket in buckets:
            sum_up_flow = bucket.get("total_up_flow", {}).get("value", 0)
            avg_bw = (sum_up_flow * 8) / 300 / 1024
            
            ts_ms = bucket['key']
            # 这里的 ts_ms 是窗口开始时间
            ts_dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(timezone(timedelta(hours=8)))
            ts_dt = ts_dt.replace(tzinfo=None)
            
            data_points.append({
                "timestamp": ts_dt,
                "bandwidth": avg_bw
            })
            
            # 添加到明细数据
            raw_data_points.append({
                "日期": day_date.strftime("%Y-%m-%d"),
                "时间": ts_dt.strftime("%H:%M"),
                "维度": "运营商细分",
                "运营商": isp_name,
                "上行带宽": avg_bw
            })
            
            # 累加到渠道总和
            if ts_dt not in channel_total_points:
                channel_total_points[ts_dt] = 0
            channel_total_points[ts_dt] += sum_up_flow
            
            if specified_time_str:
                if ts_dt.strftime("%H:%M") == specified_time_str:
                    specified_bandwidth = avg_bw
        
        # 计算该 ISP 的 95 峰值
        sorted_points = sorted(data_points, key=lambda x: x["bandwidth"], reverse=True)
        count = len(sorted_points)
        if count == 0:
            continue
            
        rank = math.ceil(count * 0.05)
        index = rank - 1
        if index < 0: index = 0
        if index >= count: index = count - 1
        
        target_point = sorted_points[index]
        peak_95_bandwidth = target_point["bandwidth"]
        
        print(f"DEBUG: ISP {isp_name}, 日期 {day_date.strftime('%Y-%m-%d')}, 总点数: {count}, 95%位置: 第 {rank} 个, 时间 {target_point['timestamp'].strftime('%H:%M')}, 峰值 {peak_95_bandwidth:.4f}")
        
        res = {
            "date": day_date.strftime("%Y-%m-%d"),
            "isp": isp_name,
            "bandwidth": peak_95_bandwidth,
            "time_point": target_point["timestamp"].strftime("%H:%M")
        }
        
        if specified_time_str:
            if specified_bandwidth is None:
                 specified_bandwidth = 0
            res["specified_bandwidth"] = specified_bandwidth
            res["diff"] = peak_95_bandwidth - specified_bandwidth
            
        isp_results.append(res)
        
    # 计算渠道总维度的 95 峰值
    channel_data_points = []
    channel_specified_bandwidth = None
    for ts_dt, total_flow in channel_total_points.items():
        avg_bw = (total_flow * 8) / 300 / 1024
        channel_data_points.append({
            "timestamp": ts_dt,
            "bandwidth": avg_bw
        })
        # 添加到明细数据
        raw_data_points.append({
            "日期": day_date.strftime("%Y-%m-%d"),
            "时间": ts_dt.strftime("%H:%M"),
            "维度": "渠道汇总",
            "运营商": "ALL",
            "上行带宽": avg_bw
        })
        if specified_time_str and ts_dt.strftime("%H:%M") == specified_time_str:
            channel_specified_bandwidth = avg_bw
            
    sorted_channel = sorted(channel_data_points, key=lambda x: x["bandwidth"], reverse=True)
    count_ch = len(sorted_channel)
    channel_result = None
    if count_ch > 0:
        rank_ch = math.ceil(count_ch * 0.05)
        idx_ch = rank_ch - 1
        if idx_ch < 0: idx_ch = 0
        if idx_ch >= count_ch: idx_ch = count_ch - 1
        
        target_ch = sorted_channel[idx_ch]
        peak_ch = target_ch["bandwidth"]
        
        print(f"DEBUG: Channel Total, 日期 {day_date.strftime('%Y-%m-%d')}, 总点数: {count_ch}, 95%位置: 第 {rank_ch} 个, 时间 {target_ch['timestamp'].strftime('%H:%M')}, 峰值 {peak_ch:.4f}")
        print("DEBUG: Channel Top 20 数据点:")
        for i, p in enumerate(sorted_channel[:20]):
            print(f"  {i+1}. {p['timestamp'].strftime('%H:%M')} : {p['bandwidth']:.4f}")
            
        channel_result = {
            "date": day_date.strftime("%Y-%m-%d"),
            "bandwidth": peak_ch,
            "time_point": target_ch["timestamp"].strftime("%H:%M")
        }
        if specified_time_str:
            if channel_specified_bandwidth is None: channel_specified_bandwidth = 0
            channel_result["specified_bandwidth"] = channel_specified_bandwidth
            channel_result["diff"] = peak_ch - channel_specified_bandwidth
            
    return {
        "channel_peak": channel_result,
        "isp_peaks": isp_results,
        "raw_data_points": raw_data_points
    }



def scan_early_peak_channels(es, start_date, end_date):
    """
    扫描指定日期范围内所有渠道，找出95峰值在08:00到17:00之间的渠道，并按isp细分
    """
    print(f"\n🚀 开始扫描 {start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')} 期间所有渠道和运营商的95峰值...")
    
    target_records = []
    
    current_date = start_date
    while current_date <= end_date:
        day_str = current_date.strftime('%Y-%m-%d')
        print(f"\nProcessing {day_str}...")
        
        curr_index = f"eds_billing-{current_date.strftime('%Y%m%d')}"
        prev_date = current_date - timedelta(days=1)
        prev_index = f"eds_billing-{prev_date.strftime('%Y%m%d')}"
        target_indices = f"{prev_index},{curr_index}"
        
        start_time = current_date
        end_time = current_date + timedelta(days=1) - timedelta(seconds=1)
        start_str = start_time.strftime("%Y-%m-%dT%H:%M:%S")
        end_str = end_time.strftime("%Y-%m-%dT%H:%M:%S")
        
        query = {
            "size": 0,
            "query": {
                "bool": {
                    "must": [
                        {"exists": {"field": "srm_channel"}},
                        {"range": {
                            "@timestamp": {
                                "gte": start_str,
                                "lte": end_str,
                                "time_zone": "+08:00"
                            }
                        }}
                    ]
                }
            },
            "aggs": {
                "by_channel": {
                    "terms": {"field": "srm_channel", "size": 10000},
                    "aggs": {
                        "by_isp": {
                            "terms": {"field": "isp", "size": 10},
                            "aggs": {
                                "by_5min": {
                                    "date_histogram": {
                                        "field": "@timestamp",
                                        "fixed_interval": "5m",
                                        "min_doc_count": 0,
                                        "time_zone": "+08:00",
                                        "extended_bounds": {
                                            "min": start_str,
                                            "max": end_str
                                        }
                                    },
                                    "aggs": {
                                        "total_up_flow": {"sum": {"field": "up_flow"}}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        try:
            resp = es.search(index=target_indices, body=query, ignore_unavailable=True, request_timeout=180)
        except Exception as e:
            print(f"❌ 查询失败: {e}")
            current_date += timedelta(days=1)
            continue
            
        channels_buckets = resp.get("aggregations", {}).get("by_channel", {}).get("buckets", [])
        
        for ch_bucket in channels_buckets:
            channel_name = ch_bucket["key"]
            isp_buckets = ch_bucket.get("by_isp", {}).get("buckets", [])
            
            # 用来存储整个渠道的总和点位
            ch_total_points = {} # timestamp -> sum_up_flow
            
            for isp_bucket in isp_buckets:
                isp_name = isp_bucket["key"]
                buckets = isp_bucket.get("by_5min", {}).get("buckets", [])
                
                # Calculate ISP 95 peak
                data_points = []
                for bucket in buckets:
                    sum_up_flow = bucket.get("total_up_flow", {}).get("value", 0)
                    avg_bw = (sum_up_flow * 8) / 300 / 1024
                    
                    ts_ms = bucket['key']
                    ts_dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(timezone(timedelta(hours=8)))
                    ts_dt = ts_dt.replace(tzinfo=None)
                    
                    data_points.append({"timestamp": ts_dt, "bandwidth": avg_bw})
                    
                    # Accumulate for channel total
                    if ts_dt not in ch_total_points: ch_total_points[ts_dt] = 0
                    ch_total_points[ts_dt] += sum_up_flow
                    
                sorted_points = sorted(data_points, key=lambda x: x["bandwidth"], reverse=True)
                count = len(sorted_points)
                if count > 0:
                    rank = math.ceil(count * 0.05)
                    idx = rank - 1
                    if idx < 0: idx = 0
                    if idx >= count: idx = count - 1
                    
                    target_point = sorted_points[idx]
                    peak_time = target_point["timestamp"]
                    if 8 <= peak_time.hour <= 17:
                        target_records.append({
                            "时间戳": peak_time.strftime("%Y-%m-%d %H:%M"),
                            "维度": "运营商细分",
                            "运营商": isp_name,
                            "上行带宽": target_point["bandwidth"],
                            "渠道ID": channel_name
                        })
            
            # Calculate Channel Total 95 peak
            ch_data_points = []
            for ts, total_flow in ch_total_points.items():
                ch_data_points.append({"timestamp": ts, "bandwidth": (total_flow * 8) / 300 / 1024})
            
            sorted_ch = sorted(ch_data_points, key=lambda x: x["bandwidth"], reverse=True)
            if sorted_ch:
                rank_ch = math.ceil(len(sorted_ch) * 0.05)
                idx_ch = rank_ch - 1
                if idx_ch < 0: idx_ch = 0
                if idx_ch >= len(sorted_ch): idx_ch = len(sorted_ch) - 1
                
                target_ch = sorted_ch[idx_ch]
                if 8 <= target_ch["timestamp"].hour <= 17:
                    target_records.append({
                        "时间戳": target_ch["timestamp"].strftime("%Y-%m-%d %H:%M"),
                        "维度": "渠道汇总",
                        "运营商": "ALL",
                        "上行带宽": target_ch["bandwidth"],
                        "渠道ID": channel_name
                    })
                
        current_date += timedelta(days=1)
        
    # Save to Excel
    if target_records:
        df = pd.DataFrame(target_records)
        df = df[["时间戳", "维度", "运营商", "上行带宽", "渠道ID"]]
        output_file = f"8点-17点峰值汇总_综合_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.xlsx"
        try:
            df.to_excel(output_file, index=False)
            print(f"\n✅ 结果已保存至: {output_file}")
            print(f"共发现 {len(target_records)} 条记录")
        except Exception as e:
            print(f"❌ 保存Excel失败: {e}")
    else:
        print("\nℹ️ 未发现任何符合条件的记录")


def main():
    ES_URL = "http://e.es.kingdata.ksyun.com:9200"
    USERNAME = "readonly"
    PASSWORD = "re2)f1MaFsa"
    INDEX_PATTERN = "eds_billing-*"
    
    print("=== 95错峰查询工具 ===")
    print("1. 指定渠道查询")
    print("2. 扫描所有渠道(筛选8:00-17:00峰值)")
    
    choice = input("请输入功能编号 (1/2): ").strip()
    
    if choice == "2":
        start_date_str = input("请输入开始日期 (YYYY-MM-DD): ").strip()
        end_date_str = input("请输入结束日期 (YYYY-MM-DD): ").strip()
        
        try:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        except ValueError:
            print("❌ 日期格式错误")
            return
            
        if start_date > end_date:
            print("❌ 日期范围错误")
            return
            
        es = connect_to_es(ES_URL, USERNAME, PASSWORD)
        if not es: return
        
        scan_early_peak_channels(es, start_date, end_date)
        try: es.transport.close()
        except: pass
        return

    # Original Mode 1 logic
    srm_channel_input = input("请输入 srm_channel (多个用逗号分隔): ").strip()
    if not srm_channel_input:
        print("❌ srm_channel 不能为空")
        return
    
    srm_channels = [ch.strip() for ch in srm_channel_input.split(",") if ch.strip()]
        
    start_date_str = input("请输入开始日期 (YYYY-MM-DD): ").strip()
    end_date_str = input("请输入结束日期 (YYYY-MM-DD): ").strip()
    
    specified_time_input = input("请输入指定窗口时间 (HH:MM，或输入 auto 自动获取，可选): ").strip()
    
    auto_fetch_time = False
    specified_time_str = None
    
    if specified_time_input.lower() == 'auto':
        auto_fetch_time = True
    elif specified_time_input:
        try:
            datetime.strptime(specified_time_input, "%H:%M")
            specified_time_str = specified_time_input
        except ValueError:
            print("❌ 时间格式错误，请使用 HH:MM 格式")
            return
    
    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    except ValueError:
        print("❌ 日期格式错误，请使用 YYYY-MM-DD 格式")
        return
        
    if start_date > end_date:
        print("❌ 开始日期不能晚于结束日期")
        return
        
    es = connect_to_es(ES_URL, USERNAME, PASSWORD)
    if not es:
        return

    # 如果是自动模式，预先获取所有日期的计费时间
    daily_specified_times = {}
    api_times_records = []
    if auto_fetch_time:
        print("\n>>> 正在批量获取接口计费时间... <<<")
        temp_date = start_date
        while temp_date <= end_date:
            time_str = get_billing_time_from_api(temp_date)
            if time_str:
                daily_specified_times[temp_date.strftime('%Y-%m-%d')] = time_str
                api_times_records.append({
                    "日期": temp_date.strftime('%Y-%m-%d'),
                    "接口获取时间": time_str
                })
            temp_date += timedelta(days=1)
    
    all_results = {} # channel -> { 'total': [], 'isp': [], 'raw': [] }
    early_peak_records = [] 
    diff_summary_records = [] # 用于汇总所有渠道的带宽差
    
    for channel in srm_channels:
        print(f"\n>>> 正在处理渠道: {channel} <<<")
        channel_results = {'total': [], 'isp': [], 'raw': []}
        current_date = start_date
        while current_date <= end_date:
            # 如果是自动模式，从预获取的字典中查找时间
            if auto_fetch_time:
                current_date_str = current_date.strftime('%Y-%m-%d')
                specified_time_str = daily_specified_times.get(current_date_str)
                if not specified_time_str:
                    print(f"未获取到 {current_date_str} 的计费时间，跳过带宽差计算")
            
            curr_index = f"eds_billing-{current_date.strftime('%Y%m%d')}"
            prev_date = current_date - timedelta(days=1)
            prev_index = f"eds_billing-{prev_date.strftime('%Y%m%d')}"
            target_indices = f"{prev_index},{curr_index}"
            
            day_data = get_95_peak_for_day(es, target_indices, channel, current_date, specified_time_str)
            if day_data:
                if day_data['channel_peak']:
                    peak_data = day_data['channel_peak']
                    channel_results['total'].append(peak_data)
                    
                    # 收集带宽差汇总数据 (如果指定了时间)
                    if specified_time_str and "diff" in peak_data:
                        diff_summary_records.append({
                            "日期": peak_data["date"],
                            "维度": "渠道汇总",
                            "运营商": "ALL",
                            "渠道ID": channel,
                            "95峰值": peak_data["bandwidth"],
                            "渠道在当天大盘95时间点带宽": peak_data["specified_bandwidth"],
                            "带宽差": peak_data["diff"]
                        })

                    tp_str = peak_data.get("time_point", "")
                    if tp_str:
                        try:
                            hour = int(tp_str.split(":")[0])
                            if hour < 12:
                                early_peak_records.append({
                                    "时间戳": f"{day_data['channel_peak']['date']} {tp_str}",
                                    "维度": "渠道汇总",
                                    "运营商": "ALL",
                                    "上行带宽": day_data['channel_peak']["bandwidth"],
                                    "渠道ID": channel
                                })
                        except ValueError: pass

                for isp_res in day_data['isp_peaks']:
                    channel_results['isp'].append(isp_res)
                    
                    # 同时收集运营商维度的带宽差数据
                    if specified_time_str and "diff" in isp_res:
                        diff_summary_records.append({
                            "日期": isp_res["date"],
                            "维度": "运营商细分",
                            "运营商": isp_res.get("isp", "unknown"),
                            "渠道ID": channel,
                            "95峰值": isp_res["bandwidth"],
                            "渠道在当天大盘95时间点带宽": isp_res["specified_bandwidth"],
                            "带宽差": isp_res["diff"]
                        })

                    tp_str = isp_res.get("time_point", "")
                    if tp_str:
                        try:
                            hour = int(tp_str.split(":")[0])
                            if hour < 12:
                                early_peak_records.append({
                                    "时间戳": f"{isp_res['date']} {tp_str}",
                                    "维度": "运营商细分",
                                    "运营商": isp_res.get("isp", "unknown"),
                                    "上行带宽": isp_res["bandwidth"],
                                    "渠道ID": channel
                                })
                        except ValueError: pass
                
                # 收集明细数据
                if 'raw_data_points' in day_data:
                    channel_results['raw'].extend(day_data['raw_data_points'])
                    
            current_date += timedelta(days=1)
            
        if channel_results['total'] or channel_results['isp']:
            all_results[channel] = channel_results
        
    if all_results:
        output_file = f"95错峰查询_综合汇总_{start_date_str}_{end_date_str}.xlsx"
        try:
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                if early_peak_records:
                    df_early = pd.DataFrame(early_peak_records)
                    df_early = df_early[["时间戳", "维度", "运营商", "上行带宽", "渠道ID"]]
                    df_early.to_excel(writer, sheet_name="12点前峰值汇总", index=False)
                
                if api_times_records:
                    df_api_times = pd.DataFrame(api_times_records)
                    df_api_times.to_excel(writer, sheet_name="接口获取时间", index=False)
                    print(f"✅ 已写入 {len(api_times_records)} 条记录到 Sheet: 接口获取时间")

                if diff_summary_records:
                    df_diff = pd.DataFrame(diff_summary_records)
                    # 调整列顺序
                    diff_cols = ["日期", "维度", "运营商", "渠道ID", "95峰值", "渠道在当天大盘95时间点带宽", "带宽差"]
                    df_diff = df_diff[diff_cols]
                    df_diff.to_excel(writer, sheet_name="渠道带宽差汇总", index=False)
                    print(f"✅ 已写入 {len(diff_summary_records)} 条记录到 Sheet: 渠道带宽差汇总")
                
                for channel, results in all_results.items():
                    # 合并渠道总计和 ISP 细分 (Sheet 1: 95峰值汇总)
                    df_total = pd.DataFrame(results['total'])
                    if not df_total.empty:
                        df_total['维度'] = '渠道汇总'
                        df_total['运营商'] = 'ALL'
                    
                    df_isp = pd.DataFrame(results['isp'])
                    if not df_isp.empty:
                        df_isp['维度'] = '运营商细分'
                        if 'isp' in df_isp.columns:
                            df_isp = df_isp.rename(columns={'isp': '运营商'})
                    
                    df_combined = pd.concat([df_total, df_isp], ignore_index=True)
                    
                    rename_map = {
                        "bandwidth": "当天上行带宽", 
                        "date": "日期", 
                        "time_point": "当天上行带宽对应时间"
                    }
                    columns_order = ["日期", "维度", "运营商", "当天上行带宽", "当天上行带宽对应时间"]
                    if specified_time_str:
                        rename_map["specified_bandwidth"] = f"{specified_time_str} 带宽"
                        rename_map["diff"] = f"95峰值 - {specified_time_str} 差值"
                        columns_order.append(f"{specified_time_str} 带宽")
                        columns_order.append(f"95峰值 - {specified_time_str} 差值")
                    
                    df_combined = df_combined.rename(columns=rename_map)
                    existing_cols = [c for c in columns_order if c in df_combined.columns]
                    df_combined = df_combined[existing_cols]
                    
                    sheet_name = f"{channel[:25]}_95汇总"
                    df_combined.to_excel(writer, sheet_name=sheet_name, index=False)
                    
                    # Sheet 2: 明细数据
                    if results['raw']:
                        df_raw = pd.DataFrame(results['raw'])
                        # 排序：日期、维度、时间、运营商
                        df_raw = df_raw.sort_values(by=["日期", "维度", "时间", "运营商"])
                        raw_sheet_name = f"{channel[:25]}_明细"
                        df_raw.to_excel(writer, sheet_name=raw_sheet_name, index=False)
                        print(f"✅ 渠道 {channel} 数据已写入 Sheet: {sheet_name} 和 {raw_sheet_name}")
                    else:
                        print(f"✅ 渠道 {channel} 数据已写入 Sheet: {sheet_name}")
            print(f"\n✅ 所有结果已保存至: {output_file}")

        except Exception as e:
            print(f"❌ 保存Excel失败: {e}")
    else:
        print("\n⚠️ 未查询到任何数据")
        
    try:
        es.transport.close()
    except:
        pass

if __name__ == "__main__":
    main()

from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import pandas as pd
import os

def connect_to_es(es_url, username=None, password=None):
    try:
        if username and password:
            return Elasticsearch(es_url, basic_auth=(username, password))
        return Elasticsearch(es_url)
    except Exception:
        return None

def query_index_channels(es, index):
    query = {
        "size": 0,
        "query": {"bool": {"must": [{"exists": {"field": "srm_channel"}}]}},
        "aggs": {
            "by_channel": {
                "terms": {"field": "srm_channel", "size": 10000},
                "aggs": {
                    "by_1min": {
                        "date_histogram": {
                            "field": "@timestamp",
                            "fixed_interval": "1m",
                            "min_doc_count": 1
                        },
                        "aggs": {"sum_up_flow": {"sum": {"field": "up_flow"}}}
                    }
                }
            }
        }
    }
    resp = es.search(index=index, body=query, request_timeout=120)
    ch_buckets = resp.get("aggregations", {}).get("by_channel", {}).get("buckets", [])
    result = {}
    for ch in ch_buckets:
        key = ch.get("key")
        hist = ch.get("by_1min", {}).get("buckets", [])
        if not hist:
            continue
        filtered = []
        for b in hist:
            ts = datetime.fromtimestamp(b["key"] / 1000.0)
            if ts.minute % 5 == 4:
                filtered.append(b)
        if not filtered:
            continue
        sorted_hist = sorted(
            filtered,
            key=lambda b: (b.get("sum_up_flow", {}) or {}).get("value", 0) or 0,
            reverse=True,
        )
        n = len(sorted_hist)
        if n == 0:
            continue
        top_k = int(n * 0.05)
        if top_k >= n:
            continue
        selected = sorted_hist[top_k]
        ts = datetime.fromtimestamp(selected["key"] / 1000.0)
        val = (selected.get("sum_up_flow", {}) or {}).get("value", 0)
        result[key] = {"timestamp": ts, "up_flow": val}
    return result

def _yesterday_index(index_today):
    try:
        prefix, date_str = index_today.split("-", 1)
        dt = datetime.strptime(date_str, "%Y%m%d") - timedelta(days=1)
        return f"{prefix}-{dt.strftime('%Y%m%d')}"
    except Exception:
        return index_today

def main():
    ES_URL = "http://e.es.kingdata.ksyun.com:9200"
    USERNAME = "readonly"
    PASSWORD = "re2)f1MaFsa"

    DEFAULT_DATE = "20251214"
    user_date = input("请输入今日日期(YYYYMMDD)，直接回车跳过): ").strip()
    if user_date and len(user_date) == 8 and user_date.isdigit():
        INDEX_TODAY = f"eds_billing-{user_date}"
        INDEX_YESTERDAY = _yesterday_index(INDEX_TODAY)
    else:
        pair = input("请输入两个对比日期(YYYYMMDD YYYYMMDD)，直接回车退出): ").strip()
        if not pair:
            print("退出程序")
            return
        parts = [p for p in pair.split() if p.isdigit() and len(p) == 8]
        if len(parts) != 2:
            print("日期格式不正确")
            return
        INDEX_YESTERDAY = f"eds_billing-{parts[0]}"
        INDEX_TODAY = f"eds_billing-{parts[1]}"

    FIVE_G_BITS = 1 * 1000 * 1000 * 1000
    HIGH_WEIGHT_CHANNELS = [
        "1000121915",
        "1000121916",
        "1000121925",
        "1000122015",
        "1000121986",
        "1000122355",
        "1000121986",
        "1000121997",
        "2000127791",
        "1000121967",
    ]
    LOW_WEIGHT_CHANNELS = [
        "2021072609",
        "2022071409",
        "2022071801",
        "2022090506",
        "2022090507",
        "2023032002",
        "2023032003",
        "2023081602",
        "2023082801",
        "2023101106",
        "2024011610",
        "2024040905",
        "2024041001",
        "2024062601",
        "2024062603",
        "2024062802",
        "2024072604",
        "2024091001",
        "2024112910",
        "2024122301",
        "2024123101",
        "2025020801",
        "2025031803",
        "2025032505",
        "2025040105",
        "2025071602",
        "2025082502",
        "2025091601",
        "2025091702",
    ]

    print(f"比对索引 昨日={INDEX_YESTERDAY} 今日={INDEX_TODAY}")
    es = connect_to_es(ES_URL, USERNAME, PASSWORD)
    if not es or not es.ping():
        print("无法连接到ES")
        return

    try:
        ymap = query_index_channels(es, INDEX_YESTERDAY)
        tmap = query_index_channels(es, INDEX_TODAY)
    except Exception as e:
        print(f"查询失败: {e}")
        return

    alerts_high = []
    alerts_low = []

    for ch in HIGH_WEIGHT_CHANNELS:
        if ch in ymap and ch in tmap:
            yd = ymap[ch]["up_flow"]
            td = tmap[ch]["up_flow"]
            delta = td - yd
            if delta >= FIVE_G_BITS:
                alerts_high.append({"srm_channel": ch, "yesterday": yd, "today": td, "delta": delta})

    for ch in LOW_WEIGHT_CHANNELS:
        if ch in ymap and ch in tmap:
            yd = ymap[ch]["up_flow"]
            td = tmap[ch]["up_flow"]
            delta = td - yd
            if delta <= -FIVE_G_BITS:
                alerts_low.append({"srm_channel": ch, "yesterday": yd, "today": td, "delta": delta})

    def _dedup_rows(rows):
        seen = set()
        out = []
        for r in rows:
            key = (r.get("srm_channel"), r.get("yesterday"), r.get("today"), r.get("delta"))
            if key in seen:
                continue
            seen.add(key)
            out.append(r)
        return out
    alerts_high = _dedup_rows(alerts_high)
    alerts_low = _dedup_rows(alerts_low)

    factor = 8 * 1000 * 1000 * 1000
    print("高权重渠道告警:")
    for a in alerts_high:
        print(f"{a['srm_channel']} Δ={(a['delta']/factor):.6f}Gbytes today={(a['today']/factor):.6f}Gbytes yesterday={(a['yesterday']/factor):.6f}Gbytes")
    print("低权重渠道告警:")
    for a in alerts_low:
        print(f"{a['srm_channel']} Δ={(a['delta']/factor):.6f}Gbytes today={(a['today']/factor):.6f}Gbytes yesterday={(a['yesterday']/factor):.6f}Gbytes")
    if not alerts_high and not alerts_low:
        print("无异常差异")

    filename = f"high_low_change_{INDEX_YESTERDAY.split('-')[-1]}_{INDEX_TODAY.split('-')[-1]}.xlsx"
    output_path = os.path.join(os.getcwd(), filename)
    if os.path.exists(output_path):
        ts_suffix = datetime.now().strftime('%H%M%S')
        output_path = os.path.join(os.getcwd(), f"high_low_change_{INDEX_YESTERDAY.split('-')[-1]}_{INDEX_TODAY.split('-')[-1]}_{ts_suffix}.xlsx")
    def _to_gbytes_df(rows):
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        factor = 8 * 1000 * 1000 * 1000
        df["yesterday_Gbytes"] = df["yesterday"] / factor
        df["today_Gbytes"] = df["today"] / factor
        df["delta_Gbytes"] = df["delta"] / factor
        return df[["srm_channel", "yesterday_Gbytes", "today_Gbytes", "delta_Gbytes"]]
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        _to_gbytes_df(alerts_high).to_excel(writer, sheet_name="alerts_high", index=False)
        _to_gbytes_df(alerts_low).to_excel(writer, sheet_name="alerts_low", index=False)
    print(f"已生成Excel: {output_path}")

    try:
        es.transport.close()
        print("\n🔌 已关闭ES连接")
    except Exception:
        pass

if __name__ == "__main__":
    main()

import pandas as pd
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta, timezone
import math
import os

import requests

# 业务大盘 job_id 映射
BUSINESS_JOB_IDS = {
    "快手业务": [10118, 10119, 10074, 10075, 10123, 10121, 10122, 10096],
    "字节业务": [10095, 10091, 10012, 10080, 10047, 10081, 10090, 10094, 10017, 10097],
    "小度业务": [10129],
    "百度XCDN业务": [10136],
}

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

def get_business_95_peak_times(es, index_pattern, day_date):
    """
    获取指定日期各个业务大盘的 95 峰值时刻
    """
    start_time = day_date
    end_time = day_date + timedelta(days=1) - timedelta(seconds=1)
    start_str = start_time.strftime("%Y-%m-%dT%H:%M:%S")
    end_str = end_time.strftime("%Y-%m-%dT%H:%M:%S")
    
    business_times = {}
    
    for biz_name, job_ids in BUSINESS_JOB_IDS.items():
        print(f"正在计算 {biz_name} 的 95 峰值时刻 ({day_date.strftime('%Y-%m-%d')})...")
        
        query = {
            "size": 0,
            "query": {
                "bool": {
                    "must": [
                        {"terms": {"job_id": job_ids}},
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
        
        try:
            resp = es.search(index=index_pattern, body=query, ignore_unavailable=True)
            buckets = resp.get("aggregations", {}).get("by_5min", {}).get("buckets", [])
            
            if not buckets:
                print(f"⚠️ {biz_name} 在 {day_date.strftime('%Y-%m-%d')} 无数据")
                continue
                
            data_points = []
            for bucket in buckets:
                sum_up_flow = bucket.get("total_up_flow", {}).get("value", 0)
                # 业务大盘逻辑：原始数据辑* 8 / 300 / 1000000000据 * 8 / 300 / 1000000000
                avg_bw = (sum_up_flow * 8) / 3001000000
                
                ts_ms = bucket['key']
                ts_dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(timezone(timedelta(hours=8)))
                ts_dt = ts_dt.replace(tzinfo=None)
                
                data_points.append({"timestamp": ts_dt, "bandwidth": avg_bw})
            
            sorted_points = sorted(data_points, key=lambda x: x["bandwidth"], reverse=True)
            count = len(sorted_points)
            if count > 0:
                # 修正 95 峰值计算逻辑：去掉前 5% 个点，取下一个点
                # 对于 288 个点，288 * 0.05 = 14.4，向上取整为 15
                # 使用 int(count * 0.05) + 1 确保即使在 count=280 时也能取到第 15 个点
                rank = int(count * 0.05) + 1
                idx = rank - 1
                if idx < 0: idx = 0
                if idx >= count: idx = count - 1
                
                target_point = sorted_points[idx]
                time_str = target_point["timestamp"].strftime("%H:%M")
                peak_bw = target_point["bandwidth"]
                
                business_times[biz_name] = {
                    "time": time_str,
                    "bandwidth": peak_bw
                }
                print(f"DEBUG: {biz_name}, 总点数: {count}, 5%对应点数: {count * 0.05:.2f}, 95%位置: 第 {rank} 个, 峰值时刻: {time_str}, 95峰值: {peak_bw:.4f}")
                
        except Exception as e:
            print(f"❌ 计算 {biz_name} 95峰值时刻失败: {e}")
            
    return business_times

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

def get_total_up_speed_gb_at_times(es, day_date, channel, target_times):
    """
    从 eds_machine_heartbeat-* 索引获取指定渠道在指定日期的 total_up_speed，
    并返回指定时刻（5分钟窗口起始 HH:MM）的值（单位：Gb，按1000进制）。

    5分钟粒度聚合：date_histogram + sum(total_up_speed)
    数据处理算法：原始数据(KB) * 8 / 5，换算至Gb时进制为1000（Kb -> Gb：/1_000_000）
    """
    start_time = day_date
    end_time = day_date + timedelta(days=1) - timedelta(seconds=1)
    start_str = start_time.strftime("%Y-%m-%dT%H:%M:%S")
    end_str = end_time.strftime("%Y-%m-%dT%H:%M:%S")

    curr_index = f"eds_machine_heartbeat-{day_date.strftime('%Y%m%d')}"
    prev_date = day_date - timedelta(days=1)
    prev_index = f"eds_machine_heartbeat-{prev_date.strftime('%Y%m%d')}"
    target_indices = f"{prev_index},{curr_index}"

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
                    "total_up_speed_sum": {"sum": {"field": "total_up_speed"}}
                }
            }
        }
    }

    result = {t: 0 for t in target_times}
    try:
        resp = es.search(index=target_indices, body=query, ignore_unavailable=True)
    except Exception as e:
        print(f"❌ 查询 total_up_speed 失败 (channel={channel}, date={day_date.strftime('%Y-%m-%d')}): {e}")
        return result

    buckets = resp.get("aggregations", {}).get("by_5min", {}).get("buckets", [])
    if not buckets:
        return result

    target_set = set(target_times)
    for bucket in buckets:
        ts_ms = bucket.get("key")
        if ts_ms is None:
            continue
        ts_dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(timezone(timedelta(hours=8)))
        ts_dt = ts_dt.replace(tzinfo=None)
        ts_str = ts_dt.strftime("%H:%M")
        if ts_str not in target_set:
            continue

        raw_kb = bucket.get("total_up_speed_sum", {}).get("value", 0) or 0
        gb_value = (raw_kb * 8) / 5 / 1_000_000
        result[ts_str] = gb_value

    return result

def identify_shift_reason(shift_cost, up_00_gb, up_10_gb, up_19_gb):
    diff = up_00_gb - up_19_gb
    threshold = up_10_gb * 0.05
    if shift_cost > 0 and up_00_gb > up_19_gb and diff > threshold:
        return "供应商晚高峰前下架"
    return ""


def _safe_filename(name: str) -> str:
    bad = '<>:"/\\|?*'
    out = []
    for ch in str(name):
        out.append("_" if ch in bad else ch)
    return "".join(out).strip()


def _safe_sheet_title(name: str, default: str = "Sheet") -> str:
    bad = r'[]:*?/\\'
    s = str(name or "").strip()
    if not s:
        s = default
    out = []
    for ch in s:
        out.append("_" if ch in bad else ch)
    s2 = "".join(out).strip()
    if not s2:
        s2 = default
    return s2[:31]


def _unique_sheet_title(base: str, used: set[str]) -> str:
    title = _safe_sheet_title(base)
    if title not in used:
        used.add(title)
        return title
    i = 1
    while True:
        suffix = f"_{i}"
        cand = _safe_sheet_title(title[: (31 - len(suffix))] + suffix)
        if cand not in used:
            used.add(cand)
            return cand
        i += 1


def _clean_excel_str(s: str) -> str:
    out = []
    for ch in str(s):
        o = ord(ch)
        if (o < 32 and o not in (9, 10, 13)) or o == 127:
            out.append(" ")
        else:
            out.append(ch)
    return "".join(out)


def _clean_df_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    def _clean_value(v):
        if isinstance(v, str):
            return _clean_excel_str(v)
        if isinstance(v, (int, float, bool)) or v is None:
            return v
        return _clean_excel_str(str(v))
    for col in df.columns:
        df[col] = df[col].map(_clean_value)
    return df


def _resolve_output_dir(base_dir_input: str | None) -> str:
    base_dir = (base_dir_input or "").strip()
    if not base_dir:
        base_dir = os.path.join(os.getcwd(), "output")
    run_day = datetime.now().strftime("%Y-%m-%d")
    out_dir = os.path.join(base_dir, run_day)
    os.makedirs(out_dir, exist_ok=True)
    return out_dir


def _build_mode1_output_filename(fetch_ern: bool, fetch_eds: bool, specified_time_str: str | None, start_date_str: str, end_date_str: str) -> str:
    date_part = start_date_str if start_date_str == end_date_str else f"{start_date_str}_{end_date_str}"
    if fetch_ern and not fetch_eds:
        base = f"ERN渠道错峰汇总_{date_part}"
    elif fetch_eds and not fetch_ern:
        base = f"转售渠道错峰汇总_{date_part}"
    elif fetch_ern and fetch_eds:
        base = f"全部渠道错峰汇总_{date_part}"
    else:
        t = (specified_time_str or "").strip() or "HHMM"
        base = f"手动窗口渠道错峰汇总_{date_part}_{t}"
    return _safe_filename(base) + ".xlsx"


def _unique_output_path(path: str) -> str:
    if not path:
        return path
    base, ext = os.path.splitext(path)
    if not os.path.exists(path):
        return path
    i = 1
    while True:
        cand = f"{base}（{i}）{ext}"
        if not os.path.exists(cand):
            return cand
        i += 1


def _get_program_5min_series(es, index_pattern, channel, program_name, day_date):
    start_time = day_date
    end_time = day_date + timedelta(days=1) - timedelta(seconds=1)
    start_str = start_time.strftime("%Y-%m-%dT%H:%M:%S")
    end_str = end_time.strftime("%Y-%m-%dT%H:%M:%S")

    query = {
        "size": 0,
        "query": {
            "bool": {
                "must": [
                    {"term": {"srm_channel": channel}},
                    {"term": {"program_name": program_name}},
                    {"range": {"@timestamp": {"gte": start_str, "lte": end_str, "time_zone": "+08:00"}}},
                ]
            }
        },
        "aggs": {
            "by_5min": {
                "date_histogram": {
                    "field": "@timestamp",
                    "fixed_interval": "5m",
                    "min_doc_count": 0,
                    "time_zone": "+08:00",
                    "extended_bounds": {"min": start_str, "max": end_str},
                },
                "aggs": {"total_up_flow": {"sum": {"field": "up_flow"}}},
            }
        },
    }

    try:
        resp = es.search(index=index_pattern, body=query, ignore_unavailable=True)
    except Exception:
        return []

    buckets = resp.get("aggregations", {}).get("by_5min", {}).get("buckets", []) or []
    points = []
    for b in buckets:
        sum_up_flow = b.get("total_up_flow", {}).get("value", 0) or 0
        avg_bw = (sum_up_flow * 8) / 300 / 1_000_000_000
        ts_ms = b.get("key")
        if ts_ms is None:
            continue
        ts_dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(timezone(timedelta(hours=8)))
        ts_dt = ts_dt.replace(tzinfo=None)
        points.append((ts_dt, avg_bw))
    points.sort(key=lambda x: x[0])
    return points


def _find_first_spike_time(points):
    if len(points) < 4:
        return None
    ratio = 3.0
    abs_delta = 0.05
    for i in range(3, len(points)):
        baseline = (points[i - 1][1] + points[i - 2][1] + points[i - 3][1]) / 3.0
        curr = points[i][1]
        if baseline <= 1e-9:
            if curr >= abs_delta:
                return points[i][0]
            continue
        if curr - baseline >= max(baseline * (ratio - 1.0), abs_delta) and curr >= baseline * ratio:
            return points[i][0]
    return None


def _avg_bw_in_window(points, start_dt, end_dt):
    vals = [bw for (ts, bw) in points if start_dt <= ts < end_dt]
    if not vals:
        return 0.0
    return sum(vals) / len(vals)


def _is_vendor_run_two_hours(es, index_pattern, channel, day_date, program_name):
    points = _get_program_5min_series(es, index_pattern, channel, program_name, day_date)
    if not points:
        return False
    start_dt = _find_first_spike_time(points)
    if start_dt is None:
        return False
    end_dt = points[-1][0]
    tail_start = end_dt - timedelta(hours=2)
    pre_start = start_dt - timedelta(hours=2)
    if pre_start < day_date:
        pre_start = day_date
    pre_avg = _avg_bw_in_window(points, pre_start, start_dt)
    tail_avg = _avg_bw_in_window(points, tail_start, end_dt + timedelta(seconds=1))
    if tail_avg <= 1e-9:
        return False
    return pre_avg <= tail_avg * 0.5


def _append_reason(existing_reason, to_add):
    if not to_add:
        return existing_reason or ""
    if not existing_reason:
        return to_add
    parts = [p.strip() for p in str(existing_reason).split("；") if p.strip()]
    if to_add in parts:
        return "；".join(parts)
    return "；".join(parts + [to_add])

def get_95_peak_for_day(es, index_pattern, channel, day_date, specified_times_dict=None):
    """
    查询指定日期的95峰值，以及多个可选的指定时间点带宽，按isp字段细分
    day_date: datetime object (representing the start of the day 00:00:00)
    specified_times_dict: dict of {name: "HH:MM"}, optional
    """
    start_time = day_date
    end_time = day_date + timedelta(days=1) - timedelta(seconds=1) # 23:59:59
    
    # 转换为ISO格式字符串
    start_str = start_time.strftime("%Y-%m-%dT%H:%M:%S")
    end_str = end_time.strftime("%Y-%m-%dT%H:%M:%S")
    
    print(f"正在查询日期: {day_date.strftime('%Y-%m-%d')} (Channel: {channel})...")
    
    # 如果 specified_times_dict 是字符串，转换为字典（向下兼容旧调用）
    if isinstance(specified_times_dict, str):
        specified_times_dict = {"指定窗口时间": specified_times_dict}
    elif specified_times_dict is None:
        specified_times_dict = {}

    # 构造业务大盘过滤聚合
    business_filters = {
        biz_name: {"terms": {"job_id": job_ids}}
        for biz_name, job_ids in BUSINESS_JOB_IDS.items()
    }

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
            "by_business": {
                "filters": {
                    "filters": business_filters
                },
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
            },
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
            },
            "by_program_name": {
                "terms": {"field": "program_name", "size": 100},
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
            },
            "by_5min_total": { # 新增：用于计算渠道总流量
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
    
    try:
        resp = es.search(index=index_pattern, body=query, ignore_unavailable=True)
    except Exception as e:
        print(f"❌ 查询失败: {e}")
        return None

    isp_buckets = resp.get("aggregations", {}).get("by_isp", {}).get("buckets", [])
    program_name_buckets = resp.get("aggregations", {}).get("by_program_name", {}).get("buckets", [])
    total_5min_buckets = resp.get("aggregations", {}).get("by_5min_total", {}).get("buckets", [])
    business_buckets = resp.get("aggregations", {}).get("by_business", {}).get("buckets", {})
    
    if not isp_buckets and not program_name_buckets and not total_5min_buckets and not business_buckets:
        print(f"⚠️ 日期 {day_date.strftime('%Y-%m-%d')} 无数据")
        return None
        
    isp_results = []
    program_name_results = []
    business_results = []
    raw_data_points = [] # list of {date, timestamp, dimension, isp, program_name, bandwidth}

    # --- 处理分业务（job_id 过滤）的 95 峰值及差值 ---
    for biz_name, biz_bucket in business_buckets.items():
        buckets = biz_bucket.get("by_5min", {}).get("buckets", [])
        if not buckets: continue

        data_points = []
        biz_peak_time_bw = None
        target_biz_peak_time = specified_times_dict.get(biz_name)

        for bucket in buckets:
            sum_up_flow = bucket.get("total_up_flow", {}).get("value", 0)
            # 使用业务大盘逻辑：* 8 / 300
            avg_bw = (sum_up_flow * 8) / 300/1000000000
            
            ts_ms = bucket['key']
            ts_dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(timezone(timedelta(hours=8)))
            ts_dt = ts_dt.replace(tzinfo=None)
            ts_str = ts_dt.strftime("%H:%M")
            
            data_points.append({"timestamp": ts_dt, "bandwidth": avg_bw})
            
            if target_biz_peak_time and ts_str == target_biz_peak_time:
                biz_peak_time_bw = avg_bw

        sorted_points = sorted(data_points, key=lambda x: x["bandwidth"], reverse=True)
        count = len(sorted_points)
        if count > 0:
            rank = int(count * 0.05) + 1
            idx = rank - 1
            if idx < 0: idx = 0
            if idx >= count: idx = count - 1
            
            peak_bw_95 = sorted_points[idx]["bandwidth"]
            
            business_results.append({
                "date": day_date.strftime("%Y-%m-%d"),
                "biz_name": biz_name,
                "bandwidth_95": peak_bw_95,
                "peak_time_bandwidth": biz_peak_time_bw if biz_peak_time_bw is not None else 0,
                "diff": peak_bw_95 - (biz_peak_time_bw if biz_peak_time_bw is not None else 0),
                "peak_time": target_biz_peak_time
            })
    
    # --- 计算渠道总维度的 95 峰值 ---
    channel_data_points = []
    channel_specified_bandwidths = {} # {name: bandwidth}
    
    for bucket in total_5min_buckets:
        sum_up_flow = bucket.get("total_up_flow", {}).get("value", 0)
        avg_bw = (sum_up_flow * 8) / 300 / 1024/1000000
        
        ts_ms = bucket['key']
        ts_dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(timezone(timedelta(hours=8)))
        ts_dt = ts_dt.replace(tzinfo=None)
        ts_str = ts_dt.strftime("%H:%M")
        
        channel_data_points.append({
            "timestamp": ts_dt,
            "bandwidth": avg_bw
        })
        # 添加到明细数据
        raw_data_points.append({
            "日期": day_date.strftime("%Y-%m-%d"),
            "时间": ts_str,
            "维度": "渠道汇总",
            "运营商": "ALL",
            "节目名称": "ALL", # 渠道汇总维度不区分节目名称
            "上行带宽": avg_bw
        })
        
        for name, target_time in specified_times_dict.items():
            if ts_str == target_time:
                channel_specified_bandwidths[name] = avg_bw
            
    sorted_channel = sorted(channel_data_points, key=lambda x: x["bandwidth"], reverse=True)
    count_ch = len(sorted_channel)
    channel_result = None
    if count_ch > 0:
        # 修正 95 峰值计算逻辑
        rank_ch = int(count_ch * 0.05) + 1
        idx_ch = rank_ch - 1
        if idx_ch < 0: idx_ch = 0
        if idx_ch >= count_ch: idx_ch = count_ch - 1
        
        target_ch = sorted_channel[idx_ch]
        peak_ch = target_ch["bandwidth"]
        
        print(f"DEBUG: Channel Total, 日期 {day_date.strftime('%Y-%m-%d')}, 总点数: {count_ch}, 95%位置: 第 {rank_ch} 个, 时间 {target_ch['timestamp'].strftime('%H:%M')}, 峰值 {peak_ch:.4f}")
        
        channel_result = {
            "date": day_date.strftime("%Y-%m-%d"),
            "bandwidth": peak_ch,
            "time_point": target_ch["timestamp"].strftime("%H:%M"),
            "specified_times": {} # 存放多个指定时间的数据
        }
        
        for name, target_time in specified_times_dict.items():
            bw = channel_specified_bandwidths.get(name, 0)
            channel_result["specified_times"][name] = {
                "bandwidth": bw,
                "diff": peak_ch - bw,
                "time": target_time
            }
            
    # --- 处理 ISP 维度数据 ---
    for isp_bucket in isp_buckets:
        isp_name = isp_bucket["key"]
        buckets = isp_bucket.get("by_5min", {}).get("buckets", [])
        
        # 处理数据
        data_points = []
        isp_specified_bandwidths = {}
        
        for bucket in buckets:
            sum_up_flow = bucket.get("total_up_flow", {}).get("value", 0)
            avg_bw = (sum_up_flow * 8) / 300 / 1024/1000000
            
            ts_ms = bucket['key']
            ts_dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(timezone(timedelta(hours=8)))
            ts_dt = ts_dt.replace(tzinfo=None)
            ts_str = ts_dt.strftime("%H:%M")
            
            data_points.append({
                "timestamp": ts_dt,
                "bandwidth": avg_bw
            })
            
            # 添加到明细数据
            raw_data_points.append({
                "日期": day_date.strftime("%Y-%m-%d"),
                "时间": ts_str,
                "维度": "运营商细分",
                "运营商": isp_name,
                "节目名称": "ALL", # ISP 维度不区分节目名称
                "上行带宽": avg_bw
            })
            
            for name, target_time in specified_times_dict.items():
                if ts_str == target_time:
                    isp_specified_bandwidths[name] = avg_bw
        
        # 计算该 ISP 的 95 峰值
        sorted_points = sorted(data_points, key=lambda x: x["bandwidth"], reverse=True)
        count = len(sorted_points)
        if count == 0:
            continue
            
        # 修正 95 峰值计算逻辑
        rank = int(count * 0.05) + 1
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
            "time_point": target_point["timestamp"].strftime("%H:%M"),
            "specified_times": {}
        }
        
        for name, target_time in specified_times_dict.items():
            bw = isp_specified_bandwidths.get(name, 0)
            res["specified_times"][name] = {
                "bandwidth": bw,
                "diff": peak_95_bandwidth - bw,
                "time": target_time
            }
            
        isp_results.append(res)
        
    # --- 处理 program_name 维度数据 (嵌套 isp) ---
    for program_bucket in program_name_buckets:
        program_name = program_bucket["key"]
        isp_buckets_for_prog = program_bucket.get("by_isp", {}).get("buckets", [])
        
        for isp_bucket in isp_buckets_for_prog:
            isp_name = isp_bucket["key"]
            buckets = isp_bucket.get("by_5min", {}).get("buckets", [])
            
            data_points = []
            # Special handling for aurora
            aurora_api_time_bandwidth = None
            # The key for the API time is "渠道在当天大盘95时间点"
            api_time_str = specified_times_dict.get("渠道在当天大盘95时间点")
            daily_peak_bandwidth = None
            daily_peak_time = None

            for bucket in buckets:
                sum_up_flow = bucket.get("total_up_flow", {}).get("value", 0)
                # 统一单位为 Gbps
                avg_bw = (sum_up_flow * 8) / 300 / 1_000_000_000
                
                ts_ms = bucket['key']
                ts_dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(timezone(timedelta(hours=8)))
                ts_dt = ts_dt.replace(tzinfo=None)
                
                data_points.append({
                    "timestamp": ts_dt,
                    "bandwidth": avg_bw
                })

                if daily_peak_bandwidth is None or avg_bw > daily_peak_bandwidth:
                    daily_peak_bandwidth = avg_bw
                    daily_peak_time = ts_dt.strftime("%H:%M")

                # If it's aurora, find its bandwidth at the API peak time
                if program_name == "aurora" and api_time_str and ts_dt.strftime("%H:%M") == api_time_str:
                    aurora_api_time_bandwidth = avg_bw
                
                # 添加到明细数据
                raw_data_points.append({
                    "日期": day_date.strftime("%Y-%m-%d"),
                    "时间": ts_dt.strftime("%H:%M"),
                    "维度": "节目名称细分",
                    "运营商": isp_name,
                    "节目名称": program_name,
                    "上行带宽": avg_bw
                })
                
            sorted_points = sorted(data_points, key=lambda x: x["bandwidth"], reverse=True)
            count = len(sorted_points)
            if count == 0:
                continue
                
            # 修正 95 峰值计算逻辑
            rank = int(count * 0.05) + 1
            index = rank - 1
            if index < 0: index = 0
            if index >= count: index = count - 1
            
            target_point = sorted_points[index]
            peak_95_bandwidth = target_point["bandwidth"]
            
            print(f"DEBUG: Program Name {program_name} (ISP: {isp_name}), 日期 {day_date.strftime('%Y-%m-%d')}, 总点数: {count}, 95%位置: 第 {rank} 个, 时间 {target_point['timestamp'].strftime('%H:%M')}, 峰值 {peak_95_bandwidth:.4f}")
            
            program_result = {
                "date": day_date.strftime("%Y-%m-%d"),
                "program_name": program_name,
                "isp": isp_name,
                "bandwidth": peak_95_bandwidth,
                "time_point": target_point["timestamp"].strftime("%H:%M"),
                "daily_peak_bandwidth": daily_peak_bandwidth if daily_peak_bandwidth is not None else 0,
                "daily_peak_time": daily_peak_time if daily_peak_time is not None else ""
            }

            # Add the special diff for aurora
            if program_name == "aurora" and aurora_api_time_bandwidth is not None:
                program_result["aurora_diff"] = peak_95_bandwidth - aurora_api_time_bandwidth

            program_name_results.append(program_result)
        
    return {
        "channel_peak": channel_result,
        "isp_peaks": isp_results,
        "program_peaks": program_name_results,
        "business_peaks": business_results,
        "raw_data_points": raw_data_points
    }



def scan_early_peak_channels(es, start_date, end_date, output_dir: str, scan_mode: str):
    """
    scan_mode:
      - "early_peak": 查找 95 峰值在 08:00-17:00 的渠道（按 ISP + 渠道汇总）
      - "offline_before_evening": 查找晚高峰前离线渠道（按渠道汇总），并展示该渠道的日 95 峰值与峰值时刻
      - "precise_shift": 精准查询错峰渠道：若(95峰值 - 大盘计费时刻带宽) > 1G，标注为错峰渠道
    """
    if scan_mode == "offline_before_evening":
        print(f"\n🚀 开始扫描 {start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')} 晚高峰前离线渠道...")
    elif scan_mode == "precise_shift":
        print(f"\n🚀 开始扫描 {start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')} 精准查询错峰渠道(差值>1G)...")
    else:
        print(f"\n🚀 开始扫描 {start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')} 期间所有渠道和运营商的95峰值...")
    
    target_records = []
    
    current_date = start_date
    while current_date <= end_date:
        day_str = current_date.strftime('%Y-%m-%d')
        print(f"\nProcessing {day_str}...")

        api_time_str = None
        if scan_mode == "precise_shift":
            api_time_str = get_billing_time_from_api(current_date)
            if not api_time_str:
                print(f"⚠️ {day_str} 未获取到接口计费时间，跳过当天精准错峰扫描")
                current_date += timedelta(days=1)
                continue
        
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
        speed_cache_for_day = {}
        
        for ch_bucket in channels_buckets:
            channel_name = ch_bucket["key"]
            isp_buckets = ch_bucket.get("by_isp", {}).get("buckets", [])
            
            # 用来存储整个渠道的总和点位
            ch_total_points = {} # timestamp -> sum_up_flow
            
            for isp_bucket in isp_buckets:
                isp_name = isp_bucket["key"]
                buckets = isp_bucket.get("by_5min", {}).get("buckets", [])
                
                if scan_mode in {"offline_before_evening", "precise_shift"}:
                    for bucket in buckets:
                        sum_up_flow = bucket.get("total_up_flow", {}).get("value", 0)
                        ts_ms = bucket.get("key")
                        if ts_ms is None:
                            continue
                        ts_dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(timezone(timedelta(hours=8)))
                        ts_dt = ts_dt.replace(tzinfo=None)
                        if ts_dt not in ch_total_points:
                            ch_total_points[ts_dt] = 0
                        ch_total_points[ts_dt] += sum_up_flow
                else:
                    data_points = []
                    for bucket in buckets:
                        sum_up_flow = bucket.get("total_up_flow", {}).get("value", 0)
                        avg_bw = (sum_up_flow * 8) / 300 / 1024/1000000
                        
                        ts_ms = bucket['key']
                        ts_dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(timezone(timedelta(hours=8)))
                        ts_dt = ts_dt.replace(tzinfo=None)
                        
                        data_points.append({"timestamp": ts_dt, "bandwidth": avg_bw})
                        
                        if ts_dt not in ch_total_points: ch_total_points[ts_dt] = 0
                        ch_total_points[ts_dt] += sum_up_flow
                        
                    sorted_points = sorted(data_points, key=lambda x: x["bandwidth"], reverse=True)
                    count = len(sorted_points)
                    if count > 0:
                        rank = int(count * 0.05) + 1
                        idx = rank - 1
                        if idx < 0: idx = 0
                        if idx >= count: idx = count - 1
                        
                        target_point = sorted_points[idx]
                        peak_time = target_point["timestamp"]
                        if 8 <= peak_time.hour <= 17:
                            shift_cost = target_point["bandwidth"]
                            up_speeds = speed_cache_for_day.get(channel_name)
                            if up_speeds is None:
                                up_speeds = get_total_up_speed_gb_at_times(es, current_date, channel_name, ["00:00", "10:00", "19:00"])
                                speed_cache_for_day[channel_name] = up_speeds
                            up00 = up_speeds.get("00:00", 0)
                            up10 = up_speeds.get("10:00", 0)
                            up19 = up_speeds.get("19:00", 0)
                            diff_speed = up00 - up19
                            threshold_speed = up10 * 0.05
                            reason = identify_shift_reason(shift_cost, up00, up10, up19)
                            target_records.append({
                                "时间戳": peak_time.strftime("%Y-%m-%d %H:%M"),
                                "维度": "运营商细分",
                                "运营商": isp_name,
                                "上行带宽": target_point["bandwidth"],
                                "渠道ID": channel_name,
                                "00:00_total_up_speed(Gb)": up00,
                                "10:00_total_up_speed(Gb)": up10,
                                "19:00_total_up_speed(Gb)": up19,
                                "00-19差值(Gb)": diff_speed,
                                "阈值(10:00*5%)(Gb)": threshold_speed,
                                "错峰原因": reason
                            })
            
            # Calculate Channel Total 95 peak
            ch_data_points = []
            for ts, total_flow in ch_total_points.items():
                if scan_mode == "precise_shift":
                    ch_data_points.append({"timestamp": ts, "bandwidth": (total_flow * 8) / 300 / 1024 / 1_000_000})
                else:
                    ch_data_points.append({"timestamp": ts, "bandwidth": (total_flow * 8) / 300 / 1_000_000_000})
            
            sorted_ch = sorted(ch_data_points, key=lambda x: x["bandwidth"], reverse=True)
            if sorted_ch:
                rank_ch = int(len(sorted_ch) * 0.05) + 1
                idx_ch = rank_ch - 1
                if idx_ch < 0: idx_ch = 0
                if idx_ch >= len(sorted_ch): idx_ch = len(sorted_ch) - 1
                
                target_ch = sorted_ch[idx_ch]

                if scan_mode == "precise_shift":
                    billing_bw = None
                    for p in ch_data_points:
                        if p["timestamp"].strftime("%H:%M") == api_time_str:
                            billing_bw = p["bandwidth"]
                            break
                    if billing_bw is None:
                        continue
                    diff_bw = target_ch["bandwidth"] - billing_bw
                    if diff_bw > 1.0:
                        target_records.append({
                            "日期": day_str,
                            "渠道ID": channel_name,
                            "日95峰值": target_ch["bandwidth"],
                            "峰值时刻": target_ch["timestamp"].strftime("%H:%M"),
                            "大盘计费时刻": api_time_str,
                            "大盘时刻带宽": billing_bw,
                            "错峰带宽差": diff_bw,
                        })
                    continue

                up_speeds = speed_cache_for_day.get(channel_name)
                if up_speeds is None:
                    up_speeds = get_total_up_speed_gb_at_times(es, current_date, channel_name, ["00:00", "10:00", "19:00"])
                    speed_cache_for_day[channel_name] = up_speeds
                up00 = up_speeds.get("00:00", 0)
                up10 = up_speeds.get("10:00", 0)
                up19 = up_speeds.get("19:00", 0)
                diff_speed = up00 - up19
                threshold_speed = up10 * 0.05

                if scan_mode == "offline_before_evening":
                    offline = (up00 > up19) and (diff_speed > threshold_speed)
                    if offline:
                        target_records.append({
                            "日期": day_str,
                            "渠道ID": channel_name,
                            "日95峰值": target_ch["bandwidth"],
                            "峰值时刻": target_ch["timestamp"].strftime("%H:%M"),
                            "00:00_total_up_speed(Gb)": up00,
                            "10:00_total_up_speed(Gb)": up10,
                            "19:00_total_up_speed(Gb)": up19,
                            "00-19差值(Gb)": diff_speed,
                            "阈值(10:00*5%)(Gb)": threshold_speed,
                            "错峰原因": "供应商晚高峰前下架",
                        })
                else:
                    if 8 <= target_ch["timestamp"].hour <= 17:
                        shift_cost = target_ch["bandwidth"]
                        reason = identify_shift_reason(shift_cost, up00, up10, up19)
                        target_records.append({
                            "时间戳": target_ch["timestamp"].strftime("%Y-%m-%d %H:%M"),
                            "维度": "渠道汇总",
                            "运营商": "ALL",
                            "上行带宽": target_ch["bandwidth"],
                            "渠道ID": channel_name,
                            "00:00_total_up_speed(Gb)": up00,
                            "10:00_total_up_speed(Gb)": up10,
                            "19:00_total_up_speed(Gb)": up19,
                            "00-19差值(Gb)": diff_speed,
                            "阈值(10:00*5%)(Gb)": threshold_speed,
                            "错峰原因": reason
                        })
                
        current_date += timedelta(days=1)
        
    # Save to Excel
    if target_records:
        df = pd.DataFrame(target_records)
        if scan_mode == "offline_before_evening":
            df = df[["日期", "渠道ID", "日95峰值", "峰值时刻", "00:00_total_up_speed(Gb)", "10:00_total_up_speed(Gb)", "19:00_total_up_speed(Gb)", "00-19差值(Gb)", "阈值(10:00*5%)(Gb)", "错峰原因"]]
            df = _clean_df_for_excel(df)
            output_file = os.path.join(
                output_dir,
                _safe_filename(f"晚高峰前离线渠道汇总_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}") + ".xlsx",
            )
            output_file = _unique_output_path(output_file)
        elif scan_mode == "precise_shift":
            df = df[["日期", "渠道ID", "日95峰值", "峰值时刻", "大盘计费时刻", "大盘时刻带宽", "错峰带宽差"]]
            df = _clean_df_for_excel(df)
            output_file = os.path.join(
                output_dir,
                _safe_filename(f"精准查询错峰渠道汇总_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}") + ".xlsx",
            )
            output_file = _unique_output_path(output_file)
        else:
            df = df[["时间戳", "维度", "运营商", "上行带宽", "渠道ID", "00:00_total_up_speed(Gb)", "10:00_total_up_speed(Gb)", "19:00_total_up_speed(Gb)", "00-19差值(Gb)", "阈值(10:00*5%)(Gb)", "错峰原因"]]
            df = _clean_df_for_excel(df)
            output_file = os.path.join(
                output_dir,
                _safe_filename(f"8点-17点峰值汇总_综合_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}") + ".xlsx",
            )
            output_file = _unique_output_path(output_file)
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
    output_base_dir = input("请输入输出目录 (默认./output): ").strip()
    output_dir = _resolve_output_dir(output_base_dir)
    
    if choice == "2":
        print("\n功能2查询类型:")
        print("1. 查找95峰值在08:00-17:00的渠道")
        print("2. 查找晚高峰前离线的渠道")
        print("3. 精准查询错峰渠道(大盘计费时刻差值>1G)")
        scan_choice = input("请选择 (1/2/3, 默认1): ").strip()
        if scan_choice == "2":
            scan_mode = "offline_before_evening"
        elif scan_choice == "3":
            scan_mode = "precise_shift"
        else:
            scan_mode = "early_peak"
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
        
        scan_early_peak_channels(es, start_date, end_date, output_dir, scan_mode)
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
    
    print("\n输出模式:")
    print("1. 精简版 (只输出汇总结果)")
    print("2. 详细版 (包含汇总及各渠道明细)")
    output_mode_choice = input("请选择输出模式 (1/2, 默认1): ").strip()
    is_brief_mode = output_mode_choice != "2"
    
    print("\n对比时间源:")
    print("1. ERN (仅接口大盘时刻)")
    print("2. EDS (仅业务大盘时刻: 快手, 字节, 小度)")
    print("3. 全部 (ERN + EDS)")
    print("4. 手动输入 (HH:MM)")
    time_source_choice = input("请选择对比时间源 (1/2/3/4, 默认3): ").strip()

    specified_time_str = None
    # 默认获取全部
    fetch_ern = time_source_choice in ['1', '3', '']
    fetch_eds = time_source_choice in ['2', '3', '']

    if time_source_choice == '4':
        specified_time_str = input("请输入指定窗口时间 (HH:MM): ").strip()
        try:
            datetime.strptime(specified_time_str, "%H:%M")
        except ValueError:
            print("❌ 时间格式错误，请使用 HH:MM 格式")
            return
        # 手动模式下，不自动获取任何数据
        fetch_ern = False
        fetch_eds = False
    
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
    daily_business_times = {} # {date_str: {biz_name: time_str}}
    api_times_records = []
    
    if fetch_ern or fetch_eds:
        print("\n>>> 正在批量获取对比时间源数据... <<<")
        temp_date = start_date
        while temp_date <= end_date:
            current_date_str = temp_date.strftime('%Y-%m-%d')
            curr_index = f"eds_billing-{temp_date.strftime('%Y%m%d')}"
            prev_date = temp_date - timedelta(days=1)
            prev_index = f"eds_billing-{prev_date.strftime('%Y%m%d')}"
            target_indices = f"{prev_index},{curr_index}"
            
            # 1. 获取接口计费时间 (ERN)
            if fetch_ern:
                time_str = get_billing_time_from_api(temp_date)
                if time_str:
                    daily_specified_times[current_date_str] = time_str
                    
            # 2. 获取业务大盘峰值时间与带宽 (EDS)
            if fetch_eds:
                biz_data = get_business_95_peak_times(es, target_indices, temp_date)
                daily_business_times[current_date_str] = biz_data
            
            # 记录汇总到接口时间表
            record = {"日期": current_date_str}
            if fetch_ern:
                record["接口获取时间"] = daily_specified_times.get(current_date_str, "N/A")
            if fetch_eds:
                for biz_name, data in daily_business_times.get(current_date_str, {}).items():
                    record[f"{biz_name}峰值时刻"] = data["time"]
                    record[f"{biz_name}95带宽"] = data["bandwidth"]
            api_times_records.append(record)
            
            temp_date += timedelta(days=1)
    
    all_results = {} # channel -> { 'total': [], 'isp': [], 'raw': [], 'program': [], 'business': [] }
    early_peak_records = [] 
    diff_summary_records = [] # 用于汇总所有渠道的带宽差
    program_name_summary_records = [] # 用于汇总所有渠道的节目名称峰值
    business_diff_records = [] # 用于汇总所有渠道的分业务错峰带宽差
    business_pivot_records = [] # 新增：汇总天维度各个渠道的业务带宽差值 (透视格式)
    speed_snapshot_cache = {} # (date_str, channel) -> {"00:00": gb, "10:00": gb, "19:00": gb}
    vendor_two_hours_cache = {}
    
    for channel in srm_channels:
        print(f"\n>>> 正在处理渠道: {channel} <<<")
        channel_results = {'total': [], 'isp': [], 'raw': [], 'program': [], 'business': []}
        current_date = start_date
        while current_date <= end_date:
            current_date_str = current_date.strftime('%Y-%m-%d')
            
            # 构建该日期的所有指定时间点
            specified_times_for_day = {}
            
            # a. 添加大盘时间 (API 或手动输入)
            if fetch_ern:
                api_time = daily_specified_times.get(current_date_str)
                if api_time:
                    specified_times_for_day["渠道在当天大盘95时间点"] = api_time
            elif specified_time_str:
                specified_times_for_day["指定窗口时间"] = specified_time_str
            
            # b. 添加业务大盘时间
            if fetch_eds:
                biz_data = daily_business_times.get(current_date_str, {})
                for biz_name, data in biz_data.items():
                    specified_times_for_day[biz_name] = data["time"]
            
            curr_index = f"eds_billing-{current_date.strftime('%Y%m%d')}"
            prev_date = current_date - timedelta(days=1)
            prev_index = f"eds_billing-{prev_date.strftime('%Y%m%d')}"
            target_indices = f"{prev_index},{curr_index}"
            
            day_data = get_95_peak_for_day(es, target_indices, channel, current_date, specified_times_for_day)
            if day_data:
                vendor_two_hours = False
                if fetch_eds:
                    vendor_key = (current_date_str, channel)
                    cached_vendor_two_hours = vendor_two_hours_cache.get(vendor_key)
                    if cached_vendor_two_hours is None:
                        cached_vendor_two_hours = _is_vendor_run_two_hours(
                            es, target_indices, channel, current_date, "bytedance.server.linux"
                        )
                        vendor_two_hours_cache[vendor_key] = cached_vendor_two_hours
                    vendor_two_hours = bool(cached_vendor_two_hours)
                active_businesses = set()
                for biz_res in day_data.get("business_peaks", []) or []:
                    biz_name = biz_res.get("biz_name")
                    if biz_name:
                        active_businesses.add(biz_name)
                if day_data['channel_peak']:
                    peak_data = day_data['channel_peak']
                    channel_results['total'].append(peak_data)
                    
                    # 收集带宽差汇总数据
                    row = {
                        "日期": peak_data["date"],
                        "维度": "渠道汇总",
                        "运营商": "ALL",
                        "渠道ID": channel,
                        "95峰值": peak_data["bandwidth"],
                        "95峰值时间戳": f"{peak_data['date']} {peak_data.get('time_point', '')}" if peak_data.get("time_point") else "",
                        "渠道备注": "供应商只跑两小时" if vendor_two_hours else ""
                    }
                    # 添加各个指定时间的数据
                    for name, info in peak_data.get("specified_times", {}).items():
                        if name in BUSINESS_JOB_IDS and name not in active_businesses:
                            continue
                        row[f"{name}带宽"] = info["bandwidth"]
                        row[f"{name}差值"] = info["diff"]
                    shift_cost = row.get("渠道在当天大盘95时间点差值", 0)
                    reason = ""
                    if shift_cost > 0:
                        cache_key = (peak_data["date"], channel)
                        speeds = speed_snapshot_cache.get(cache_key)
                        if speeds is None:
                            speeds = get_total_up_speed_gb_at_times(es, current_date, channel, ["00:00", "10:00", "19:00"])
                            speed_snapshot_cache[cache_key] = speeds
                        up00 = speeds.get("00:00", 0)
                        up10 = speeds.get("10:00", 0)
                        up19 = speeds.get("19:00", 0)
                        row["00:00_total_up_speed(Gb)"] = up00
                        row["10:00_total_up_speed(Gb)"] = up10
                        row["19:00_total_up_speed(Gb)"] = up19
                        row["00-19差值(Gb)"] = up00 - up19
                        row["阈值(10:00*5%)(Gb)"] = up10 * 0.05
                        reason = identify_shift_reason(shift_cost, up00, up10, up19)
                        if vendor_two_hours:
                            reason = _append_reason(reason, "供应商只跑两小时")
                    row["错峰原因"] = reason
                    diff_summary_records.append(row)

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
                    
                    # 收集运营商维度的带宽差汇总数据
                    row = {
                        "日期": isp_res["date"],
                        "维度": "运营商细分",
                        "运营商": isp_res.get("isp", "unknown"),
                        "渠道ID": channel,
                        "95峰值": isp_res["bandwidth"],
                        "95峰值时间戳": f"{isp_res['date']} {isp_res.get('time_point', '')}" if isp_res.get("time_point") else "",
                        "渠道备注": "供应商只跑两小时" if vendor_two_hours else ""
                    }
                    for name, info in isp_res.get("specified_times", {}).items():
                        if name in BUSINESS_JOB_IDS and name not in active_businesses:
                            continue
                        row[f"{name}带宽"] = info["bandwidth"]
                        row[f"{name}差值"] = info["diff"]
                    shift_cost = row.get("渠道在当天大盘95时间点差值", 0)
                    reason = ""
                    if shift_cost > 0:
                        cache_key = (isp_res["date"], channel)
                        speeds = speed_snapshot_cache.get(cache_key)
                        if speeds is None:
                            speeds = get_total_up_speed_gb_at_times(es, current_date, channel, ["00:00", "10:00", "19:00"])
                            speed_snapshot_cache[cache_key] = speeds
                        up00 = speeds.get("00:00", 0)
                        up10 = speeds.get("10:00", 0)
                        up19 = speeds.get("19:00", 0)
                        row["00:00_total_up_speed(Gb)"] = up00
                        row["10:00_total_up_speed(Gb)"] = up10
                        row["19:00_total_up_speed(Gb)"] = up19
                        row["00-19差值(Gb)"] = up00 - up19
                        row["阈值(10:00*5%)(Gb)"] = up10 * 0.05
                        reason = identify_shift_reason(shift_cost, up00, up10, up19)
                        if vendor_two_hours:
                            reason = _append_reason(reason, "供应商只跑两小时")
                    row["错峰原因"] = reason
                    diff_summary_records.append(row)

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
                
                # 收集分业务错峰带宽差数据
                if 'business_peaks' in day_data:
                    pivot_row = {
                        "日期": current_date_str,
                        "渠道ID": channel
                    }
                    for biz_res in day_data['business_peaks']:
                        biz_name = biz_res["biz_name"]
                        business_diff_records.append({
                            "日期": biz_res["date"],
                            "渠道ID": channel,
                            "业务类型": biz_name,
                            "业务日95带宽": biz_res["bandwidth_95"],
                            "业务大盘峰值时刻": biz_res["peak_time"],
                            "大盘时刻渠道带宽": biz_res["peak_time_bandwidth"],
                            "错峰带宽差": biz_res["diff"]
                        })
                        # 同时加入透视行数据
                        pivot_row[f"{biz_name}带宽差"] = biz_res["diff"]
                        channel_results['business'].append(biz_res)
                    
                    if len(pivot_row) > 2: # 只有包含业务数据时才记录
                        business_pivot_records.append(pivot_row)

                # 收集节目名称维度的峰值数据
                if 'program_peaks' in day_data and day_data['program_peaks']:
                    for program_res in day_data['program_peaks']:
                        record = {
                            "日期": program_res["date"],
                            "渠道ID": channel,
                            "节目名称": program_res["program_name"],
                            "运营商": program_res.get("isp", "ALL"),
                            "95峰值": program_res["bandwidth"],
                            "峰值时间": program_res["time_point"],
                            "渠道分节目的单日峰值带宽": program_res.get("daily_peak_bandwidth", 0),
                            "分业务单日峰值时刻": program_res.get("daily_peak_time", "")
                        }
                        if "aurora_diff" in program_res:
                            record["Aurora错峰带宽"] = program_res["aurora_diff"]
                        
                        program_name_summary_records.append(record)
                        channel_results['program'].append(program_res) # 也添加到 channel_results
                
                # 收集明细数据
                if 'raw_data_points' in day_data:
                    channel_results['raw'].extend(day_data['raw_data_points'])
                    
            current_date += timedelta(days=1)
            
        if channel_results['total'] or channel_results['isp'] or channel_results['program'] or channel_results['business']:
            all_results[channel] = channel_results
        
    if all_results:
        output_file = os.path.join(
            output_dir,
            _build_mode1_output_filename(fetch_ern, fetch_eds, specified_time_str, start_date_str, end_date_str),
        )
        output_file = _unique_output_path(output_file)
        try:
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                used_sheet_names = set()
                if early_peak_records:
                    df_early = pd.DataFrame(early_peak_records)
                    df_early = df_early[["时间戳", "维度", "运营商", "上行带宽", "渠道ID"]]
                    df_early = _clean_df_for_excel(df_early)
                    df_early.to_excel(writer, sheet_name=_unique_sheet_title("12点前峰值汇总", used_sheet_names), index=False)
                
                if api_times_records:
                    df_api_times = pd.DataFrame(api_times_records)
                    df_api_times = _clean_df_for_excel(df_api_times)
                    df_api_times.to_excel(writer, sheet_name=_unique_sheet_title("接口获取时间", used_sheet_names), index=False)
                    print(f"✅ 已写入 {len(api_times_records)} 条记录到 Sheet: 接口获取时间")

                if diff_summary_records:
                    df_diff = pd.DataFrame(diff_summary_records)
                    # 调整列顺序：将基础列放在前面，其他（业务带宽/差值）按字母顺序跟在后面
                    base_cols = ["日期", "维度", "运营商", "渠道ID", "95峰值", "95峰值时间戳", "00:00_total_up_speed(Gb)", "10:00_total_up_speed(Gb)", "19:00_total_up_speed(Gb)", "00-19差值(Gb)", "阈值(10:00*5%)(Gb)", "渠道备注", "错峰原因"]
                    for c in base_cols:
                        if c not in df_diff.columns:
                            if c in {"日期", "维度", "运营商", "渠道ID", "95峰值时间戳", "渠道备注", "错峰原因"}:
                                df_diff[c] = ""
                            else:
                                df_diff[c] = 0
                    other_cols = sorted([c for c in df_diff.columns if c not in base_cols])
                    df_diff = df_diff[base_cols + other_cols]
                    df_diff = _clean_df_for_excel(df_diff)
                    df_diff.to_excel(writer, sheet_name=_unique_sheet_title("渠道带宽差汇总", used_sheet_names), index=False)
                    print(f"✅ 已写入 {len(diff_summary_records)} 条记录到 Sheet: 渠道带宽差汇总")
                
                if program_name_summary_records:
                    df_program = pd.DataFrame(program_name_summary_records)
                    df_program = _clean_df_for_excel(df_program)
                    df_program.to_excel(writer, sheet_name=_unique_sheet_title("按节目名称95峰值汇总", used_sheet_names), index=False)
                    print(f"✅ 已写入 {len(program_name_summary_records)} 条记录到 Sheet: 按节目名称95峰值汇总")
                
                if business_diff_records:
                    df_biz_diff = pd.DataFrame(business_diff_records)
                    df_biz_diff = _clean_df_for_excel(df_biz_diff)
                    df_biz_diff.to_excel(writer, sheet_name=_unique_sheet_title("分业务错峰带宽差", used_sheet_names), index=False)
                    print(f"✅ 已写入 {len(business_diff_records)} 条记录到 Sheet: 分业务错峰带宽差")
                
                if business_pivot_records:
                    df_pivot = pd.DataFrame(business_pivot_records)
                    # 排序：日期、渠道ID
                    df_pivot = df_pivot.sort_values(by=["日期", "渠道ID"])
                    df_pivot = _clean_df_for_excel(df_pivot)
                    df_pivot.to_excel(writer, sheet_name=_unique_sheet_title("业务带宽差值汇总", used_sheet_names), index=False)
                    print(f"✅ 已写入 {len(business_pivot_records)} 条记录到 Sheet: 业务带宽差值汇总")
                
                if not is_brief_mode:
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
                        
                        # 处理 specified_times 列，将其展开为多列
                        if 'specified_times' in df_combined.columns:
                            # 遍历每一行，展开字典
                            for idx, row in df_combined.iterrows():
                                spec_times = row['specified_times']
                                if isinstance(spec_times, dict):
                                    for name, info in spec_times.items():
                                        df_combined.at[idx, f"{name}带宽"] = info["bandwidth"]
                                        df_combined.at[idx, f"{name}差值"] = info["diff"]
                            # 删除原始字典列
                            df_combined = df_combined.drop(columns=['specified_times'])

                        rename_map = {
                            "bandwidth": "当天上行带宽", 
                            "date": "日期", 
                            "time_point": "当天上行带宽对应时间"
                        }
                        df_combined = df_combined.rename(columns=rename_map)
                        
                        # 重新排序列顺序
                        cols = ["日期", "维度", "运营商", "当天上行带宽", "当天上行带宽对应时间"]
                        # 将动态生成的带宽和差值列加入
                        dynamic_cols = sorted([c for c in df_combined.columns if c not in cols])
                        final_cols = [c for c in (cols + dynamic_cols) if c in df_combined.columns]
                        df_combined = df_combined[final_cols]
                        df_combined = _clean_df_for_excel(df_combined)
                        
                        sheet_name = _unique_sheet_title(f"{channel}_95汇总", used_sheet_names)
                        df_combined.to_excel(writer, sheet_name=sheet_name, index=False)
                        
                        # Sheet 2: 明细数据
                        if results['raw']:
                            df_raw = pd.DataFrame(results['raw'])
                            # 排序：日期、维度、时间、运营商
                            df_raw = df_raw.sort_values(by=["日期", "维度", "时间", "运营商"])
                            df_raw = _clean_df_for_excel(df_raw)
                            raw_sheet_name = _unique_sheet_title(f"{channel}_明细", used_sheet_names)
                            df_raw.to_excel(writer, sheet_name=raw_sheet_name, index=False)
                            print(f"✅ 渠道 {channel} 数据已写入 Sheet: {sheet_name} 和 {raw_sheet_name}")
                        else:
                            print(f"✅ 渠道 {channel} 数据已写入 Sheet: {sheet_name}")
                else:
                    print("\nℹ️ 已选择精简模式，跳过渠道明细 Sheet 生成。")
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

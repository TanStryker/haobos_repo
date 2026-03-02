import json
import uuid
import time
import re
import pandas as pd
import matplotlib.pyplot as plt
from elasticsearch import Elasticsearch, ApiError, AuthenticationException
from openai import OpenAI
from google import genai
from google.genai import types

# ---------------------- 1. 基础配置 ----------------------
#ES配置
ES_URL = "http://e.es.kingdata.ksyun.com:9200"
ES_USERNAME = "readonly"
ES_PASSWORD = "re2)f1MaFsa"

# LLM 配置
LLM_PROVIDER = "gemini"  # 可选：gemini / deepseek

# Gemini 配置
GEMINI_API_KEY = "AIzaSyCzYJE_fpxYH3yDVgmzSK1q0GXHH2HXL2c"
GEMINI_MODEL = "gemini-2.0-flash"

# DeepSeek 配置
DEEPSEEK_API_KEY = "sk-53e569888e054f5f9775b2dd6dfc82d2"
DEEPSEEK_BASE_URL = "https://api.deepseek.com"
DEEPSEEK_MODEL = "deepseek-reasoner"
# 硬件运维索引前缀（支持 eds_line_heartbeat-YYYYMMDD）
ES_HW_INDEX_PREFIX = "eds_line_heartbeat"

# 调优规则库（针对新的字段名）
OPTIMIZE_RULES = {
    "ping_loss_v4": {
        "threshold": 5.0,
        "warning": "IPv4 丢包率超过 5%",
        "suggestion": ["检查 IPv4 链路质量", "排查交换机 IPv4 端口负载"]
    },
    "ping_loss_v6": {
        "threshold": 5.0,
        "warning": "IPv6 丢包率超过 5%",
        "suggestion": ["检查 IPv6 路由配置", "排查双栈链路切换点"]
    },
    "retrans": {
        "threshold": 3.0,
        "warning": "设备重传率超过 3%",
        "suggestion": ["优化 TCP 窗口", "扩容网络带宽"]
    },
    "ping_v4": {
        "threshold": 100.0,
        "warning": "IPv4 延时超过 100ms",
        "suggestion": ["优化 IPv4 路由策略", "检查网关设备性能"]
    },
    "ping_v6": {
        "threshold": 120.0,
        "warning": "IPv6 延时超过 120ms",
        "suggestion": ["检查 IPv6 隧道或专线质量", "排查 IPv6 邻居发现机制"]
    },
    "test_speed_v4": {
        "threshold": 50.0, # 示例：低于50Mbps告警
        "warning": "IPv4 测速带宽偏低",
        "suggestion": ["检查宽带账号限速", "排查物理链路衰减"]
    }
}

# ---------------------- 2. NLU解析模块（支持 多模型 + 规则兜底） ----------------------
class NLUParser:
    def __init__(self):
        self.provider = LLM_PROVIDER
        # 初始化各模型客户端
        self.gemini_client = genai.Client(api_key=GEMINI_API_KEY)
        self.deepseek_client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url=DEEPSEEK_BASE_URL)

    def _rule_based_parse(self, user_text):
        """规则兜底：针对固定格式的指令进行正则解析"""
        structured_cmd = {
            "time_range": {"start": "now-1d/d", "end": "now/d"},
            "machine_codes": [],
            "analysis_dimensions": ["ping_v4", "ping_loss_v4", "retrans"],
            "analysis_type": "summary"
        }
        
        # 1. 先提取设备ID ( machine_code )，并从原文本中移除，防止干扰日期识别
        mc_match = re.search(r"ID为([A-Z0-9]+)", user_text)
        text_for_date = user_text
        if mc_match:
            structured_cmd["machine_codes"] = [mc_match.group(1)]
            text_for_date = user_text.replace(mc_match.group(0), "")
            
        # 2. 提取日期 ( 必须是 20 开头的 8 位或 10 位数字 )
        # 匹配 2026-02-26 或 20260226
        date_match = re.search(r"(20\d{2})[-]?(\d{2})[-]?(\d{2})", text_for_date)
        if date_match:
            y, m, d = date_match.groups()
            structured_cmd["time_range"] = {"start": f"{y}-{m}-{d}", "end": f"{y}-{m}-{d}"}
            
        # 3. 提取维度关键词
        if "丢包" in user_text: structured_cmd["analysis_dimensions"].append("ping_loss_v4")
        if "延时" in user_text: structured_cmd["analysis_dimensions"].append("ping_v4")
        if "重传" in user_text: structured_cmd["analysis_dimensions"].append("retrans")
        if "带宽" in user_text: structured_cmd["analysis_dimensions"].append("test_speed_v4")
        
        # 去重
        structured_cmd["analysis_dimensions"] = list(set(structured_cmd["analysis_dimensions"]))
        return structured_cmd

    def parse_user_text(self, user_text):
        """
        解析用户自然语言
        尝试：1. LLM解析 -> 2. 规则兜底
        """
        # 优先尝试正则（针对极简指令提速）
        if "设备ID为" in user_text and re.search(r"\d{8}", user_text):
            return self._rule_based_parse(user_text), "规则解析成功"

        prompt = f"""
你是硬件运维系统的NLU解析专家，需将用户自然语言转换为结构化JSON，仅输出JSON，不要其他内容。
JSON结构要求：
{{
  "time_range": {{
    "start": "相对时间（如now-7d/d）或具体日期（如2026-02-25）",
    "end": "相对时间（如now/d）或具体日期（如2026-02-26）"
  }},
  "machine_codes": ["具体设备的 machine_code 列表，为空则表示所有设备"],
  "analysis_dimensions": ["分析维度：ping_v4/ping_v6（延时）、ping_loss_v4/ping_loss_v6（丢包）、retrans（重传率）、test_speed_v4/test_speed_v6（带宽）、http_status_v4/http_status_v6（HTTP探测）"],
  "analysis_type": "分析类型：summary（汇总）、trend（趋势）、comparison（设备对比）"
}}

用户输入：{user_text}
        """

        # 尝试 LLM 解析
        try:
            if self.provider == "gemini":
                response = self.gemini_client.models.generate_content(
                    model=GEMINI_MODEL,
                    contents=prompt,
                    config=types.GenerateContentConfig(response_mime_type="application/json", temperature=0.1)
                )
                text = response.text.strip()
            else: # deepseek
                response = self.deepseek_client.chat.completions.create(
                    model=DEEPSEEK_MODEL,
                    messages=[{"role": "user", "content": prompt}],
                    response_format={"type": "json_object"},
                    temperature=0.1
                )
                text = response.choices[0].message.content.strip()

            if "```" in text:
                text = re.sub(r"```json|```", "", text).strip()
            return json.loads(text), f"{self.provider}解析成功"

        except Exception as e:
            # 记录错误并切换到规则解析
            print(f"⚠️ {self.provider}解析异常（可能欠费或限流）: {e}")
            return self._rule_based_parse(user_text), "已启用规则兜底解析"

# ---------------------- 3. 上下文协议层（标准化存储全流程数据） ----------------------
class HWContextProtocol:
    """硬件运维专属上下文协议"""
    def __init__(self):
        self.protocol_version = "1.0"

    def generate_context(self, user_text, structured_cmd, user_id="default"):
        """生成初始上下文（包含用户输入、解析后的指令）"""
        
        # 处理索引日期通配符
        # 如果用户指定了具体日期，如 2026-02-25，则使用 eds_line_heartbeat-20260225
        # 否则默认使用通配符检索 eds_line_heartbeat-*
        start_time = structured_cmd["time_range"]["start"]
        es_index = f"{ES_HW_INDEX_PREFIX}-*"
        
        # 简单的日期格式识别 (YYYY-MM-DD 或 YYYYMMDD)
        import re
        date_match = re.search(r"(20\d{2})[-]?(\d{2})[-]?(\d{2})", start_time)
        
        final_start = start_time
        final_end = structured_cmd["time_range"]["end"]
        
        if date_match:
            formatted_date = "".join(date_match.groups()) # 20260225
            es_index = f"{ES_HW_INDEX_PREFIX}-{formatted_date}"
            # 扩展时间范围到全天
            y, m, d = date_match.groups()
            final_start = f"{y}-{m}-{d}T00:00:00.000Z"
            final_end = f"{y}-{m}-{d}T23:59:59.999Z"

        context = {
            "metadata": {
                "context_id": f"ctx_hw_{uuid.uuid4().hex[:12]}",
                "timestamp": int(time.time() * 1000),
                "version": self.protocol_version,
                "user_id": user_id,
                "user_text": user_text,  # 原始用户输入
                "status": "initial"  # 上下文状态：initial/retrieved/analyzed/suggested
            },
            "retrieval": {  # ES检索参数
                "es_index": es_index,
                "query": {
                    "bool": {
                        "filter": [
                            {"range": {"@timestamp": {
                                "gte": final_start,
                                "lte": final_end
                            }}}
                        ]
                    }
                },
                "fields": ["machine_code", "@timestamp", "ping_v4", "ping_v6", "ping_loss_v4", "ping_loss_v6", "retrans", "test_speed_v4", "test_speed_v6", "http_status_v4", "http_status_v6"],
                "size": 10000
            },
            "analysis": {  # 分析配置
                "dimensions": structured_cmd["analysis_dimensions"],
                "type": structured_cmd["analysis_type"],
                "result": {}  # 分析结果填充位
            },
            "optimization": {  # 调优建议填充位
                "warnings": [],
                "suggestions": []
            }
        }
        # 如果指定了设备ID
        if structured_cmd.get("machine_codes"):
            context["retrieval"]["query"]["bool"]["filter"].append(
                {"terms": {"machine_code": structured_cmd["machine_codes"]}}
            )
        # 校验上下文
        is_valid, msg = self._validate_context(context)
        if not is_valid:
            raise ValueError(f"上下文校验失败：{msg}")
        return context

    def _validate_context(self, context):
        """校验上下文格式"""
        required_layers = ["metadata", "retrieval", "analysis", "optimization"]
        for layer in required_layers:
            if layer not in context:
                return False, f"缺失核心层级：{layer}"
        if not context["retrieval"]["es_index"]:
            return False, "缺失ES索引名"
        return True, "校验通过"

    def update_context(self, context, layer, data):
        """更新上下文指定层级的数据"""
        if layer not in context:
            return False, f"层级{layer}不存在"
        context[layer].update(data)
        # 更新状态
        if layer == "retrieval":
            context["metadata"]["status"] = "retrieved"
        elif layer == "analysis":
            context["metadata"]["status"] = "analyzed"
        elif layer == "optimization":
            context["metadata"]["status"] = "suggested"
        return True, "更新成功"

# ---------------------- 4. ES数据检索+分析模块 ----------------------
class HWESAnalyzer:
    def __init__(self, es_url=ES_URL, es_username=ES_USERNAME, es_password=ES_PASSWORD):
        try:
            self.es = Elasticsearch(
                es_url,
                basic_auth=(es_username, es_password),
                verify_certs=False,
                request_timeout=30,
            )
            if not self.es.ping():
                raise ConnectionError("ES ping failed")
        except AuthenticationException as e:
            raise ConnectionError(f"ES authentication failed: {str(e)}")
        except Exception as e:
            raise ConnectionError(f"ES client init failed: {str(e)}")

    def close(self):
        if hasattr(self, "es") and self.es is not None:
            try:
                self.es.transport.close()
            except Exception:
                pass

    def retrieve_data(self, context):
        """从ES检索硬件参数数据"""
        try:
            retrieval_cfg = context["retrieval"]
            response = self.es.search(
                index=retrieval_cfg["es_index"],
                query=retrieval_cfg["query"],
                _source=retrieval_cfg["fields"],
                size=retrieval_cfg["size"]
            )
            # 提取数据并转换为DataFrame（方便分析）
            hits = response["hits"]["hits"]
            data_list = [hit["_source"] for hit in hits]
            df = pd.DataFrame(data_list) if data_list else pd.DataFrame()
            # 数据预处理：转换时间字段、数值字段
            if not df.empty:
                # 转换 @timestamp
                if "@timestamp" in df.columns:
                    df["@timestamp"] = pd.to_datetime(df["@timestamp"])
                
                numeric_fields = [
                    "ping_v4", "ping_v6", "ping_loss_v4", "ping_loss_v6", 
                    "retrans", "test_speed_v4", "test_speed_v6"
                ]
                for field in numeric_fields:
                    if field in df.columns:
                        df[field] = pd.to_numeric(df[field], errors='coerce')
            # 构造检索结果
            retrieval_result = {
                "total_count": response["hits"]["total"]["value"],
                "data_frame": df.to_dict(orient="records"),  # 转为字典存储
                "raw_df": df  # 保留DataFrame供分析使用
            }
            return retrieval_result, "检索成功"
        except ApiError as e:
            return {}, f"检索失败：{str(e)}"

    def analyze_data(self, context, retrieval_result):
        """基于检索结果做硬件运维分析"""
        df = retrieval_result["raw_df"]
        analysis_cfg = context["analysis"]
        analysis_result = {}

        if df.empty:
            analysis_result["error"] = "无符合条件的硬件参数数据"
            return analysis_result, "分析失败"

        # 1. 汇总分析（summary）：计算各维度的均值、最大值、最小值
        if analysis_cfg["type"] == "summary":
            summary = {}
            for dim in analysis_cfg["dimensions"]:
                if dim in df.columns:
                    summary[dim] = {
                        "avg": round(df[dim].mean(), 2),
                        "max": round(df[dim].max(), 2),
                        "min": round(df[dim].min(), 2),
                        "device_count": df["machine_code"].nunique()
                    }
            analysis_result["summary"] = summary

        # 2. 趋势分析（trend）：按天聚合各维度均值
        elif analysis_cfg["type"] == "trend":
            trend = {}
            if "@timestamp" in df.columns:
                df["date"] = df["@timestamp"].dt.date
                for dim in analysis_cfg["dimensions"]:
                    if dim in df.columns:
                        trend[dim] = df.groupby("date")[dim].mean().round(2).to_dict()
            analysis_result["trend"] = trend

        # 3. 设备对比（comparison）：各设备各维度均值对比
        elif analysis_cfg["type"] == "comparison":
            comparison = {}
            for dim in analysis_cfg["dimensions"]:
                if dim in df.columns:
                    comparison[dim] = df.groupby("machine_code")[dim].mean().round(2).to_dict()
            analysis_result["comparison"] = comparison

        return analysis_result, "分析成功"

# ---------------------- 5. 调优建议生成模块 ----------------------
class HWOptimizer:
    def generate_suggestions(self, analysis_result):
        """基于分析结果和调优规则库，生成调优建议"""
        warnings = []
        suggestions = []

        # 处理汇总分析的建议生成
        if "summary" in analysis_result:
            for dim, stats in analysis_result["summary"].items():
                if dim in OPTIMIZE_RULES:
                    rule = OPTIMIZE_RULES[dim]
                    if stats["avg"] > rule["threshold"]:
                        # 添加告警
                        warnings.append(f"{rule['warning']}（当前均值：{stats['avg']}%）")
                        # 添加调优建议
                        suggestions.extend([f"- {s}" for s in rule["suggestion"]])

        # 去重
        warnings = list(set(warnings))
        suggestions = list(set(suggestions))

        return {
            "warnings": warnings,
            "suggestions": suggestions
        }

# ---------------------- 6. 结果输出模块 ----------------------
class HWResultFormatter:
    @staticmethod
    def format_result(context):
        """格式化输出结果（自然语言+结构化数据）"""
        metadata = context["metadata"]
        analysis_result = context["analysis"]["result"]
        optimization = context["optimization"]

        # 基础信息
        output = f"""
### 硬件运维分析报告
请求ID：{metadata['context_id']}
查询时间：{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(metadata['timestamp']/1000))}
用户查询：{metadata['user_text']}

### 分析结果
"""
        # 输出汇总分析
        if "summary" in analysis_result:
            output += "#### 核心指标汇总\n"
            for dim, stats in analysis_result["summary"].items():
                dim_cn = {
                    "ping_v4": "IPv4 延时(ms)",
                    "ping_v6": "IPv6 延时(ms)",
                    "ping_loss_v4": "IPv4 丢包率",
                    "ping_loss_v6": "IPv6 丢包率",
                    "retrans": "重传率",
                    "test_speed_v4": "IPv4 带宽(Mbps)",
                    "test_speed_v6": "IPv6 带宽(Mbps)",
                    "http_status_v4": "HTTP IPv4 状态",
                    "http_status_v6": "HTTP IPv6 状态"
                }.get(dim, dim)
                output += f"- {dim_cn}：均值 {stats['avg']}，最大值 {stats['max']}，最小值 {stats['min']}\n"

        # 输出趋势分析
        if "trend" in analysis_result:
            output += "#### 指标趋势（按天）\n"
            for dim, trend in analysis_result["trend"].items():
                dim_cn = {
                    "ping_v4": "IPv4 延时(ms)",
                    "ping_v6": "IPv6 延时(ms)",
                    "ping_loss_v4": "IPv4 丢包率",
                    "ping_loss_v6": "IPv6 丢包率",
                    "retrans": "重传率",
                    "test_speed_v4": "IPv4 带宽(Mbps)",
                    "test_speed_v6": "IPv6 带宽(Mbps)"
                }.get(dim, dim)
                output += f"- {dim_cn}：{trend}\n"

        # 输出对比分析
        if "comparison" in analysis_result:
            output += "#### 设备对比分析\n"
            for dim, comp in analysis_result["comparison"].items():
                dim_cn = {
                    "ping_v4": "IPv4 延时(ms)",
                    "ping_v6": "IPv6 延时(ms)",
                    "ping_loss_v4": "IPv4 丢包率",
                    "ping_loss_v6": "IPv6 丢包率",
                    "retrans": "重传率",
                    "test_speed_v4": "IPv4 带宽(Mbps)",
                    "test_speed_v6": "IPv6 带宽(Mbps)"
                }.get(dim, dim)
                output += f"- {dim_cn}：\n"
                for mc, val in comp.items():
                    output += f"  - 设备 {mc}: {val}\n"

        # 输出调优建议
        output += "\n### 运维告警与调优建议\n"
        if optimization["warnings"]:
            output += "#### 告警信息\n"
            for warn in optimization["warnings"]:
                output += f"- {warn}\n"
        else:
            output += "#### 告警信息：无异常指标\n"

        if optimization["suggestions"]:
            output += "#### 调优建议\n"
            for sugg in optimization["suggestions"]:
                output += f"{sugg}\n"
        else:
            output += "#### 调优建议：当前指标均正常，无需调优\n"

        return output

# ---------------------- 7. 主流程整合 ----------------------
def main():
    # 1. 初始化各模块
    # 使用 DeepSeek 作为 NLU 解析器
    nlu_parser = NLUParser()
    context_protocol = HWContextProtocol()
    es_analyzer = HWESAnalyzer()
    optimizer = HWOptimizer()
    formatter = HWResultFormatter()

    print("=== 硬件运维 NLU 解析系统 ===")
    print("输入 'exit' 或 'quit' 退出系统\n")

    try:
        while True:
            # 2. 获取用户输入
            user_text = input("请输入您的运维查询指令：").strip()
            
            if not user_text:
                continue
            if user_text.lower() in ['exit', 'quit']:
                print("系统退出。")
                break

            print(f"\n正在解析指令: {user_text}...\n")

            # 3. NLU解析
            structured_cmd, nlu_msg = nlu_parser.parse_user_text(user_text)
            if not structured_cmd:
                print(f"NLU解析失败：{nlu_msg}")
                continue
            print(f"NLU解析结果：{json.dumps(structured_cmd, indent=2, ensure_ascii=False)}\n")

            # 4. 生成初始上下文
            context = context_protocol.generate_context(user_text, structured_cmd)
            print(f"初始上下文：{json.dumps(context, indent=2, ensure_ascii=False)}\n")

            # 5. ES数据检索
            retrieval_result, retrieval_msg = es_analyzer.retrieve_data(context)
            if not retrieval_result:
                print(f"ES检索失败：{retrieval_msg}")
                continue
            context_protocol.update_context(context, "retrieval", {
                "result": {
                    "total_count": retrieval_result["total_count"],
                    "data_frame": retrieval_result["data_frame"],
                }
            })

            # 6. 数据分析
            analysis_result, analysis_msg = es_analyzer.analyze_data(context, retrieval_result)
            if "error" in analysis_result:
                print(f"分析失败：{analysis_result['error']}")
                continue
            context_protocol.update_context(context, "analysis", {"result": analysis_result})

            # 7. 生成调优建议
            optimization = optimizer.generate_suggestions(analysis_result)
            context_protocol.update_context(context, "optimization", optimization)

            # 8. 格式化输出结果
            final_output = formatter.format_result(context)
            print("=" * 80)
            print(final_output)
            print("=" * 80 + "\n")

            # 可选：生成可视化图表
            if "trend" in analysis_result:
                try:
                    df_trend = pd.DataFrame(analysis_result["trend"])
                    df_trend.plot(kind="line", figsize=(10, 6))
                    plt.title("硬件参数趋势图")
                    plt.xlabel("日期")
                    plt.ylabel("指标值")
                    plt.grid(True)
                    plt.savefig("hw_metrics_trend.png")
                    print("趋势图表已更新并保存为：hw_metrics_trend.png\n")
                except Exception as e:
                    print(f"生成图表失败: {e}\n")
    finally:
        es_analyzer.close()

if __name__ == "__main__":
    # 执行主流程
    main()

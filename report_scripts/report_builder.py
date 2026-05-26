#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
report_builder.py — 动态报告构建器 v1.0
功能：
  1. 根据报告类型自动编排章节结构
  2. 自动生成 Executive Summary（基于全文提炼 3-5 条核心洞察）
  3. 数据可信度评估（新鲜度 + 权威性双维度）
  4. 竞品/竞争格局数据结构化整理
  5. 三情景预测（乐观/基准/悲观）

使用方式：
    from report_builder import ReportBuilder

    builder = ReportBuilder(topic="AI支付", report_type="market")
    structure = builder.get_chapter_structure()
    summary   = builder.generate_executive_summary(sections_data)
    score     = builder.score_source(source_dict)
"""

from datetime import datetime
from typing import Optional


# ─── 报告类型定义 ─────────────────────────────────────────────────────────────

REPORT_TEMPLATES = {
    "market": {
        "name": "市场研报",
        "description": "聚焦市场规模、增长预测、竞争格局",
        "chapters": [
            {"num": 1, "title": "执行摘要",    "subtitle": "核心洞察与战略建议"},
            {"num": 2, "title": "市场规模",    "subtitle": "当前体量与增长预测"},
            {"num": 3, "title": "驱动因素",    "subtitle": "需求端与供给端核心驱动"},
            {"num": 4, "title": "竞争格局",    "subtitle": "主要参与方对比分析"},
            {"num": 5, "title": "区域分析",    "subtitle": "主要市场地区差异"},
            {"num": 6, "title": "投资机会",    "subtitle": "高价值赛道与时间窗口"},
            {"num": 7, "title": "风险与挑战",  "subtitle": "主要风险矩阵"},
            {"num": 8, "title": "战略建议",    "subtitle": "分阶段行动路线图"},
        ]
    },
    "technology": {
        "name": "技术研报",
        "description": "聚焦技术架构、实现路径、成熟度",
        "chapters": [
            {"num": 1, "title": "执行摘要",    "subtitle": "技术现状与战略价值"},
            {"num": 2, "title": "技术概述",    "subtitle": "核心原理与架构解析"},
            {"num": 3, "title": "技术成熟度",  "subtitle": "TRL 评估与演进路径"},
            {"num": 4, "title": "应用场景",    "subtitle": "典型用例与实施案例"},
            {"num": 5, "title": "核心玩家",    "subtitle": "主要厂商技术能力对比"},
            {"num": 6, "title": "标准与生态",  "subtitle": "行业标准、开源协议、生态布局"},
            {"num": 7, "title": "实施挑战",    "subtitle": "技术瓶颈与工程复杂度"},
            {"num": 8, "title": "演进展望",    "subtitle": "未来 3-5 年技术路线图"},
        ]
    },
    "regulatory": {
        "name": "监管研报",
        "description": "聚焦政策法规、合规要求、监管趋势",
        "chapters": [
            {"num": 1, "title": "执行摘要",    "subtitle": "监管现状与合规建议"},
            {"num": 2, "title": "监管背景",    "subtitle": "立法动因与政策目标"},
            {"num": 3, "title": "全球监管图谱", "subtitle": "主要司法管辖区横向对比"},
            {"num": 4, "title": "核心合规要求", "subtitle": "许可证、资本要求、报告义务"},
            {"num": 5, "title": "执法案例",    "subtitle": "典型处罚案例与经验教训"},
            {"num": 6, "title": "监管趋势",    "subtitle": "未来 2-3 年政策走向"},
            {"num": 7, "title": "合规策略",    "subtitle": "企业合规路径与最佳实践"},
        ]
    },
    "competitive": {
        "name": "竞争研报",
        "description": "聚焦竞争格局、差异化分析、战略定位",
        "chapters": [
            {"num": 1, "title": "执行摘要",    "subtitle": "竞争态势与差异化机会"},
            {"num": 2, "title": "行业格局",    "subtitle": "市场集中度与竞争强度"},
            {"num": 3, "title": "竞争者画像",  "subtitle": "主要竞争方详细分析"},
            {"num": 4, "title": "产品对比",    "subtitle": "功能/定价/用户体验矩阵"},
            {"num": 5, "title": "战略定位图",  "subtitle": "差异化坐标与定位空间"},
            {"num": 6, "title": "竞争动态",    "subtitle": "近期重要动作与战略意图"},
            {"num": 7, "title": "战略建议",    "subtitle": "差异化路径与竞争优先级"},
        ]
    },
    "full": {
        "name": "综合深度研报",
        "description": "市场 + 技术 + 监管 + 竞争全维度",
        "chapters": [
            {"num": 1, "title": "执行摘要",        "subtitle": "核心洞察与战略建议"},
            {"num": 2, "title": "市场规模与机遇",   "subtitle": "规模预测与增长驱动"},
            {"num": 3, "title": "技术架构",         "subtitle": "技术栈与实现路径"},
            {"num": 4, "title": "竞争格局",         "subtitle": "主要参与方分析"},
            {"num": 5, "title": "监管与合规",       "subtitle": "政策环境与合规路径"},
            {"num": 6, "title": "投资机会与风险",   "subtitle": "机会矩阵与风险评估"},
            {"num": 7, "title": "战略建议",         "subtitle": "分阶段行动路线图"},
        ]
    }
}


# ─── ReportBuilder ────────────────────────────────────────────────────────────

class ReportBuilder:
    """动态报告构建器：根据主题和类型自动编排结构、生成摘要。"""

    def __init__(self, topic: str, report_type: str = "full"):
        self.topic = topic
        self.report_type = report_type if report_type in REPORT_TEMPLATES else "full"
        self.template = REPORT_TEMPLATES[self.report_type]

    def get_chapter_structure(self) -> list:
        """获取章节结构列表。"""
        return self.template["chapters"]

    def get_template_description(self) -> str:
        """获取模板说明。"""
        return f"[{self.template['name']}] {self.template['description']}"

    # ── Executive Summary 生成提示 ────────────────────────────────────────────
    def get_executive_summary_prompt(self, sections_data: dict) -> str:
        """
        生成 Executive Summary 提示词，供 LLM 调用。
        sections_data: {"章节名": "该章节的核心内容摘要", ...}
        返回提示词字符串，让 AI 生成精炼的执行摘要。
        """
        content_lines = []
        for sec, content in sections_data.items():
            content_lines.append(f"## {sec}\n{content}\n")

        prompt = f"""你是一位顶级咨询顾问，请基于以下报告各章节内容，
为《{self.topic}》研究报告撰写执行摘要（Executive Summary）。

要求：
1. 提炼 3-5 条核心洞察（每条一句话，数据支撑）
2. 给出 2-3 条战略建议（可执行的行动方向）
3. 标注最关键的 1-2 个风险
4. 语言简练有力，使用"高管友好"的表述（避免技术术语堆砌）
5. 总字数控制在 400-600 字
6. 输出格式：Markdown，分"核心洞察"、"战略建议"、"主要风险"三节

【报告内容概要】
{"".join(content_lines)}

请直接输出执行摘要内容，不要加多余的说明。"""
        return prompt

    # ── 数据质量评估 ──────────────────────────────────────────────────────────
    def score_source(self, source: dict) -> dict:
        """
        对单条数据来源进行质量评分（0-100）。
        维度：新鲜度（freshness）+ 权威性（authority）
        """
        score = 0
        details = []

        # ── 权威性评分（0-60分）
        authority_score = 0
        src_name = source.get("source_name", "").lower()
        src_url  = source.get("source_url", "").lower()
        confidence = source.get("confidence", "medium")

        if confidence == "high":
            authority_score = 55
        elif confidence == "medium":
            authority_score = 35
        else:
            authority_score = 10

        # 额外加权：已知权威机构
        authority_sources = [
            "gartner", "mckinsey", "accenture", "forrester", "idc",
            "bloomberg", "reuters", "ft.com", "wsj", "hkma", "mas",
            "pboc", "fed", "ecb", "bis", "world bank", "imf",
            "emvco", "iso", "pci", "ieee", "w3c", "paypal", "visa",
            "mastercard", "stripe", "openai", "google", "microsoft"
        ]
        for auth in authority_sources:
            if auth in src_name or auth in src_url:
                authority_score = min(authority_score + 5, 60)
                details.append(f"权威机构加权 (+5): {auth}")
                break

        score += authority_score

        # ── 新鲜度评分（0-40分）
        freshness_score = 0
        src_date = source.get("source_date", "")

        if src_date:
            try:
                if len(src_date) == 7:  # YYYY-MM
                    date_obj = datetime.strptime(src_date, "%Y-%m")
                elif len(src_date) == 4:  # YYYY
                    date_obj = datetime(int(src_date), 1, 1)
                elif len(src_date) >= 10:  # YYYY-MM-DD
                    date_obj = datetime.strptime(src_date[:10], "%Y-%m-%d")
                else:
                    date_obj = None

                if date_obj:
                    months_old = (datetime.now() - date_obj).days / 30
                    if months_old <= 3:
                        freshness_score = 40
                        details.append("极新数据（3个月内）+40")
                    elif months_old <= 6:
                        freshness_score = 35
                        details.append("新鲜数据（6个月内）+35")
                    elif months_old <= 12:
                        freshness_score = 28
                        details.append("近期数据（1年内）+28")
                    elif months_old <= 24:
                        freshness_score = 18
                        details.append("尚可数据（2年内）+18")
                    elif months_old <= 36:
                        freshness_score = 8
                        details.append("偏旧数据（3年内）+8")
                    else:
                        freshness_score = 2
                        details.append("过期数据（超3年）+2")
            except Exception:
                freshness_score = 15  # 无法解析日期，给默认分
                details.append("日期格式无法解析，默认 +15")
        else:
            freshness_score = 5
            details.append("无日期信息 +5")

        score += freshness_score

        # ── 综合评级
        if score >= 80:
            grade = "A"
            recommendation = "优先使用"
        elif score >= 60:
            grade = "B"
            recommendation = "可以使用，建议注明估算属性"
        elif score >= 40:
            grade = "C"
            recommendation = "谨慎使用，最好找替代来源"
        else:
            grade = "D"
            recommendation = "建议删除或替换"

        return {
            "score":          score,
            "authority_score": authority_score,
            "freshness_score": freshness_score,
            "grade":           grade,
            "recommendation":  recommendation,
            "details":         details
        }

    # ── 三情景预测生成 ─────────────────────────────────────────────────────────
    @staticmethod
    def generate_scenario_table(base_value: float, metric: str,
                                year: int, unit: str = "",
                                bull_factor: float = 1.3,
                                bear_factor: float = 0.7) -> dict:
        """
        生成三情景（乐观/基准/悲观）预测数据。
        base_value: 基准预测值
        bull_factor: 乐观倍数（默认 1.3，即 +30%）
        bear_factor: 悲观倍数（默认 0.7，即 -30%）
        返回可直接插入表格的字典。
        """
        bull  = round(base_value * bull_factor, 2)
        bear  = round(base_value * bear_factor, 2)

        return {
            "metric":    metric,
            "year":      year,
            "unit":      unit,
            "scenarios": {
                "乐观情景": {
                    "value":      bull,
                    "assumption": "政策高度支持，技术快速落地，市场接受度超预期",
                    "probability": "20%"
                },
                "基准情景": {
                    "value":      base_value,
                    "assumption": "政策稳步推进，技术按预期成熟，市场正常增长",
                    "probability": "60%"
                },
                "悲观情景": {
                    "value":      bear,
                    "assumption": "监管收紧、技术延迟或市场接受度不及预期",
                    "probability": "20%"
                }
            }
        }

    # ── 竞争格局数据整理 ──────────────────────────────────────────────────────
    @staticmethod
    def structure_competitive_analysis(competitors: list) -> dict:
        """
        整理竞争格局数据，输出可用于表格/雷达图的结构化数据。
        competitors: [
            {
                "name": "公司A",
                "dimensions": {"技术": 8, "规模": 9, "合规": 7, "用户体验": 8},
                "strengths": ["...", "..."],
                "weaknesses": ["..."],
                "market_share": 25.0  # 百分比
            },
            ...
        ]
        """
        if not competitors:
            return {}

        # 提取所有维度
        all_dims = set()
        for comp in competitors:
            all_dims.update(comp.get("dimensions", {}).keys())
        all_dims = sorted(all_dims)

        # 雷达图数据
        radar_data = {}
        for comp in competitors:
            dims = comp.get("dimensions", {})
            radar_data[comp["name"]] = [dims.get(d, 0) for d in all_dims]

        # 对比表格数据
        table_rows = []
        for comp in competitors:
            row = {
                "name":         comp.get("name", ""),
                "market_share": comp.get("market_share", "N/A"),
                "strengths":    "、".join(comp.get("strengths", [])[:2]),
                "weaknesses":   "、".join(comp.get("weaknesses", [])[:1]),
            }
            for dim in all_dims:
                row[dim] = comp.get("dimensions", {}).get(dim, "-")
            table_rows.append(row)

        return {
            "dimensions":  list(all_dims),
            "radar_data":  radar_data,
            "table_rows":  table_rows,
            "top_player":  max(competitors, key=lambda c: c.get("market_share", 0))["name"]
                           if competitors else None
        }


# ─── CLI：输出报告结构 ────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    import json

    topic = sys.argv[1] if len(sys.argv) > 1 else "示例主题"
    rtype = sys.argv[2] if len(sys.argv) > 2 else "full"

    builder = ReportBuilder(topic=topic, report_type=rtype)

    print(f"\n报告类型：{builder.get_template_description()}")
    print(f"\n章节结构：")
    for ch in builder.get_chapter_structure():
        print(f"  {ch['num']:02d}. {ch['title']} — {ch['subtitle']}")

    # 演示三情景预测
    print(f"\n三情景预测示例（市场规模）：")
    scenario = ReportBuilder.generate_scenario_table(
        base_value=120.5, metric="市场规模", year=2027, unit="亿美元"
    )
    print(json.dumps(scenario, ensure_ascii=False, indent=2))

    # 演示数据质量评分
    print(f"\n数据质量评分演示：")
    sample_source = {
        "source_name": "Gartner Research",
        "source_url":  "https://www.gartner.com/research/xxx",
        "source_date": "2025-11",
        "confidence":  "high"
    }
    score_result = builder.score_source(sample_source)
    print(f"  综合评分：{score_result['score']}/100 (评级 {score_result['grade']})")
    print(f"  建议：{score_result['recommendation']}")

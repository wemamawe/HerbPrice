"""PDF 研判报告生成模块

利用 research-report-cn skill 的脚本生成麦肯锡风格 PDF。
简化版：不依赖完整的 generate_pdf.py（太重），用 reportlab 直接生成。

用法:
    from report_gen import generate_herb_report_pdf
    pdf_path = generate_herb_report_pdf("白术", report_data)
"""

import os
import io
import tempfile
from datetime import date

# PDF 生成依赖
try:
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import cm, mm
    from reportlab.lib.colors import HexColor
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.enums import TA_LEFT, TA_CENTER
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
        PageBreak, HRFlowable
    )
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    HAS_REPORTLAB = True
except ImportError:
    HAS_REPORTLAB = False

# 字体路径
FONT_DIR = os.path.join(os.path.dirname(__file__), "report_assets", "fonts")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "data", "reports")

# 品牌色
DARK_BLUE = HexColor("#003366") if HAS_REPORTLAB else None
ACCENT_BLUE = HexColor("#0066CC") if HAS_REPORTLAB else None
BODY_TEXT = HexColor("#333333") if HAS_REPORTLAB else None
LIGHT_BG = HexColor("#F0F4F8") if HAS_REPORTLAB else None
GREEN = HexColor("#22c55e") if HAS_REPORTLAB else None
RED = HexColor("#ef4444") if HAS_REPORTLAB else None
GRAY = HexColor("#888888") if HAS_REPORTLAB else None


def _register_fonts():
    """注册中文字体"""
    regular = os.path.join(FONT_DIR, "NotoSansSC-Regular.ttf")
    bold = os.path.join(FONT_DIR, "NotoSansSC-Bold.ttf")
    if os.path.exists(regular):
        pdfmetrics.registerFont(TTFont("NotoSC", regular))
        pdfmetrics.registerFont(TTFont("NotoSCBold", bold))
        pdfmetrics.registerFontFamily("NotoSC", normal="NotoSC", bold="NotoSCBold")
        return "NotoSC", "NotoSCBold"
    return "Helvetica", "Helvetica-Bold"


def generate_herb_report_pdf(herb_name: str, report: dict) -> str:
    """生成药材研判 PDF 报告

    Args:
        herb_name: 品种名
        report: LLM 生成的研判报告 dict

    Returns:
        PDF 文件路径
    """
    if not HAS_REPORTLAB:
        raise ImportError("需要安装 reportlab: pip install reportlab")

    CN, CNB = _register_fonts()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    pdf_path = os.path.join(OUTPUT_DIR, f"{herb_name}_研判报告_{date.today().isoformat()}.pdf")

    doc = SimpleDocTemplate(
        pdf_path, pagesize=A4,
        leftMargin=50, rightMargin=50, topMargin=55, bottomMargin=50
    )

    # 样式定义
    styles = {
        "cover_title": ParagraphStyle(
            "cover_title", fontName=CNB, fontSize=22, leading=30,
            textColor=DARK_BLUE, alignment=TA_CENTER, spaceAfter=10
        ),
        "cover_sub": ParagraphStyle(
            "cover_sub", fontName=CN, fontSize=12, leading=18,
            textColor=GRAY, alignment=TA_CENTER, spaceAfter=30
        ),
        "section_title": ParagraphStyle(
            "section_title", fontName=CNB, fontSize=14, leading=22,
            textColor=DARK_BLUE, spaceBefore=20, spaceAfter=8
        ),
        "body": ParagraphStyle(
            "body", fontName=CN, fontSize=10, leading=18,
            textColor=BODY_TEXT, spaceBefore=4, spaceAfter=4,
            wordWrap="CJK"
        ),
        "highlight": ParagraphStyle(
            "highlight", fontName=CNB, fontSize=11, leading=18,
            textColor=DARK_BLUE, spaceBefore=6, spaceAfter=4,
            wordWrap="CJK"
        ),
        "caption": ParagraphStyle(
            "caption", fontName=CN, fontSize=8.5, leading=13,
            textColor=GRAY, alignment=TA_CENTER
        ),
    }

    story = []

    # ═══ 封面 ═══
    story.append(Spacer(1, 80))
    story.append(Paragraph("市场研判报告", styles["cover_title"]))
    story.append(Spacer(1, 10))
    story.append(Paragraph(f"<b>{herb_name}</b> · 价格走势与供需分析", styles["cover_title"]))
    story.append(Spacer(1, 30))
    story.append(Paragraph(
        f"分析日期：{report.get('analysis_date', date.today().isoformat())}<br/>"
        f"置信度：{((report.get('confidence', 0.5)) * 100):.0f}%",
        styles["cover_sub"]
    ))
    story.append(Spacer(1, 20))
    story.append(HRFlowable(width="80%", thickness=2, color=ACCENT_BLUE))
    story.append(Spacer(1, 10))
    story.append(Paragraph("HerbPrice Research · 药材价格智能分析系统", styles["caption"]))
    story.append(Paragraph("本报告由 AI 模型自动生成，仅供参考，不构成投资建议。", styles["caption"]))

    story.append(PageBreak())

    # ═══ 执行摘要 ═══
    story.append(Paragraph("01  执行摘要", styles["section_title"]))
    story.append(HRFlowable(width="100%", thickness=0.5, color=LIGHT_BG))
    summary = report.get("summary", "暂无摘要")
    story.append(Paragraph(summary, styles["highlight"]))
    story.append(Spacer(1, 10))

    # 方向判断
    direction = report.get("direction", "neutral")
    dir_text = {"up": "看涨 ↑", "down": "看跌 ↓", "neutral": "震荡 →"}.get(direction, "未知")
    dir_color = {"up": GREEN, "down": RED, "neutral": GRAY}.get(direction, GRAY)

    dir_hex = dir_color.hexval() if dir_color else "#888888"
    story.append(Paragraph(
        f'<font color="{dir_hex}"><b>市场方向：{dir_text}</b></font>'
        f'&nbsp;&nbsp;|&nbsp;&nbsp;置信度 {((report.get("confidence", 0.5)) * 100):.0f}%',
        styles["body"]
    ))
    story.append(Spacer(1, 10))

    # 价格区间
    pr = report.get("price_range", {})
    if pr:
        story.append(Paragraph("价格区间预测：", styles["body"]))
        range_data = []
        if pr.get("30d_low"):
            range_data.append(["30天", f"¥{pr['30d_low']}", f"¥{pr['30d_high']}"])
        if pr.get("90d_low"):
            range_data.append(["90天", f"¥{pr['90d_low']}", f"¥{pr['90d_high']}"])

        if range_data:
            t = Table(
                [["周期", "下限", "上限"]] + range_data,
                colWidths=[3 * cm, 4 * cm, 4 * cm]
            )
            t.setStyle(TableStyle([
                ("FONTNAME", (0, 0), (-1, 0), CNB),
                ("FONTNAME", (0, 1), (-1, -1), CN),
                ("FONTSIZE", (0, 0), (-1, -1), 10),
                ("TEXTCOLOR", (0, 0), (-1, 0), DARK_BLUE),
                ("BACKGROUND", (0, 0), (-1, 0), LIGHT_BG),
                ("GRID", (0, 0), (-1, -1), 0.5, LIGHT_BG),
                ("ALIGN", (1, 0), (-1, -1), "CENTER"),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]))
            story.append(t)

    story.append(Spacer(1, 15))

    # ═══ 关键驱动因素 ═══
    factors = report.get("key_factors", [])
    if factors:
        story.append(Paragraph("02  关键驱动因素", styles["section_title"]))
        story.append(HRFlowable(width="100%", thickness=0.5, color=LIGHT_BG))
        for f in factors:
            impact = f.get("impact", "neutral")
            icon = {"positive": "[+]", "negative": "[-]", "neutral": "[=]"}.get(impact, "[=]")
            color = {"positive": "#22c55e", "negative": "#ef4444", "neutral": "#888"}.get(impact, "#888")
            text = f.get("detail") or f.get("factor", "")
            weight = f.get("weight", "")
            weight_text = f" (权重{weight*100:.0f}%)" if weight else ""
            story.append(Paragraph(
                f'<font color="{color}"><b>{icon}</b></font> {text}{weight_text}',
                styles["body"]
            ))
        story.append(Spacer(1, 10))

    # ═══ 风险与机遇 ═══
    risks = report.get("risks", [])
    opportunities = report.get("opportunities", [])
    if risks or opportunities:
        story.append(Paragraph("03  风险与机遇", styles["section_title"]))
        story.append(HRFlowable(width="100%", thickness=0.5, color=LIGHT_BG))
        for r in risks:
            story.append(Paragraph(f'<font color="#ef4444">▲ 风险：</font>{r}', styles["body"]))
        for o in opportunities:
            story.append(Paragraph(f'<font color="#22c55e">▼ 机会：</font>{o}', styles["body"]))
        story.append(Spacer(1, 10))

    # ═══ 操作建议 ═══
    rec = report.get("recommendation", "")
    if rec:
        story.append(Paragraph("04  操作建议", styles["section_title"]))
        story.append(HRFlowable(width="100%", thickness=0.5, color=LIGHT_BG))
        story.append(Paragraph(f"<b>{rec}</b>", styles["highlight"]))
        story.append(Spacer(1, 10))

    # ═══ 分析逻辑 ═══
    reasoning = report.get("reasoning", "")
    if reasoning:
        story.append(Paragraph("05  分析逻辑", styles["section_title"]))
        story.append(HRFlowable(width="100%", thickness=0.5, color=LIGHT_BG))
        story.append(Paragraph(reasoning, styles["body"]))
        story.append(Spacer(1, 10))

    # ═══ 免责声明 ═══
    story.append(Spacer(1, 30))
    story.append(HRFlowable(width="100%", thickness=0.5, color=GRAY))
    story.append(Paragraph(
        "免责声明：本报告由 AI 模型（Hy3 Preview + XGBoost + Prophet）自动生成，"
        "数据来源包括中药材天地网、Open-Meteo 气象API、海关统计等公开数据。"
        "报告内容仅供参考，不构成任何投资建议。",
        styles["caption"]
    ))

    # 生成 PDF
    doc.build(story)
    return pdf_path


if __name__ == "__main__":
    import sys
    name = sys.argv[1] if len(sys.argv) > 1 else "白术"

    # 测试生成
    from research_agent import generate_report_fallback
    report = generate_report_fallback(name)
    path = generate_herb_report_pdf(name, report)
    print(f"PDF 已生成: {path}")

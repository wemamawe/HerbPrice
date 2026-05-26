#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Agentic Payment 行业深度研究报告（简体中文版）
麦肯锡风格 PDF 生成器
"""

import io
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import cm, mm
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, HRFlowable, Image
)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

from reportlab.pdfbase.ttfonts import TTFont as TTFontObj
pdfmetrics.registerFont(TTFontObj('NotoSC',     '/data/workspace/fonts/NotoSansSC-Regular.ttf'))
pdfmetrics.registerFont(TTFontObj('NotoSCBold', '/data/workspace/fonts/NotoSansSC-Bold.ttf'))
pdfmetrics.registerFontFamily('NotoSC', normal='NotoSC', bold='NotoSCBold')
CN  = 'NotoSC'      # 正文字体：思源黑体 Regular（非衬线，清晰易读）
CNB = 'NotoSCBold'  # 标题字体：思源黑体 Bold（粗体，层级分明）

BLUE   = colors.HexColor('#0A2240')
ACCENT = colors.HexColor('#00B5E2')
TEAL   = colors.HexColor('#007B8A')
ORANGE = colors.HexColor('#E8622A')
DGRAY  = colors.HexColor('#4A4A4A')
MGRAY  = colors.HexColor('#9B9B9B')
LGRAY  = colors.HexColor('#F5F5F5')
WHITE  = colors.white
LINE   = colors.HexColor('#D0D0D0')
PAGE_W, PAGE_H = A4

# ─── matplotlib 中文字体设置 ────────────────────────────────────────────────
# 使用系统安装的 Noto CJK 字体，确保图表中文正常显示
import matplotlib.font_manager as fm
_noto_ttc = '/usr/share/fonts/google-noto-cjk/NotoSansCJK-Regular.ttc'
try:
    fm.fontManager.addfont(_noto_ttc)
    _noto_name = fm.FontProperties(fname=_noto_ttc).get_name()
    plt.rcParams['font.sans-serif'] = [_noto_name, 'DejaVu Sans']
except Exception:
    plt.rcParams['font.sans-serif'] = ['DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False


def S(name):
    # 关键修复：
    # 1. 正文使用 TA_LEFT 而非 TA_JUSTIFY，避免中英混排时英文词间距异常
    # 2. 加大正文字号至 10pt，行距至 18pt（适合中文阅读）
    # 3. 表格内容字号 9pt，行距 14pt
    defs = {
        'ch_num':   ParagraphStyle('ch_num',  fontName=CNB, fontSize=10,   leading=14,  textColor=ACCENT, spaceAfter=3),
        'ch_title': ParagraphStyle('ch_title',fontName=CNB, fontSize=21,   leading=28,  textColor=BLUE,   spaceAfter=10),
        'sec':      ParagraphStyle('sec',     fontName=CNB, fontSize=13,   leading=18,  textColor=BLUE,   spaceBefore=14, spaceAfter=6),
        'sub':      ParagraphStyle('sub',     fontName=CNB, fontSize=10.5, leading=15,  textColor=TEAL,   spaceBefore=10, spaceAfter=4),
        'body':     ParagraphStyle('body',    fontName=CN,  fontSize=10,   leading=18,  textColor=DGRAY,  alignment=TA_LEFT, spaceAfter=5, wordWrap='CJK'),
        'bold':     ParagraphStyle('bold',    fontName=CNB, fontSize=10,   leading=16,  textColor=DGRAY,  spaceAfter=3),
        'bullet':   ParagraphStyle('bullet',  fontName=CN,  fontSize=10,   leading=16,  textColor=DGRAY,  leftIndent=18, firstLineIndent=-12, spaceAfter=3, wordWrap='CJK'),
        'kpi_v':    ParagraphStyle('kpi_v',   fontName=CNB, fontSize=20,   leading=25,  textColor=BLUE,   alignment=TA_CENTER),
        'kpi_u':    ParagraphStyle('kpi_u',   fontName=CN,  fontSize=9,    leading=13,  textColor=ACCENT, alignment=TA_CENTER),
        'kpi_l':    ParagraphStyle('kpi_l',   fontName=CN,  fontSize=8,    leading=11,  textColor=MGRAY,  alignment=TA_CENTER),
        'cap':      ParagraphStyle('cap',     fontName=CN,  fontSize=8.5,  leading=12,  textColor=MGRAY,  spaceAfter=6),
        'toc_ch':   ParagraphStyle('toc_ch',  fontName=CNB, fontSize=11,   leading=16,  textColor=BLUE,   spaceBefore=7, spaceAfter=2),
        'hbox':     ParagraphStyle('hbox',    fontName=CN,  fontSize=10,   leading=16,  textColor=BLUE,   wordWrap='CJK'),
        'th':       ParagraphStyle('th',      fontName=CNB, fontSize=9,    leading=13,  textColor=WHITE,  alignment=TA_CENTER),
        'td':       ParagraphStyle('td',      fontName=CN,  fontSize=9,    leading=13,  textColor=DGRAY,  wordWrap='CJK'),
        'tdb':      ParagraphStyle('tdb',     fontName=CNB, fontSize=9,    leading=13,  textColor=DGRAY),
        'tdc':      ParagraphStyle('tdc',     fontName=CN,  fontSize=9,    leading=13,  textColor=DGRAY,  alignment=TA_CENTER),
    }
    return defs[name]


def img(fig, w=15, h=8):
    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=150, bbox_inches='tight', facecolor='white')
    buf.seek(0)
    plt.close(fig)
    return Image(buf, width=w*cm, height=h*cm)


def kpi_row(items):
    n = len(items)
    cw = (PAGE_W - 3*cm) / n
    cells = [[Paragraph(v, S('kpi_v')), Paragraph(u, S('kpi_u')), Paragraph(l, S('kpi_l'))]
             for v, u, l in items]
    t = Table([cells], colWidths=[cw]*n)
    t.setStyle(TableStyle([
        ('BACKGROUND',    (0,0),(-1,-1), LGRAY),
        ('TOPPADDING',    (0,0),(-1,-1), 10),
        ('BOTTOMPADDING', (0,0),(-1,-1), 10),
        ('LINEABOVE',  (0,0),(-1,0), 3, ACCENT),
        ('BOX',        (0,0),(-1,-1), 0.5, LINE),
        ('INNERGRID',  (0,0),(-1,-1), 0.5, LINE),
    ]))
    return t


def hbox(text, bc=ACCENT, bg=None):
    if bg is None:
        bg = colors.HexColor('#EBF7FC')
    t = Table([[Paragraph(text, S('hbox'))]], colWidths=[PAGE_W - 3*cm])
    t.setStyle(TableStyle([
        ('BACKGROUND',    (0,0),(-1,-1), bg),
        ('LINEABOVE',     (0,0),(-1,0),  3, bc),
        ('LINEBELOW',     (0,-1),(-1,-1),0.5, LINE),
        ('LINEBEFORE',    (0,0),(0,-1),  0.5, LINE),
        ('LINEAFTER',     (-1,0),(-1,-1),0.5, LINE),
        ('TOPPADDING',    (0,0),(-1,-1), 9),
        ('BOTTOMPADDING', (0,0),(-1,-1), 9),
        ('LEFTPADDING',   (0,0),(-1,-1), 10),
        ('RIGHTPADDING',  (0,0),(-1,-1), 10),
    ]))
    return t


def ch_header(num, title, sub=None):
    e = []
    e.append(Paragraph('第 %s 章' % num, S('ch_num')))
    e.append(Paragraph(title, S('ch_title')))
    if sub:
        e.append(Paragraph(sub, S('body')))
    e.append(HRFlowable(width='100%', thickness=1.5, color=ACCENT, spaceAfter=10))
    return e


class Footer:
    TITLE = 'Agentic Payment：数字商业的下一个前沿'
    META  = '战略研究 | 2026年3月'
    def __call__(self, canv, doc):
        canv.saveState()
        canv.setFillColor(BLUE)
        canv.rect(0, PAGE_H - 6*mm, PAGE_W, 6*mm, fill=1, stroke=0)
        canv.setFillColor(ACCENT)
        canv.rect(0, PAGE_H - 6*mm, 3.5*cm, 6*mm, fill=1, stroke=0)
        canv.setStrokeColor(LINE)
        canv.setLineWidth(0.5)
        canv.line(1.5*cm, 1.5*cm, PAGE_W - 1.5*cm, 1.5*cm)
        canv.setFont(CN, 7.5)
        canv.setFillColor(MGRAY)
        canv.drawString(1.5*cm, 0.9*cm, self.TITLE)
        canv.drawString(1.5*cm, 0.4*cm, self.META)
        canv.setFont(CNB, 8)
        canv.setFillColor(BLUE)
        canv.drawRightString(PAGE_W - 1.5*cm, 0.9*cm, str(doc.page))
        canv.setFont(CN, 7)
        canv.setFillColor(MGRAY)
        canv.drawCentredString(PAGE_W/2, 0.4*cm, '保密文件 · 仅供内部使用')
        canv.restoreState()


def draw_cover(canv, doc):
    canv.saveState()
    canv.setFillColor(BLUE)
    canv.rect(0, 0, PAGE_W, PAGE_H, fill=1, stroke=0)
    p = canv.beginPath()
    p.moveTo(PAGE_W - 120, PAGE_H)
    p.lineTo(PAGE_W, PAGE_H)
    p.lineTo(PAGE_W, PAGE_H - 180)
    p.close()
    canv.setFillColor(ACCENT)
    canv.drawPath(p, fill=1, stroke=0)
    canv.setFillColor(ACCENT)
    canv.rect(1.5*cm, PAGE_H*0.52, PAGE_W - 3*cm, 3, fill=1, stroke=0)
    canv.setFillColor(colors.HexColor('#061428'))
    canv.rect(0, 0, PAGE_W, 3*cm, fill=1, stroke=0)
    canv.setFont(CN, 9)
    canv.setFillColor(ACCENT)
    canv.drawString(1.5*cm, PAGE_H - 2.8*cm, '深度研究报告  //  2026年3月')
    title_lines = ['Agentic Payment：', '数字商业的', '下一个前沿']
    y = PAGE_H * 0.72
    for i, line in enumerate(title_lines):
        canv.setFont(CNB, 28 if i == 0 else 26)
        canv.setFillColor(WHITE)
        canv.drawString(1.5*cm, y, line)
        y -= 34
    canv.setFont(CN, 12)
    canv.setFillColor(colors.HexColor('#8BBFD4'))
    canv.drawString(1.5*cm, PAGE_H * 0.72 - 116,
                    'AI 代理如何重塑 2.4 万亿美元的全球支付版图')
    stats = [
        ('3-5万亿', '美元（麦肯锡预测）\n2030年全球规模'),
        ('6项', '竞争中的\n协议标准'),
        ('43%', '消费者已用\n生成式AI购物'),
        ('97%', 'CFO知晓\nAgentic AI能力'),
    ]
    sx = 1.5*cm
    sy = PAGE_H * 0.72 - 172
    bw = (PAGE_W - 3*cm) / 4
    for val, label in stats:
        canv.setFillColor(colors.HexColor('#0D2E52'))
        canv.rect(sx, sy - 1.4*cm, bw - 0.3*cm, 2.0*cm, fill=1, stroke=0)
        canv.setFont(CNB, 16)
        canv.setFillColor(ACCENT)
        canv.drawCentredString(sx + (bw - 0.3*cm)/2, sy - 0.05*cm, val)
        canv.setFont(CN, 7)
        canv.setFillColor(colors.HexColor('#8BBFD4'))
        for li, ll in enumerate(label.split('\n')):
            canv.drawCentredString(sx + (bw - 0.3*cm)/2, sy - 0.65*cm - li*9, ll)
        sx += bw
    canv.setFont(CN, 7.5)
    canv.setFillColor(colors.HexColor('#607080'))
    canv.drawString(1.5*cm, 1.0*cm,
                    '来源：麦肯锡、BCG、摩根士丹利、科尔尼、a16z、Forrester  |  基于截至2026年3月的公开研究')
    canv.restoreState()


def build_toc():
    e = []
    e.append(Paragraph('目录', S('ch_title')))
    e.append(HRFlowable(width='100%', thickness=1, color=ACCENT, spaceAfter=14))
    items = [
        ('01', '执行摘要', '3'),
        ('02', '定义与范式转变：什么是Agentic Payment？', '5'),
        ('03', '市场规模与增长预测', '7'),
        ('04', '生态架构：七层价值链', '9'),
        ('05', '协议之战：ACP、AP2、UCP、MCP、A2A与Visa TAP', '11'),
        ('06', '竞争格局：传统巨头与新兴挑战者', '13'),
        ('07', '信任、安全与欺诈：全新攻击面', '15'),
        ('08', '监管与合规环境', '17'),
        ('09', '战略启示与行动建议', '19'),
        ('10', '研究方法与来源', '22'),
    ]
    for num, title, pg in items:
        row = [[
            Paragraph('<font color="#00B5E2"><b>%s</b></font>' % num, S('toc_ch')),
            Paragraph(title, S('toc_ch')),
            Paragraph(pg, S('toc_ch')),
        ]]
        t = Table(row, colWidths=[1.2*cm, 13.5*cm, 1.5*cm])
        t.setStyle(TableStyle([
            ('ALIGN', (2,0), (2,0), 'RIGHT'),
            ('BOTTOMPADDING', (0,0), (-1,-1), 2),
            ('TOPPADDING', (0,0), (-1,-1), 2),
        ]))
        e.append(t)
    e.append(PageBreak())
    return e


def ch01():
    e = []
    e += ch_header('01', '执行摘要', 'Agentic Payment 革命已不再是未来场景——它正在当下发生。')
    e.append(kpi_row([
        ('3-5万亿', '美元（2030年）', '全球Agentic商务规模\n（麦肯锡预测）'),
        ('2.4万亿', '美元（2029年）', '全球支付收入规模\n（BCG预测）'),
        ('3850亿',  '美元（2030年）', '美国Agentic电商\n（摩根士丹利估算）'),
        ('6项',     '竞争协议标准',   '截至2026年市场上\n的竞争协议数量'),
    ]))
    e.append(Spacer(1, 12))
    e.append(Paragraph('核心论点', S('sec')))
    e.append(Paragraph(
        '支付行业正在经历自信用卡问世以来最深刻的变革。从人类发起交易到'
        '<b>AI代理自主发起交易</b>的转变，不仅仅是用户体验的升级——'
        '它代表着支付价值链、信任模型和竞争格局的全面重构。'
        '麦肯锡于2025年10月指出：2025年很可能是消费者以现有方式购物的最后一年。',
        S('body')))
    e.append(Spacer(1, 6))
    e.append(hbox(
        '<b>核心洞察：</b>万事达卡估计，AI代理到2030年可影响全球超过'
        '<b>3万亿美元的消费</b>。'
        '掌控代理层的商户、银行或支付网络，就掌控了购买决策权。'))
    e.append(Spacer(1, 8))
    e.append(Paragraph('重塑格局的五大力量', S('sec')))
    forces = [
        ('一、AI平台主导权之争',
         'OpenAI、谷歌和Anthropic已成为商业平台。ChatGPT的即时结账、Gemini的代理购物以及'
         'Perplexity的立即购买功能，代表了一种18个月前尚不存在的全新支付渠道类别。'),
        ('二、协议碎片化',
         '六项竞争中的Agentic商业协议——ACP、AP2、UCP、MCP、A2A和Visa TAP——'
         '正在争夺行业标准地位。胜出者将决定哪个主体掌控认证、授权和交易轨道。'),
        ('三、传统巨头的反击',
         '万事达卡（Agent Pay）和Visa（智能商务/TAP）均已推出企业级Agentic基础设施，'
         '部署模拟16位卡号的代理令牌凭证，以维持其网络的核心地位。'),
        ('四、信任赤字',
         '尽管97%的企业CFO知晓Agentic AI，但仅有15%正在部署。这一信任鸿沟——'
         '横跨消费者同意、欺诈责任与AI幻觉三大层面——是当前制约采用速度的首要障碍。'),
        ('五、监管真空',
         '现行支付法规（PSD2/3、EFTA、Regulation E）是为人类发起的交易而制定的。'
         'Agentic AI提出了全新的责任归属问题：当AI代理过度购买、授权欺诈性交易'
         '或误解用户意图时，谁来承担责任？'),
    ]
    for title, body in forces:
        e.append(Paragraph('<b>%s</b>' % title, S('sub')))
        e.append(Paragraph(body, S('body')))
    e.append(Spacer(1, 8))
    e.append(Paragraph('战略要务', S('sec')))
    imperatives = [
        '支付网络：在AI平台构建专有钱包基础设施之前，将可信代理令牌化确立为行业默认标准。',
        '银行与发行机构：部署委托授权框架；定位为Agentic交易中的信任锚，而非被动轨道提供者。',
        '商户：投资代理可读的产品目录和结构化数据API；针对代理发现优化的零售商将获得不成比例的市场份额。',
        '监管机构：为AI发起的支付制定明确的责任框架；强制要求审计追踪和人工覆盖能力。',
        '金融科技与初创公司：七层Agentic商业栈的每一层都存在白空间——聚焦于信任基础设施、代理钱包和策略执行。',
    ]
    for imp in imperatives:
        e.append(Paragraph('· ' + imp, S('bullet')))
    e.append(PageBreak())
    return e


def ch02():
    e = []
    e += ch_header('02', '定义与范式转变：什么是Agentic Payment？',
                   '从点击购买到策略驱动的全天候自主商业。')
    e.append(Paragraph('何为Agentic Payment？', S('sec')))
    e.append(Paragraph(
        'Agentic Payment是指由<b>AI代理代表用户自主发起并执行</b>的金融交易，'
        '无需用户在交易节点进行实时确认。'
        '与传统电商（用户点击"购买"）不同，Agentic系统在预授权策略下运行，'
        '根据用户定义的目标和参数自主完成采购。'
        '科尔尼（2025）将Agentic商务定义为：AI代理代表用户自主发现商品、'
        '比较选项并完成购买的数字购物体验。',
        S('body')))
    e.append(Spacer(1, 8))
    e.append(Paragraph('商业进化的三个时代', S('sec')))
    hdrs = ['维度', '第一时代：实体（2000年前）', '第二时代：数字（2000-2024）', '第三时代：代理（2025+）']
    rows = [
        ['交易发起者', '线下到场的人类', '网络/移动端人类', 'AI代理（委托授权）'],
        ['决策主体', '人类判断', '人类+算法', 'AI策略+LLM推理'],
        ['支付凭证', '现金/刷卡', '令牌化卡/电子钱包', '代理令牌/委托授权'],
        ['结账体验', '实体POS终端', '购物车+确认', '无界面——策略执行'],
        ['欺诈信号', '签名/PIN码', '设备指纹', '代理身份证明'],
        ['商户关系', '直接关系', '平台中介', 'AI中介/API驱动'],
        ['价值链控制', '发行方/收单行', '卡组织', 'AI平台+网络'],
    ]
    cw = [(PAGE_W-3*cm)*f for f in [0.20, 0.22, 0.28, 0.30]]
    td = [[Paragraph(h, S('th')) for h in hdrs]]
    for row in rows:
        td.append([Paragraph(row[0], S('tdb'))] + [Paragraph(c, S('td')) for c in row[1:]])
    t = Table(td, colWidths=cw)
    t.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), BLUE),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [WHITE, colors.HexColor('#F7F9FB')]),
        ('GRID', (0,0), (-1,-1), 0.5, LINE),
        ('TOPPADDING',    (0,0), (-1,-1), 5),
        ('BOTTOMPADDING', (0,0), (-1,-1), 5),
        ('LEFTPADDING',   (0,0), (-1,-1), 5),
        ('RIGHTPADDING',  (0,0), (-1,-1), 5),
        ('LINEABOVE', (0,0), (-1,0), 2.5, ACCENT),
    ]))
    e.append(t)
    e.append(Paragraph('图表2.1：商业从实体到代理时代跨关键维度的演进。', S('cap')))
    e.append(Spacer(1, 8))
    e.append(Paragraph('一笔Agentic交易的解剖', S('sec')))
    steps = [
        ('1. 目标设定', '用户定义偏好和策略：当家庭必需品低于阈值时自动补货；月度预算上限200美元；优先选择可持续品牌。'),
        ('2. 代理激活', 'AI代理（如ChatGPT、Gemini内置或专用商业代理）接收目标，访问已授权的支付令牌和商户API。'),
        ('3. 发现与比较', '代理通过ACP、UCP或MCP支持的商户API查询结构化产品信息，将选项与用户策略进行比对。'),
        ('4. 决策与授权', '代理选出最优方案；核对消费限额和策略约束；生成签名支付意图。'),
        ('5. 交易执行', '代理将令牌化凭证（万事达卡Agent Pay令牌或Visa TAP令牌）提交至商户结账系统；支付网络按标准卡处理。'),
        ('6. 确认与审计', '用户收到通知；交易记录至审计追踪，含完整的代理决策溯源，用于争议解决。'),
    ]
    for st, sb in steps:
        e.append(Paragraph('<b>%s</b>' % st, S('sub')))
        e.append(Paragraph(sb, S('body')))
    e.append(Spacer(1, 8))
    e.append(hbox(
        '<b>全新价值创造：</b>科尔尼识别出以下全新收入来源：代理即服务费、签名报价API、'
        '委托授权产品、委托验证、风险评分和网络担保——同时颠覆搜索/广告漏斗、'
        '传统网络结账、争议处理运营和拒付经济学。',
        ORANGE, colors.HexColor('#FFF3EE')))
    e.append(PageBreak())
    return e


def ch03():
    e = []
    e += ch_header('03', '市场规模与增长预测',
                   '麦肯锡、BCG、摩根士丹利和科尔尼的预测汇聚形成共识：到2030年，Agentic商务将中介数万亿美元的全球消费。')
    fig, ax = plt.subplots(figsize=(13, 5.5))
    fig.patch.set_facecolor('white')
    ax.set_facecolor('#F7F9FB')
    labels = ['麦肯锡\n全球B2C', '麦肯锡\n美国B2C', '摩根士丹利\n美国电商',
              'BCG\n支付收入', '万事达卡\n全球影响']
    lo = [3000, 800, 190, 2400, 2500]
    hi = [5000, 1000, 385, 2400, 3000]
    x = np.arange(len(labels))
    ax.bar(x, hi, 0.5, color='#0A2240', label='高预测', zorder=3)
    ax.bar(x, lo, 0.5, color='#00B5E2', label='低预测', zorder=4)
    ax.set_ylabel('十亿美元（USD Billions）', fontsize=10, color='#4A4A4A')
    ax.set_title('2030年市场规模预测（十亿美元）', fontsize=12, fontweight='bold', color='#0A2240', pad=12)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=9, color='#4A4A4A')
    ax.set_ylim(0, 6200)
    ax.grid(axis='y', color='#D0D0D0', linewidth=0.5, zorder=0)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    for i, h in enumerate(hi):
        ax.text(i, h + 60, '$%dB' % h, ha='center', va='bottom', fontsize=8, color='#0A2240', fontweight='bold')
    ax.axhline(y=1000, color='#E8622A', linestyle='--', linewidth=1.2, zorder=5, label='1万亿美元里程碑')
    ax.legend(loc='upper right', fontsize=8, framealpha=0.9)
    plt.tight_layout()
    e.append(img(fig, 16, 7))
    e.append(Paragraph(
        '图表3.1：各主要研究机构对2030年Agentic商务/支付的市场规模预测。注：BCG数据为全球支付总收入（非仅限Agentic）。所有数据为估算值。',
        S('cap')))
    e.append(Spacer(1, 8))
    e.append(Paragraph('采用轨迹预测', S('sec')))
    fig2, ax2 = plt.subplots(figsize=(13, 5))
    fig2.patch.set_facecolor('white')
    ax2.set_facecolor('#F7F9FB')
    years = [2024, 2025, 2026, 2027, 2028, 2029, 2030]
    us    = [0.5, 2.5, 6.0, 11.0, 18.0, 27.0, 38.0]
    gl    = [50, 200, 500, 1100, 2200, 3500, 5000]
    ax2b = ax2.twinx()
    l1, = ax2.plot(years, us, 'o-', color='#0A2240', linewidth=2.5, markersize=6, label='美国Agentic电商占比（%）', zorder=5)
    l2, = ax2b.plot(years, gl, 's--', color='#00B5E2', linewidth=2, markersize=6, label='全球规模（十亿美元）', zorder=5)
    ax2.fill_between(years, us, alpha=0.1, color='#0A2240')
    ax2b.fill_between(years, gl, alpha=0.08, color='#00B5E2')
    ax2.set_ylabel('美国Agentic电商占比（%）', fontsize=9, color='#0A2240')
    ax2b.set_ylabel('全球规模（十亿美元）', fontsize=9, color='#00B5E2')
    ax2.set_title('Agentic商务采用曲线预测（2024-2030）', fontsize=12, fontweight='bold', color='#0A2240', pad=12)
    ax2.set_xticks(years)
    ax2.grid(axis='y', color='#D0D0D0', linewidth=0.5, zorder=0)
    ax2.spines['top'].set_visible(False)
    ax2.tick_params(labelsize=9)
    ax2b.tick_params(labelsize=9)
    events = {2024: '首批Agentic\n支付上线', 2025: 'OpenAI/Stripe ACP\n万事达卡/Visa推出', 2026: '协议收敛\n监管框架出台'}
    for yr, txt in events.items():
        ax2.axvline(x=yr, color='#E8622A', linewidth=0.8, linestyle=':', alpha=0.7)
        ax2.text(yr, max(us)*0.55, txt, fontsize=7, color='#E8622A', ha='center', va='bottom',
                 bbox=dict(boxstyle='round,pad=0.2', facecolor='white', alpha=0.8))
    ax2.legend([l1, l2], [l1.get_label(), l2.get_label()], loc='upper left', fontsize=8, framealpha=0.9)
    plt.tight_layout()
    e.append(img(fig2, 16, 6.5))
    e.append(Paragraph(
        '图表3.2：基于Forrester、麦肯锡和摩根士丹利预测的示意性采用曲线。数据为估算值，存在较大不确定性。',
        S('cap')))
    e.append(Spacer(1, 8))
    e.append(Paragraph('关键市场数据', S('sec')))
    data = [
        ('麦肯锡（2025年10月）',
         '全球Agentic商务2030年达3-5万亿美元；仅美国B2C零售即可带来1万亿美元协同收入。'
         '描述为"策略驱动、全天候购买"取代点击购买模式。新增收入项：代理即服务费、签名报价API、委托授权产品。'),
        ('BCG（2025年9月）',
         '全球支付收入预计2029年达2.4万亿美元。Agentic AI被认定为主要增长驱动力，'
         '预计2028年成为银行业转型最大加速器。'),
        ('摩根士丹利（2025年）',
         'Agentic购物者到2030年可带来1900-3850亿美元的美国电商消费。'
         '23%的美国人在过去一个月内完成了AI辅助购买。食杂和快消品是最大增长类别。'),
        ('万事达卡（2025年）',
         'AI代理预计2030年影响全球超过3万亿美元消费（万事达卡内部估算）。'
         '信任与身份验证被确定为主要基础设施需求。'),
        ('BCG零售银行（2025年11月）',
         'AI为零售银行带来的利润潜力到2030年达3700亿美元。'
         'Agentic AI预计到2028年占AI驱动商业影响的29%，实现规模化近零边际成本。'),
    ]
    for src, txt in data:
        e.append(Paragraph('<b>%s</b>' % src, S('sub')))
        e.append(Paragraph(txt, S('body')))
    e.append(PageBreak())
    return e


def ch04():
    e = []
    e += ch_header('04', '生态架构：七层价值链',
                   'Agentic商务不是单一产品——它是一个完整的技术栈，每一层都有独特的经济逻辑、现有玩家和创业机会。')
    e.append(Paragraph(
        '研究机构rye.com（2026年）将Agentic商务生态系统绘制为<b>七层价值链</b>，'
        '50余家企业在从基础AI基础设施到面向消费者的信任系统的各层进行布局。', S('body')))
    e.append(Spacer(1, 8))
    layers = [
        ('第7层', '信任与安全',   '#E8622A', '代理身份验证、欺诈检测、消费者争议解决', 'Sardine AI、Bolt、万事达卡决策智能'),
        ('第6层', '结账执行',     '#007B8A', '令牌化凭证提交、支付处理、订单确认',    'Stripe、PayPal、Adyen、Worldpay'),
        ('第5层', '商户赋能',     '#0A6688', '结构化产品数据源、目录API、库存及履约信号', 'Shopify、Salesforce Commerce、谷歌商家中心'),
        ('第4层', '卡片发行与钱包','#00B5E2', '代理令牌、消费策略、委托授权、钱包管理', '万事达卡Agent Pay、Visa TAP、PayPal代理工具包、Nekuda'),
        ('第3层', '支付轨道',     '#0D5C8C', '交易路由、清结算、跨境支付能力',        'Visa、万事达卡、SWIFT、Corpay'),
        ('第2层', 'Agentic协议',  '#0A3A60', 'ACP（OpenAI+Stripe）、AP2（谷歌）、UCP、Visa TAP、A2A', 'OpenAI、谷歌、Anthropic、Visa'),
        ('第1层', 'AI平台',       '#061428', 'LLM推理、工具调用、用户意图理解、多代理编排', 'OpenAI ChatGPT、谷歌Gemini、Anthropic Claude、Perplexity'),
    ]
    cw = [(PAGE_W-3*cm)*f for f in [0.10, 0.16, 0.36, 0.38]]
    td = [[Paragraph('<font color="white"><b>%s</b></font>' % h, S('th')) for h in ['层级','类别','功能','代表玩家']]]
    for lnum, lname, lc, lf, lp in layers:
        lnS = ParagraphStyle('ln', fontName=CNB, fontSize=8, textColor=WHITE, alignment=TA_CENTER)
        lnN = ParagraphStyle('lN', fontName=CNB, fontSize=8, textColor=WHITE)
        td.append([Paragraph('<font color="white"><b>%s</b></font>' % lnum, lnS),
                   Paragraph('<font color="white"><b>%s</b></font>' % lname, lnN),
                   Paragraph(lf, S('td')), Paragraph(lp, S('td'))])
    ts = TableStyle([
        ('GRID', (0,0), (-1,-1), 0.5, WHITE),
        ('TOPPADDING',    (0,0),(-1,-1), 6), ('BOTTOMPADDING',(0,0),(-1,-1),6),
        ('LEFTPADDING',   (0,0),(-1,-1), 5), ('RIGHTPADDING',  (0,0),(-1,-1),5),
        ('VALIGN',        (0,0),(-1,-1), 'MIDDLE'),
        ('BACKGROUND',    (0,0),(-1,0),  BLUE),
        ('LINEABOVE',     (0,0),(-1,0),  2.5, ACCENT),
    ])
    for i, (_, _, lc, _, _) in enumerate(layers):
        r = i + 1
        ts.add('BACKGROUND', (0,r),(1,r), colors.HexColor(lc))
        ts.add('BACKGROUND', (2,r),(-1,r), colors.HexColor('#F0F4F8') if r%2==0 else WHITE)
    t = Table(td, colWidths=cw)
    t.setStyle(ts)
    e.append(t)
    e.append(Paragraph('图表4.1：七层Agentic商务价值链。来源：rye.com（2026年），整合自公司公告及行业分析。', S('cap')))
    e.append(Spacer(1, 8))
    e.append(Paragraph('各层竞争动态分析', S('sec')))
    dynamics = [
        ('第1-2层（AI平台+协议）',
         '赢家通吃的动态格局可能形成。OpenAI和谷歌正在竞相将各自的协议（ACP vs. AP2/UCP）确立为行业标准。掌控发现和结账意图的平台就掌控了商业。'),
        ('第3-4层（轨道+卡片发行）',
         '万事达卡和Visa在这两层具备良好的定位，依托现有网络效应和商户关系。它们的代理令牌化框架将卡网络主导地位延伸至Agentic时代。'),
        ('第5层（商户赋能）',
         'Shopify、Salesforce和谷歌商家中心在提供机器可读产品数据方面具有结构性优势。没有结构化API的商户面临对AI代理的不可见性风险。'),
        ('第6-7层（结账+信任）',
         '竞争最激烈的层级。Stripe的ACP集成、Worldpay的MCP服务器和PayPal的代理工具包正面交锋。Sardine AI等欺诈和身份验证初创公司存在显著的白空间。'),
    ]
    for title, body in dynamics:
        e.append(Paragraph('<b>%s</b>' % title, S('sub')))
        e.append(Paragraph(body, S('body')))
    e.append(PageBreak())
    return e


def ch05():
    e = []
    e += ch_header('05', '协议之战：ACP、AP2、UCP、MCP、A2A与Visa TAP',
                   '六项竞争标准争相成为Agentic商务领域的HTTP协议。胜出者将定义谁掌控授权层。')
    e.append(Paragraph(
        'Agentic商务生态系统催生了协议标准的寒武纪大爆发，每项标准背后都有一家科技巨头支持。'
        '与以往的支付标准之战（如EMV、3DS）不同，这场竞争发生在应用层，'
        '胜出的协议将决定哪个平台掌控代理认证、意图信号传递和交易授权。', S('body')))
    e.append(Spacer(1, 8))
    protocols = [
        ('ACP', 'Agentic Commerce Protocol · 代理商业协议', 'OpenAI + Stripe', '2025年11月', '结账交易',
         '共享支付令牌；代理以令牌化凭证提交订单；商户通过Stripe或其他处理商处理',
         '已上线——ChatGPT即时结账与Etsy、Shopify合作；PayPal ACP服务器（2026年）',
         '最大AI平台分发（OpenAI）+ 最优支付基础设施（Stripe）',
         '封闭生态系统担忧；商户采用需逐一集成'),
        ('AP2', 'Agent Payments Protocol · 代理支付协议', '谷歌', '2025年9月', '支付授权（扩展A2A+MCP）',
         '代理支付授权的通用安全语言；扩展A2A以实现多代理协调',
         '已发布——Gemini辅助支付理论多于实践（Forrester，2025年9月）',
         '谷歌的生态系统覆盖；专为多代理环境设计',
         '落后于OpenAI的实际部署；Gemini支付尚未大规模上线'),
        ('UCP', 'Universal Commerce Protocol · 通用商业协议', '谷歌 + Shopify', '2025年', '完整购物旅程（发现→结账）',
         '允许AI代理通过结构化API直接查询履约能力、库存和定价',
         '已上线——谷歌搜索AI模式；Shopify商户集成',
         '覆盖完整购物旅程而非仅支付；通过Shopify拥有强大商户分发',
         '与AP2重叠；谷歌自身协议栈存在碎片化'),
        ('MCP', 'Model Context Protocol · 模型上下文协议', 'Anthropic（开放标准）', '2024年', '基础代理基础设施（工具+数据访问）',
         '连接AI代理与数据源和工具的标准；Stripe（远程MCP服务器）、Worldpay提供支付专用扩展',
         '广泛采用——Stripe MCP、Worldpay MCP、PayPal MCP服务器均已上线',
         '开放标准，行业广泛采用；基础设施层，对支付无偏好性',
         '非支付专用；需在其上叠加支付层扩展（ACP、AP2）'),
        ('A2A', 'Agent-to-Agent Protocol · 代理间协议', '谷歌', '2025年', '多代理协调',
         '使AI代理相互通信以完成复杂的多步骤商业工作流',
         '框架已发布；生产实现正在涌现',
         '对复杂Agentic工作流至关重要；企业用例的基础',
         '基础设施层；支付结果取决于AP2集成'),
        ('Visa TAP', 'Trusted Agent Protocol · 可信代理协议', 'Visa', '2025年10月', '代理身份验证+授权支付',
         '在CDN层通过Web Bot Auth标准验证AI代理合法性；在标准卡片字段中提交动态令牌验证码',
         '已上线——商户框架已发布；数百万Visa受理商户可使用',
         '依托Visa现有商户关系；通过CDN实现零代码商户集成',
         '以卡网络为中心；可能不延伸至非卡支付方式'),
    ]
    pcolors = {'ACP':'#0A2240','AP2':'#007B8A','UCP':'#00B5E2','MCP':'#4A4A4A','A2A':'#E8622A','Visa TAP':'#1A3A7C'}
    for pname, pfull, pbacker, plaunch, pscope, pmech, pstatus, pstr, prisk in protocols:
        pc = pcolors.get(pname, '#0A2240')
        hdrS = ParagraphStyle('ph', fontName=CNB, fontSize=13, textColor=WHITE, alignment=TA_CENTER)
        metaS = ParagraphStyle('pm', fontName=CN, fontSize=9, leading=13, textColor=WHITE)
        hdr = Table([[
            Paragraph('<font color="white"><b>%s</b></font>' % pname, hdrS),
            Paragraph('<b>%s</b><br/><font color="#8BBFD4">支持方：%s</font><br/><font color="#8BBFD4">发布：%s  |  覆盖：%s</font>' % (pfull, pbacker, plaunch, pscope), metaS),
        ]], colWidths=[2.0*cm, PAGE_W-3*cm-2.0*cm])
        hdr.setStyle(TableStyle([
            ('BACKGROUND',(0,0),(-1,-1),colors.HexColor(pc)),
            ('TOPPADDING',(0,0),(-1,-1),7),('BOTTOMPADDING',(0,0),(-1,-1),7),
            ('LEFTPADDING',(0,0),(-1,-1),7),('RIGHTPADDING',(0,0),(-1,-1),7),
            ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
        ]))
        e.append(hdr)
        det = Table([
            [Paragraph('<b>运作机制</b>', S('td')), Paragraph(pmech, S('td'))],
            [Paragraph('<b>当前状态</b>', S('td')), Paragraph(pstatus, S('td'))],
            [Paragraph('<b>核心优势</b>', S('td')), Paragraph(pstr, S('td'))],
            [Paragraph('<b>主要风险</b>', S('td')), Paragraph(prisk, S('td'))],
        ], colWidths=[2.2*cm, PAGE_W-3*cm-2.2*cm])
        det.setStyle(TableStyle([
            ('ROWBACKGROUNDS',(0,0),(-1,-1),[WHITE, colors.HexColor('#F5F7FA')]),
            ('GRID',(0,0),(-1,-1),0.3,LINE),
            ('TOPPADDING',(0,0),(-1,-1),4),('BOTTOMPADDING',(0,0),(-1,-1),4),
            ('LEFTPADDING',(0,0),(-1,-1),5),('RIGHTPADDING',(0,0),(-1,-1),5),
            ('VALIGN',(0,0),(-1,-1),'TOP'),
        ]))
        e.append(det)
        e.append(Spacer(1, 7))
    e.append(Paragraph('协议格局综合研判', S('sec')))
    e.append(hbox(
        '<b>战略展望：</b>协议之战短期内不太可能产生单一赢家。更可能的结果是双层格局：'
        '<b>MCP成为基础设施标准</b>（类似TCP/IP），而<b>ACP和Visa TAP竞争结账层主导权</b>'
        '（取决于商户采用率和消费者信任）。谷歌的AP2/UCP可能在企业级和复杂多步骤工作流中取得优势。',
        TEAL, colors.HexColor('#E8F4F7')))
    e.append(PageBreak())
    return e


def ch06():
    e = []
    e += ch_header('06', '竞争格局：传统巨头与新兴挑战者',
                   '争夺Agentic商务层的竞争吸引了全球最大的支付公司和一批AI原生挑战者同场角力。')
    fig, ax = plt.subplots(figsize=(12, 7))
    fig.patch.set_facecolor('white')
    ax.set_facecolor('#F7F9FB')
    players = {
        'OpenAI':     (9.5,5.0,400,'#0A2240','AI平台'),
        'Google':     (9.0,6.5,380,'#0A2240','AI平台'),
        'Anthropic':  (8.5,3.5,250,'#4A4A4A','AI平台'),
        'Mastercard': (6.5,9.5,500,'#EB001B','卡组织'),
        'Visa':       (6.0,9.0,480,'#1A1F71','卡组织'),
        'Stripe':     (7.5,8.5,350,'#635BFF','支付基础设施'),
        'PayPal':     (6.8,8.0,400,'#003087','支付基础设施'),
        'Worldpay':   (4.5,7.5,200,'#007B8A','支付基础设施'),
        'Amazon':     (8.5,8.0,420,'#FF9900','超级平台'),
        'Shopify':    (6.0,6.5,280,'#96BF48','商户赋能'),
        'Nekuda':     (7.0,5.5,120,'#00B5E2','初创公司'),
        'Payman':     (7.5,4.5,100,'#00B5E2','初创公司'),
        'Sardine AI': (7.0,6.0,110,'#E8622A','安全'),
        'Perplexity': (8.0,4.0,150,'#6B5EAC','AI平台'),
    }
    gcolors = {'AI平台':'#0A2240','卡组织':'#D32F2F','支付基础设施':'#1565C0',
               '超级平台':'#E65100','商户赋能':'#2E7D32','初创公司':'#00B5E2','安全':'#E8622A'}
    for name,(x,y,sz,c,g) in players.items():
        ax.scatter(x,y,s=sz,color=c,alpha=0.75,zorder=5,edgecolors='white',linewidths=1)
        ax.annotate(name,(x,y),xytext=(x+0.15,y+0.3),fontsize=8,color='#2A2A2A',ha='center',va='bottom')
    ax.axvline(x=7,color='#D0D0D0',linewidth=1,linestyle='--')
    ax.axhline(y=6.5,color='#D0D0D0',linewidth=1,linestyle='--')
    ax.set_xlim(3,10.5); ax.set_ylim(2,11)
    ax.set_xlabel('AI能力强度 →', fontsize=10, color='#4A4A4A')
    ax.set_ylabel('支付基础设施强度 →', fontsize=10, color='#4A4A4A')
    ax.set_title('Agentic Payment 竞争定位矩阵', fontsize=12, fontweight='bold', color='#0A2240', pad=12)
    ax.text(9.5,10.5,'主导地位',fontsize=8,color='#C62828',ha='center',alpha=0.8)
    ax.text(4.5,10.5,'支付传统玩家',fontsize=8,color='#D32F2F',ha='center',alpha=0.8)
    ax.text(9.5,3.5,'AI原生挑战者',fontsize=8,color='#1565C0',ha='center',alpha=0.8)
    ax.text(4.5,3.5,'细分市场',fontsize=8,color='#9B9B9B',ha='center',alpha=0.8)
    patches = [mpatches.Patch(color=c,label=g) for g,c in gcolors.items()]
    ax.legend(handles=patches,loc='lower right',fontsize=7.5,framealpha=0.9,title='玩家类型',title_fontsize=8)
    ax.grid(True,color='#E0E0E0',linewidth=0.5,alpha=0.7)
    ax.spines['top'].set_visible(False); ax.spines['right'].set_visible(False)
    plt.tight_layout()
    e.append(img(fig, 16, 8.5))
    e.append(Paragraph(
        '图表6.1：Agentic支付竞争定位矩阵，横轴为AI能力强度，纵轴为支付基础设施强度。'
        '气泡大小反映Agentic商务相关性估算。基于截至2026年Q1的公开信息定性评估。', S('cap')))
    e.append(Spacer(1, 8))
    e.append(Paragraph('重点玩家分析', S('sec')))
    pa = [
        ('万事达卡 — Agent Pay', '#EB001B',
         '万事达卡于2025年4月推出Agent Pay，2025年9月发布新工具。该框架使商户可通过现有卡片基础设施接受令牌化代理交易。'
         '核心创新：在CDN层实现的Web Bot Auth标准允许商户零代码接入。2025年4月宣布与Corpay战略合作（3亿美元，约3%股权）。'
         '估计AI代理到2030年可影响全球超3万亿美元消费。'),
        ('Visa — 智能商务 + TAP', '#1A1F71',
         'Visa与万事达卡同步推出智能商务，随后于2025年10月发布可信代理协议（TAP）。'
         'TAP聚焦代理身份证明，通过动态令牌验证码防止重放攻击。Visa超过一半的交易量已来自电商。'
         '首席产品官Jack Forestell：为商户提供零代码功能，安全识别具有购买意图的代理。'),
        ('OpenAI — ACP + 即时结账', '#0A2240',
         'OpenAI 2025年11月通过与Stripe共同开发的ACP推出即时结账，是迄今最具商业实践意义的Agentic支付实现。'
         '与Etsy、Shopify和PayPal的合作在启动时即创建了商户网络。开源的ACP旨在产生网络效应。'
         '还推出了共享支付令牌，与PayPal的企业级合作将ChatGPT Enterprise延伸至PayPal 24000余名员工。'),
        ('谷歌 — AP2 + UCP + A2A', '#EA4335',
         '谷歌拥有最全面的协议组合，但在实际支付部署方面落后。AP2（2025年9月）扩展MCP和A2A以实现支付授权。'
         'UCP（与Shopify合作）覆盖完整购物旅程。通过谷歌搜索AI模式与亚马逊集成"代我购买"功能。'
         'Forrester指出截至2025年底Gemini支付理论多于实践。'),
        ('PayPal — 代理工具包 + ACP合作', '#003087',
         'PayPal处于独特的双重定位：既是支付基础设施提供商，也是AI商务参与者。'
         '其MCP服务器（2025年推出）使开发者可构建Agentic支付体验。'
         '代理工具包允许商户注册AI代理接受PayPal/Venmo。'
         '与OpenAI的ACP服务器合作将PayPal数千万商户引入ChatGPT商务。'),
        ('Stripe — ACP基础设施 + 远程MCP', '#635BFF',
         'Stripe定位为Agentic商务的基础设施层，与OpenAI共同开发ACP并部署远程MCP服务器，'
         '使AI代理可直接与Stripe API交互。ACP框架下商户并非必须使用Stripe，'
         '但Stripe的深度集成使其在Agentic交易中成为强有力的默认轨道。'),
    ]
    for pname, pcolor, ptext in pa:
        row = [[
            Paragraph('<font color="white"><b>%s</b></font>' % pname,
                      ParagraphStyle('pn', fontName=CNB, fontSize=9, textColor=WHITE)),
            Paragraph(ptext, S('td')),
        ]]
        t = Table(row, colWidths=[3.5*cm, PAGE_W-3*cm-3.5*cm])
        t.setStyle(TableStyle([
            ('BACKGROUND',(0,0),(0,0),colors.HexColor(pcolor)),
            ('BACKGROUND',(1,0),(1,0),WHITE),
            ('TOPPADDING',(0,0),(-1,-1),7),('BOTTOMPADDING',(0,0),(-1,-1),7),
            ('LEFTPADDING',(0,0),(-1,-1),7),('RIGHTPADDING',(0,0),(-1,-1),7),
            ('VALIGN',(0,0),(-1,-1),'TOP'),
            ('LINEBELOW',(0,0),(-1,-1),0.5,LINE),
        ]))
        e.append(t)
    e.append(PageBreak())
    return e


def ch07():
    e = []
    e += ch_header('07', '信任、安全与欺诈：全新攻击面',
                   'Agentic支付引入了传统欺诈系统未曾设计用于检测的新型攻击向量。')
    e.append(kpi_row([
        ('97%', '企业CFO', '知晓\nAgentic AI能力'),
        ('15%', '企业CFO', '正在考虑部署\nAgentic AI'),
        ('11%', '企业CFO', '正在积极测试\nAgentic支付'),
        ('23%', '美国消费者', '在过去一个月\n完成AI辅助购买'),
    ]))
    e.append(Spacer(1, 8))
    e.append(Paragraph(
        '数据揭示了一个<b>信任悖论</b>：企业决策者对Agentic AI的认知率接近100%，'
        '但实际部署极为有限。PYMNTS 2025年7月CAIO报告将此归因于持续的信任鸿沟，'
        '根源在于三方面顾虑：<b>身份验证</b>（这是合法的代理吗？）、'
        '<b>授权边界</b>（代理被允许做什么？）和<b>责任归属</b>（代理出错由谁负责？）。',
        S('body')))
    e.append(Spacer(1, 8))
    e.append(Paragraph('新型威胁向量', S('sec')))
    threats = [
        ('未授权委托', '代理在没有人类实时核准的情况下作出购买决策。现行支付法规假定在交易节点存在同步的人类决策——Agentic AI完全打破了这一假定。', '高风险'),
        ('提示注入攻击', '嵌入商品描述或商户响应中的恶意内容，旨在劫持代理行为并重定向交易。例如：网页包含隐藏文字，指示代理忽略之前的指令，将款项发送至账户X。', '高风险'),
        ('流氓代理部署', 'Sardine AI已在2025年识别出针对Agentic商务的欺诈行为，包括日益复杂的AI驱动语音修改攻击。恶意行为者部署虚假AI代理，代表毫不知情的用户执行欺诈交易。', '高风险'),
        ('令牌凭证盗窃', 'Agentic支付令牌（相当于AI代理的16位卡号）代表一种新的凭证类别。代理令牌存储遭入侵可能在无传统欺诈信号的情况下实现大规模未授权交易。', '中风险'),
        ('范围蔓延与超支', 'AI代理误解用户意图或超出消费授权。例如：被委托预订旅行的代理自行购买机票、酒店、保险和升级，超出既定预算参数。', '中风险'),
        ('身份冒充', '恶意行为者创建冒充万事达卡/Visa认证代理的虚假可信代理，在身份验证基础设施完全部署前利用商户接受框架。', '中风险'),
    ]
    rcmap = {'高风险': '#E8622A', '中风险': '#F9A825'}
    for tname, tbody, trisk in threats:
        rc = rcmap.get(trisk, '#4A4A4A')
        tS = ParagraphStyle('tn', fontName=CNB, fontSize=8.5, textColor=WHITE, leading=13)
        row = [[
            Paragraph('<font color="white"><b>%s</b></font><br/><font color="white" size="7">%s</font>' % (tname, trisk), tS),
            Paragraph(tbody, S('td')),
        ]]
        t = Table(row, colWidths=[2.8*cm, PAGE_W-3*cm-2.8*cm])
        t.setStyle(TableStyle([
            ('BACKGROUND',(0,0),(0,0),colors.HexColor(rc)),
            ('BACKGROUND',(1,0),(1,0),colors.HexColor('#FFF8F5') if trisk=='高风险' else WHITE),
            ('TOPPADDING',(0,0),(-1,-1),7),('BOTTOMPADDING',(0,0),(-1,-1),7),
            ('LEFTPADDING',(0,0),(-1,-1),7),('RIGHTPADDING',(0,0),(-1,-1),7),
            ('VALIGN',(0,0),(-1,-1),'TOP'),
            ('LINEBELOW',(0,0),(-1,-1),0.5,LINE),
        ]))
        e.append(t)
    e.append(Spacer(1, 8))
    e.append(Paragraph('行业应对措施', S('sec')))
    resp = [
        ('万事达卡 — Agent Pay + CDN验证',
         '在CDN层实施Web Bot Auth标准，使商户无需代码改动即可验证代理真实性。动态令牌验证码提供加密保护、单次使用的交易凭证。'),
        ('Visa — 可信代理协议（TAP）',
         'TAP专注于代理身份证明，确保万事达卡认可的代理可被识别，同时屏蔽不可信流量。动态验证码防止重放攻击。'),
        ('OpenAI/Stripe — ACP信任模型',
         '在ACP框架下商户保留欺诈决策权——代理提交订单，但商户执行自己的欺诈检查并可拒绝代理发起的订单。人工确认仍可作为可选保护栏。'),
        ('谷歌 — AP2协议',
         'AP2明确将安全、可问责、已授权的商业作为核心设计要求，以支付专用授权构造扩展MCP和A2A。'),
        ('CBA/DWT白皮书（2026年1月）',
         '首份Agentic支付消费者保护综合监管分析，指出需要明确的授权框架、清晰的责任链以及AI发起交易的强制审计追踪。'),
    ]
    for rt, rb in resp:
        e.append(Paragraph('<b>%s</b>' % rt, S('sub')))
        e.append(Paragraph(rb, S('body')))
    e.append(PageBreak())
    return e


def ch08():
    e = []
    e += ch_header('08', '监管与合规环境',
                   '现行支付法律是为人类发起的交易而制定的。Agentic AI制造了法律真空——在监管参与方面的先行者将塑造后续标准。')
    e.append(Paragraph(
        '当AI代理点击付款时，核心合规问题集中在<b>认证、授权、欺诈责任和消费者保护</b>。'
        '传统监管框架（EFTA、Regulation E、PSD2/3、英国PSR）假定人类作出同步决策——'
        'Agentic AI从根本上打破了这一假定。NASCUS（2026年2月）指出：用户通常向代理提供目标，'
        '由代理识别并执行实现这些目标的交易，而无需传统支付法律所假定的同步人类决策。',
        S('body')))
    e.append(Spacer(1, 8))
    e.append(Paragraph('全球监管格局', S('sec')))
    hdrs = ['地区', '核心监管框架', 'Agentic支付现状', '主要监管空白', '预计时间线']
    rows = [
        ['美国', 'EFTA / Regulation E / CFPB', '无专项规定；现行规定类比适用', '授权定义；AI发起未授权交易的责任归属', '监管指引预期2026-2027年'],
        ['欧盟', 'PSD2 / PSD3（拟议）/ DORA', 'PSD3仍在制定中；代理发起支付的SCA要求不明确', '强客户认证适用性；AI法案下的可解释性', 'PSD3预期2026年；AI法案2025-2026年执法'],
        ['英国', 'PSR / FCA / 支付服务条例', 'FCA持续监测；尚无具体指引', '代理发起交易的SCA；责任模型', 'FCA征询意见预期2026年'],
        ['美国（稳定币）', 'GENIUS法案（2025年7月）', '稳定币发行框架已生效；PayPal/Fiserv为早期采用者', '州级差异；加州数字金融资产法（2026年7月）', '生效中——加州法律2026年7月'],
    ]
    cw = [(PAGE_W-3*cm)*f for f in [0.12, 0.20, 0.25, 0.26, 0.17]]
    td = [[Paragraph(h, S('th')) for h in hdrs]]
    for row in rows:
        td.append([Paragraph(row[0], S('tdb'))] + [Paragraph(c, S('td')) for c in row[1:]])
    t = Table(td, colWidths=cw)
    t.setStyle(TableStyle([
        ('BACKGROUND',(0,0),(-1,0),BLUE),
        ('ROWBACKGROUNDS',(0,1),(-1,-1),[WHITE,colors.HexColor('#F5F7FA')]),
        ('GRID',(0,0),(-1,-1),0.5,LINE),
        ('TOPPADDING',(0,0),(-1,-1),5),('BOTTOMPADDING',(0,0),(-1,-1),5),
        ('LEFTPADDING',(0,0),(-1,-1),5),('RIGHTPADDING',(0,0),(-1,-1),5),
        ('VALIGN',(0,0),(-1,-1),'TOP'),
        ('LINEABOVE',(0,0),(-1,0),2.5,ACCENT),
    ]))
    e.append(t)
    e.append(Paragraph('图表8.1：截至2026年Q1各地区Agentic支付监管状态。数据基于公开监管公告和行业分析。', S('cap')))
    e.append(Spacer(1, 8))
    e.append(Paragraph('核心监管挑战', S('sec')))
    chlgs = [
        ('无实时人工确认的授权',
         '现行支付法律要求消费者在交易节点明确授权。Agentic AI系统在预授权目标下运作——这是一种根本不同的同意架构。监管机构必须确定代理的常规授权是否满足现有要求，还是需要新的法律框架。'),
        ('责任链归属',
         '在Agentic商务中，责任链涵盖：用户、AI平台、支付处理商、发卡银行、卡组织、商户，以及可能的Plaid等数据聚合商。CBA 2026年白皮书将此认定为首要未决问题：当AI的行为不符合消费者意愿时，谁来承担责任？'),
        ('强客户认证（SCA）兼容性',
         'PSD2/3的SCA要求是为人机认证（生物识别、设备绑定）设计的。代理对系统的认证需要现有SCA框架未曾考虑的新型凭证类型和验证流程。'),
        ('消费者争议解决',
         '拒付和争议流程假定存在一个能够证明未授权交易特征的人类。当AI代理基于误解用户意图授权购买时，争议经济学变得复杂——传统拒付机制可能需要重新设计。'),
        ('跨境司法管辖复杂性',
         'Agentic商务天然跨越国界——AI代理可能在美国平台发现商品、通过欧洲收单行处理支付、配送至亚太地区。适用哪个监管框架？目前尚无国际协调机制。'),
    ]
    for ct, cb in chlgs:
        e.append(Paragraph('<b>%s</b>' % ct, S('sub')))
        e.append(Paragraph(cb, S('body')))
    e.append(Spacer(1, 6))
    e.append(hbox(
        '<b>合规建议：</b>FTI Consulting（2026年）建议企业在构建Agentic支付产品前重新绘制监管边界——'
        '识别代理发起的支付在受监管活动中的定位，以及历史假设不再成立的领域。'
        '避免将Agentic支付视为附加功能；在设计阶段就嵌入治理结构。',
        ORANGE, colors.HexColor('#FFF3EE')))
    e.append(PageBreak())
    return e


def ch09():
    e = []
    e += ch_header('09', '战略启示与行动建议',
                   '面向支付公司、商户、银行、金融科技企业和政策制定者的战略要务。')
    e.append(Paragraph(
        '向Agentic支付的转型不是渐进式演变——它是被压缩至2-3年窗口期的结构性颠覆。'
        '在等待协议整合期间选择观望的组织，将面临被从架构中剔除的风险。', S('body')))
    e.append(Spacer(1, 8))
    fig, ax = plt.subplots(figsize=(12, 6.5))
    fig.patch.set_facecolor('white')
    ax.set_facecolor('#F7F9FB')
    inits = [
        ('代理令牌化', 9.0, 8.5, 350, '#0A2240'),
        ('信任基础设施', 8.5, 9.0, 320, '#E8622A'),
        ('协议采用(ACP/TAP)', 8.0, 7.5, 280, '#0A2240'),
        ('结构化产品API', 7.5, 9.2, 300, '#2E7D32'),
        ('委托授权框架', 7.8, 8.0, 260, '#007B8A'),
        ('主动监管参与', 6.5, 9.5, 240, '#9B59B6'),
        ('代理欺诈模型', 8.2, 7.8, 220, '#E8622A'),
        ('消费者同意体验', 6.0, 8.5, 200, '#F9A825'),
        ('跨境合规地图', 5.5, 7.0, 180, '#9B9B9B'),
    ]
    for name, urgency, impact, size, color in inits:
        ax.scatter(urgency, impact, s=size, color=color, alpha=0.75, zorder=5, edgecolors='white', linewidths=1.5)
        ax.annotate(name, (urgency, impact), xytext=(urgency+0.12, impact+0.22),
                    fontsize=8, color='#2A2A2A', ha='left', va='bottom')
    ax.axvline(x=7, color='#D0D0D0', linewidth=1.2, linestyle='--')
    ax.axhline(y=8, color='#D0D0D0', linewidth=1.2, linestyle='--')
    ax.set_xlim(4, 10.5); ax.set_ylim(5.5, 10.5)
    ax.set_xlabel('紧迫性（立即行动 →）', fontsize=10, color='#4A4A4A')
    ax.set_ylabel('战略价值（高价值 →）', fontsize=10, color='#4A4A4A')
    ax.set_title('战略举措优先级矩阵', fontsize=12, fontweight='bold', color='#0A2240', pad=12)
    ax.text(8.8,10.2,'紧急优先',fontsize=8,color='#C62828',ha='center',alpha=0.8)
    ax.text(5.3,10.2,'投资备战',fontsize=8,color='#1565C0',ha='center',alpha=0.8)
    ax.text(8.8,6.2,'快速见效',fontsize=8,color='#2E7D32',ha='center',alpha=0.8)
    ax.text(5.3,6.2,'持续观察',fontsize=8,color='#9B9B9B',ha='center',alpha=0.8)
    ax.grid(True, color='#E0E0E0', linewidth=0.5, alpha=0.7)
    ax.spines['top'].set_visible(False); ax.spines['right'].set_visible(False)
    plt.tight_layout()
    e.append(img(fig, 15, 7.5))
    e.append(Paragraph('图表9.1：战略举措优先级矩阵，横轴为紧迫性，纵轴为业务影响力。气泡大小反映资源投入估算。', S('cap')))
    e.append(Spacer(1, 10))
    stks = [
        ('支付网络（Visa、万事达卡等）', [
            ('将代理令牌化确立为强制性标准', '推动通过网络框架将代理令牌发行作为所有Agentic交易的必要条件。使自身令牌格式成为AI代理默认凭证的网络，将复制卡网络在电商时代的核心地位。', '立即行动'),
            ('投资Web Bot Auth基础设施', '万事达卡的CDN层商户验证是优秀范例。各网络应与主要CDN提供商（Cloudflare、Akamai、Fastly）合作，在AI平台构建专有钱包解决方案之前使代理验证基础设施无处不在。', '立即行动'),
        ]),
        ('银行与发行机构', [
            ('推出委托授权产品', '构建产品，允许消费者定义消费策略并将有限的采购权限委托给AI代理，包含明确的限额、商户限制和品类约束。定位为信任锚而非仅仅是资金来源。', '6个月内'),
            ('开发代理原生欺诈模型', '以人类行为模式训练的传统欺诈模型将对合法的Agentic交易产生过多误报。投资能够区分授权代理行为与欺诈的机器学习模型。', '6个月内'),
            ('主动参与监管制定', '向CFPB、FCA和EBA提交关于Agentic支付授权框架的意见函。塑造监管结果的银行将在合规层面领先于坐等观望的竞争对手。', '持续推进'),
        ]),
        ('商户与零售商', [
            ('构建代理可读产品基础设施', '实施与ACP和UCP兼容的结构化产品数据源。麦肯锡指出：胜出者将是那些向AI代理提供正确数据的零售商——实时库存、定价、产品和采购信息。没有结构化API的商户面临Agentic不可见性风险。', '立即行动'),
            ('参与早期Agentic商务试点', 'Etsy和Shopify的OpenAI即时结账合作，提供了关于转化率、客单价和欺诈率的竞争数据。早期参与者将在制定协议要求时拥有更有利的话语权。', '立即行动'),
        ]),
        ('金融科技与初创公司', [
            ('聚焦信任基础设施的白空间', '代理身份验证、消费策略执行和审计追踪管理代表了七层价值链中服务不足的层级。Sardine AI在Agentic欺诈检测领域的早期布局验证了这一机遇。', '现在'),
            ('构建代理钱包基础设施', 'Nekuda（Agentic支付轨道）和Payman（代理钱包）等公司正在定义一个全新的基础设施类别。存在打造Stripe for Agents的机遇——对开发者友好、抽象协议复杂性的轨道。', '现在'),
        ]),
        ('监管机构与政策制定者', [
            ('制定AI支付授权框架', '发布指引，明确现有授权要求如何适用于Agentic交易。消费者保护需要在大规模部署之前建立明确的常规授权标准。', '紧急'),
            ('推动国际责任链协调', '二十国集团和金融稳定委员会应将Agentic商务跨境责任协调列为优先事项。美国GENIUS法案稳定币框架为Agentic支付治理提供了参考模型。', '12-18个月'),
        ]),
    ]
    tmap = {'立即行动':'#C62828','现在':'#C62828','紧急':'#C62828','6个月内':'#E65100','持续推进':'#1565C0','12-18个月':'#2E7D32'}
    for stitle, srecs in stks:
        e.append(Paragraph(stitle, S('sec')))
        for rtitle, rbody, rtiming in srecs:
            tc = tmap.get(rtiming, '#4A4A4A')
            tS = ParagraphStyle('rt', fontName=CNB, fontSize=8, textColor=colors.HexColor(tc), alignment=TA_CENTER)
            row = [[
                Paragraph('<font color="%s"><b>%s</b></font>' % (tc, rtiming), tS),
                Paragraph('<b>%s</b><br/>%s' % (rtitle, rbody), S('td')),
            ]]
            t = Table(row, colWidths=[2.0*cm, PAGE_W-3*cm-2.0*cm])
            t.setStyle(TableStyle([
                ('BACKGROUND',(0,0),(0,0),colors.HexColor('#F5F7FA')),
                ('BACKGROUND',(1,0),(1,0),WHITE),
                ('LINEABOVE',(0,0),(-1,0),0.5,LINE),
                ('LINEBELOW',(0,-1),(-1,-1),0.5,LINE),
                ('TOPPADDING',(0,0),(-1,-1),7),('BOTTOMPADDING',(0,0),(-1,-1),7),
                ('LEFTPADDING',(0,0),(-1,-1),7),('RIGHTPADDING',(0,0),(-1,-1),7),
                ('VALIGN',(0,0),(-1,-1),'TOP'),
            ]))
            e.append(t)
        e.append(Spacer(1, 5))
    e.append(PageBreak())
    return e


def ch10():
    e = []
    e += ch_header('10', '研究方法与来源',
                   '本报告综合整理了截至2026年3月的一手研究、机构预测、公司公告和监管文件。')
    e.append(Paragraph('研究方法', S('sec')))
    e.append(Paragraph(
        '本报告采用多来源研究方法，综合：（1）对麦肯锡、BCG、摩根士丹利、科尔尼、a16z和'
        'Forrester机构研究报告的系统性检索与分析；（2）万事达卡、Visa、OpenAI、谷歌、'
        'Stripe、PayPal等公司官方公告的梳理；（3）CBA、NASCUS、FTI Consulting等机构'
        '监管文件、白皮书和指引的分析；（4）PYMNTS、CNBC、Forbes、American Banker等'
        '媒体行业新闻的综合整理。所有数据均来源于截至2026年3月的公开披露。',
        S('body')))
    e.append(Spacer(1, 8))
    e.append(Paragraph('主要参考来源', S('sec')))
    srcs = [
        ('麦肯锡', '2025年10月', 'Agentic商务机遇：AI代理如何开启消费者和商户的新时代。预测：美国B2C 1万亿美元/全球3-5万亿美元（至2030年）。'),
        ('BCG', '2025年9月', '全球支付收入预测至2029年达2.4万亿美元；Agentic AI为主要驱动力。另见：从网点到机器人（2025年11月）。'),
        ('摩根士丹利', '2025年', 'Agentic购物者到2030年可贡献美国电商1900-3850亿美元消费。23%美国人过去一个月完成AI辅助购买。'),
        ('科尔尼（Kearney）', '2025年', 'Agentic支付：数字商业的新前沿。七层价值链框架和收入颠覆分析。'),
        ('a16z 金融科技通讯', '2025年5月', '我的代理如何付款？Agentic支付栈分析、万事达卡/Corpay合作及创业生态系统。'),
        ('Forrester Research', '2025年9月', '美国B2C电商中Agentic支付的竞争现状评估：实际部署vs宣布能力对比。'),
        ('万事达卡', '2025年4月+9月', 'Agent Pay发布；受理框架；Web Bot Auth标准；新工具与合作发布。'),
        ('Visa', '2025年10月', '智能商务与可信代理协议（TAP）发布。'),
        ('OpenAI + Stripe', '2025年11月', 'ACP开放标准；与Etsy/Shopify的即时结账；共享支付令牌。'),
        ('谷歌', '2025年', 'AP2协议；与Shopify的UCP；A2A多代理协调协议；谷歌云与PayPal合作。'),
        ('CBA + DWT', '2026年1月', 'Agentic AI支付：驾驭消费者保护、创新与合规。消费者银行家协会联合白皮书。'),
        ('NASCUS', '2026年2月', '当AI点击付款：Agentic商务中新兴的合规风险。'),
        ('FTI Consulting', '2026年', '支付2026：当创新超越监管边界。'),
        ('支付协会（TPA）', '2025-2026年', 'Agentic AI支付的兴起：标准、风险与未来发展。'),
        ('PYMNTS', '2025年', '2025年：AI代理进入支付领域；Visa、万事达卡、PayPal推动Agentic AI商务繁荣。CAIO报告（2025年7月）。'),
        ('rye.com', '2026年', 'Agentic商务格局：2026年谁在构建什么。覆盖50+玩家的七层框架。'),
    ]
    cw = [(PAGE_W-3*cm)*f for f in [0.20, 0.10, 0.70]]
    td = [[Paragraph(h, S('th')) for h in ['来源机构', '发布时间', '核心贡献']]]
    for src, dt, cont in srcs:
        td.append([Paragraph(src, S('tdb')), Paragraph(dt, S('tdc')), Paragraph(cont, S('td'))])
    t = Table(td, colWidths=cw)
    t.setStyle(TableStyle([
        ('BACKGROUND',(0,0),(-1,0),BLUE),
        ('ROWBACKGROUNDS',(0,1),(-1,-1),[WHITE,colors.HexColor('#F5F7FA')]),
        ('GRID',(0,0),(-1,-1),0.4,LINE),
        ('TOPPADDING',(0,0),(-1,-1),4),('BOTTOMPADDING',(0,0),(-1,-1),4),
        ('LEFTPADDING',(0,0),(-1,-1),5),('RIGHTPADDING',(0,0),(-1,-1),5),
        ('VALIGN',(0,0),(-1,-1),'TOP'),
        ('LINEABOVE',(0,0),(-1,0),2.5,ACCENT),
    ]))
    e.append(t)
    e.append(Spacer(1, 10))
    e.append(Paragraph('重要声明', S('sec')))
    for d in [
        '市场规模数据来自第三方研究机构的预测，存在较大不确定性，实际结果可能与预测存在实质性差异。',
        'Agentic支付生态系统正在快速演变。协议状态、公司能力和监管立场在本报告研究截止日期（2026年3月）后可能已发生变化。',
        '本报告不构成投资建议。竞争格局评估属定性判断，仅基于公开可获取的信息。',
        '所有公司名称、标志和商标归各自所有者所有。',
    ]:
        e.append(Paragraph('· ' + d, S('bullet')))
    e.append(Spacer(1, 20))
    e.append(HRFlowable(width='100%', thickness=1, color=ACCENT))
    e.append(Spacer(1, 10))
    e.append(Paragraph(
        '本报告采用AI辅助研究综合方法生成。所有预测和市场数据均来自具名第三方机构。'
        '战略建议反映截至2026年3月公开信息的综合分析。', S('cap')))
    return e


def build_report(output='/data/workspace/Agentic_Payment_研究报告_简体中文.pdf'):
    doc = SimpleDocTemplate(
        output, pagesize=A4,
        leftMargin=1.5*cm, rightMargin=1.5*cm,
        topMargin=1.4*cm, bottomMargin=2.2*cm,
        title='Agentic Payment：数字商业的下一个前沿',
        author='战略研究',
    )
    footer = Footer()
    story = [PageBreak()]  # cover is page 1
    story += build_toc()
    story += ch01()
    story += ch02()
    story += ch03()
    story += ch04()
    story += ch05()
    story += ch06()
    story += ch07()
    story += ch08()
    story += ch09()
    story += ch10()
    doc.build(story, onFirstPage=draw_cover, onLaterPages=footer)
    print('Done: ' + output)
    return output


if __name__ == '__main__':
    build_report()

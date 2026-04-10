"""中医古籍处方提取与病症治疗成本分析模块

从古籍中提取处方数据，匹配药材价格库，计算病症治疗成本。

数据流：
    古籍文本 → 处方提取(药材+用量+病症) → 药材名映射 → 价格查询 → 成本计算
"""

import os
import re
import json
import sqlite3
import logging
from collections import defaultdict

from db import get_connection, DB_PATH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BOOKS_DIR = os.path.join(os.path.dirname(__file__), "data", "tcm_books")

# ── 古籍文件分类 ──────────────────────────────────────────

# 方书类（含处方组成和用量，最有价值）
FORMULA_BOOKS = [
    "084-汤头歌诀.txt",         # 200+ 经典方剂，结构化最好
    "087-医方集解.txt",         # 详细方剂解说
    "059-太平惠民和剂局方.txt",  # 官方药典
    "075-肘后备急方.txt",       # 急症处方
    "049-五十二病方.txt",       # 最早的方书
    "051-千金翼方.txt",         # 药王孙思邈
    "070-奇效良方.txt",
    "077-严氏济生方.txt",
    "097-验方新编.txt",
    "060-圣济总录.txt",
    "074-普济方.txt",
    "055-太平圣惠方.txt",
    "085-时方歌括.txt",
    "086-时方妙用.txt",
    "088-成方切用.txt",
    "089-汤方本草.txt",
]

# 本草类（药材对应功效/病症）
MATERIA_MEDICA_BOOKS = [
    "000-神农本草经.txt",
    "013-本草纲目.txt",
    "018-本草备要.txt",
    "021-本草从新.txt",
    "025-本草求真.txt",
    "042-雷公炮制药性解.txt",
    "036-得配本草.txt",
    "038-本草分经.txt",
    "044-要药分剂.txt",
]


def read_book(filename: str) -> str:
    """读取古籍文件，自动处理编码"""
    path = os.path.join(BOOKS_DIR, filename)
    if not os.path.exists(path):
        return ""
    with open(path, "rb") as f:
        raw = f.read()
    # 尝试多种编码
    for enc in ["gb2312", "gbk", "gb18030", "utf-8", "big5"]:
        try:
            return raw.decode(enc)
        except (UnicodeDecodeError, LookupError):
            continue
    return raw.decode("gb2312", errors="ignore")


# ── 药材名称库（从价格数据库获取） ────────────────────────

def get_variety_names() -> list[str]:
    """获取价格库中所有药材名称"""
    conn = get_connection()
    rows = conn.execute(
        "SELECT DISTINCT name FROM varieties ORDER BY LENGTH(name) DESC"
    ).fetchall()
    conn.close()
    return [r["name"] for r in rows]


def get_latest_prices() -> dict[str, dict]:
    """获取所有品种的最新价格和单位"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT v.name, v.current_price, v.measure_unit,
               (SELECT price FROM estimated_daily_prices e
                WHERE e.name = v.name ORDER BY e.date DESC LIMIT 1) as est_price
        FROM varieties v
        GROUP BY v.name
    """).fetchall()
    conn.close()
    result = {}
    for r in rows:
        price = r["current_price"] or r["est_price"]
        if price and price > 0:
            result[r["name"]] = {
                "price": price,
                "unit": r["measure_unit"] or "元/千克",
            }
    return result


# ── 药材名称别名映射 ──────────────────────────────────────

# 古籍中的药材名 → 现代通用名（价格库中的名字）
HERB_ALIASES = {
    # 常见别名
    "人参": "党参",  # 古方中的人参，现代多用党参替代（野山参价格太高）
    "生地": "地黄", "生地黄": "地黄", "熟地": "地黄", "熟地黄": "地黄",
    "干地黄": "地黄",
    "白芍药": "白芍", "芍药": "白芍", "赤芍药": "赤芍",
    "甘草": "甘草", "炙甘草": "甘草", "生甘草": "甘草", "粉甘草": "甘草",
    "黄耆": "黄芪", "黄蓍": "黄芪", "生黄芪": "黄芪", "炙黄芪": "黄芪",
    "白术": "白术", "苍术": "苍术", "生白术": "白术", "炒白术": "白术",
    "茯苓": "茯苓", "白茯苓": "茯苓", "赤茯苓": "茯苓", "茯神": "茯神",
    "半夏": "半夏", "法半夏": "半夏", "姜半夏": "半夏", "制半夏": "半夏",
    "陈皮": "陈皮", "橘皮": "陈皮", "广陈皮": "陈皮",
    "当归": "当归", "归身": "当归", "归尾": "当归", "全当归": "当归",
    "川芎": "川芎", "芎穷": "川芎", "穹": "川芎",
    "柴胡": "柴胡", "北柴胡": "柴胡", "南柴胡": "柴胡",
    "大黄": "大黄", "生大黄": "大黄", "酒大黄": "大黄",
    "附子": "附子", "制附子": "附子", "炮附子": "附子", "黑附子": "附子",
    "桂枝": "桂枝", "肉桂": "肉桂", "桂心": "肉桂", "官桂": "肉桂",
    "生姜": "干姜", "干姜": "干姜", "炮姜": "干姜",
    "杏仁": "苦杏仁", "苦杏仁": "苦杏仁", "甜杏仁": "甜杏仁",
    "枳壳": "枳壳", "枳实": "枳实",
    "桔梗": "桔梗",
    "麦冬": "麦冬", "麦门冬": "麦冬",
    "天冬": "天冬", "天门冬": "天冬",
    "远志": "远志",
    "防风": "防风",
    "荆芥": "荆芥", "荆芥穗": "荆芥穗",
    "薄荷": "薄荷",
    "连翘": "连翘",
    "金银花": "金银花", "银花": "金银花", "忍冬花": "金银花",
    "黄连": "黄连", "川连": "黄连",
    "黄芩": "黄芩", "枯芩": "黄芩", "条芩": "黄芩",
    "黄柏": "黄柏", "川柏": "黄柏",
    "栀子": "栀子", "山栀": "栀子", "山栀子": "栀子",
    "石膏": "石膏", "生石膏": "石膏",
    "知母": "知母",
    "葛根": "葛根", "粉葛": "粉葛",
    "升麻": "升麻",
    "羌活": "羌活",
    "独活": "独活",
    "秦艽": "秦艽",
    "威灵仙": "威灵仙",
    "牛膝": "牛膝", "川牛膝": "川牛膝", "怀牛膝": "牛膝",
    "木香": "木香", "广木香": "木香",
    "砂仁": "砂仁",
    "豆蔻": "白豆蔻", "白蔻": "白豆蔻",
    "草果": "草果",
    "丁香": "公丁香",
    "山药": "山药", "淮山药": "山药", "怀山药": "山药",
    "山茱萸": "山茱萸", "萸肉": "山茱萸",
    "枸杞": "枸杞子", "枸杞子": "枸杞子",
    "菊花": "菊花", "杭菊花": "菊花", "野菊花": "野菊花",
    "天麻": "天麻",
    "钩藤": "钩藤",
    "石决明": "石决明",
    "牡蛎": "牡蛎", "牡蛎壳": "牡蛎",
    "龙骨": "龙骨",
    "酸枣仁": "酸枣仁", "枣仁": "酸枣仁",
    "五味子": "五味子",
    "桃仁": "桃仁",
    "红花": "红花",
    "丹参": "丹参",
    "牡丹皮": "牡丹皮", "丹皮": "牡丹皮",
    "赤芍": "赤芍",
    "益母草": "益母草",
    "三七": "三七", "田七": "三七",
    "鳖甲": "鳖甲",
    "阿胶": "阿胶",
    "龟甲": "龟甲", "龟板": "龟甲",
    "鹿茸": "鹿茸",
    "熟地黄": "地黄",
    "紫菀": "紫菀",
    "款冬花": "款冬花", "款冬": "款冬花",
    "百合": "百合",
    "百部": "百部",
    "贝母": "浙贝母", "川贝": "川贝母", "浙贝": "浙贝母",
    "川贝母": "川贝母", "浙贝母": "浙贝母",
    "玄参": "玄参", "元参": "玄参",
    "地骨皮": "地骨皮",
    "青蒿": "青蒿",
    "马兜铃": "马兜铃",
    "苍耳子": "苍耳子", "苍耳": "苍耳子",
    "辛夷": "辛夷", "辛夷花": "辛夷",
    "白芷": "白芷",
    "细辛": "细辛",
    "苍朮": "苍术",
    "茵陈": "茵陈", "茵陈蒿": "茵陈",
    "大枣": "大枣", "红枣": "大枣",
    "饴糖": "大枣",  # 无直接对应，跳过
    "牛蒡子": "牛蒡子", "鼠粘子": "牛蒡子", "牛蒡": "牛蒡子",
    "蝉蜕": "蝉蜕", "蝉衣": "蝉蜕",
    "僵蚕": "僵蚕", "白僵蚕": "僵蚕",
    "全蝎": "全蝎", "蝎子": "全蝎",
    "蜈蚣": "蜈蚣",
    "地龙": "地龙",
    "水蛭": "水蛭",
    "厚朴": "厚朴",
    "苏子": "紫苏子", "紫苏子": "紫苏子", "紫苏叶": "紫苏叶", "苏叶": "紫苏叶",
    "前胡": "前胡",
    "射干": "射干",
    "山楂": "山楂", "山查": "山楂",
    "神曲": "建曲", "六神曲": "建曲",
    "麦芽": "麦芽",
    "鸡内金": "鸡内金",
    "莱菔子": "莱菔子", "萝卜子": "莱菔子",
    "槟榔": "槟榔", "槟郎": "槟榔", "大腹皮": "大腹皮",
    "使君子": "使君子",
    "苦楝皮": "苦楝皮",
    "雷丸": "雷丸",
    "仙灵脾": "仙灵脾", "淫羊藿": "淫羊藿",
    "巴戟天": "巴戟天", "巴戟": "巴戟天",
    "肉苁蓉": "肉苁蓉", "苁蓉": "肉苁蓉",
    "补骨脂": "补骨脂", "破故纸": "补骨脂",
    "杜仲": "杜仲",
    "续断": "续断",
    "龟板胶": "龟甲胶",
    "鹿角胶": "鹿角胶",
    "沉香": "沉香",
    "檀香": "檀香",
    "木通": "木通", "川木通": "川木通",
    "通草": "通草",
    "车前子": "车前子",
    "泽泻": "泽泻",
    "滑石": "滑石粉",
    "猪苓": "猪苓",
    "瞿麦": "瞿麦",
    "萹蓄": "萹蓄",
    "海金沙": "海金沙",
    "石韦": "石韦",
    "茅根": "白茅根", "白茅根": "白茅根",
    "小蓟": "小蓟",
    "大蓟": "大蓟",
    "侧柏叶": "侧柏叶",
    "蒲黄": "蒲黄",
    "艾叶": "艾叶",
}


def build_herb_matcher(variety_names: list[str]) -> tuple[dict, re.Pattern]:
    """构建药材名称匹配器

    返回：
        alias_map: 别名→标准名映射
        pattern: 正则表达式（按长度降序匹配，避免短名匹配到长名的子串）
    """
    # 合并别名映射（先用自定义别名，再加价格库中的原名）
    alias_map = dict(HERB_ALIASES)
    for name in variety_names:
        if name not in alias_map:
            alias_map[name] = name

    # 构建正则：按长度降序排列（长的先匹配）
    all_names = sorted(set(list(alias_map.keys()) + variety_names),
                       key=len, reverse=True)
    # 只取 2 字以上的名称（避免单字误匹配）
    all_names = [n for n in all_names if len(n) >= 2]
    pattern = re.compile("|".join(re.escape(n) for n in all_names))

    return alias_map, pattern


# ── 用量解析 ──────────────────────────────────────────────

# 古代计量单位换算到克
UNIT_TO_GRAMS = {
    "两": 30.0,      # 1 两 ≈ 30g（明清标准，宋元约 37.5g）
    "钱": 3.0,       # 1 钱 ≈ 3g
    "分": 0.3,       # 1 分 ≈ 0.3g
    "厘": 0.03,
    "斤": 500.0,
    "升": 200.0,     # 液体，约 200ml≈200g
    "合": 20.0,
    "铢": 0.65,      # 汉代 1 铢 ≈ 0.65g
    "克": 1.0,
    "g": 1.0,
    "枚": 3.0,       # 大枣等按枚计
    "个": 5.0,       # 通用
    "条": 3.0,       # 蜈蚣等按条计
    "只": 5.0,
    "片": 1.0,
    "粒": 0.5,
}

# 中文数字转换
CN_NUMS = {
    "一": 1, "二": 2, "三": 3, "四": 4, "五": 5,
    "六": 6, "七": 7, "八": 8, "九": 9, "十": 10,
    "半": 0.5, "零": 0,
    "壹": 1, "贰": 2, "叁": 3, "肆": 4, "伍": 5,
    "陆": 6, "柒": 7, "捌": 8, "玖": 9, "拾": 10,
    "百": 100, "千": 1000,
}


def parse_cn_number(s: str) -> float:
    """解析中文数字字符串"""
    s = s.strip()
    if not s:
        return 0.0
    # 纯阿拉伯数字
    try:
        return float(s)
    except ValueError:
        pass
    # 中文数字
    total = 0.0
    current = 0.0
    for ch in s:
        if ch in CN_NUMS:
            val = CN_NUMS[ch]
            if val >= 10:  # 十、百、千是乘数
                if current == 0:
                    current = 1
                total += current * val
                current = 0
            else:
                current = val
        elif ch == "十":
            if current == 0:
                current = 1
            total += current * 10
            current = 0
    total += current
    return total if total > 0 else 1.0


def parse_dosage(text: str) -> float:
    """从文本中解析用量（克）

    支持格式：三钱、二两、1.5钱、钱半、两半 等
    """
    # 匹配 "数字+单位" 模式
    patterns = [
        # "三钱" "二两" "五分"
        r'([一二三四五六七八九十百千零半壹贰叁肆伍陆柒捌玖拾\d\.]+)\s*(两|钱|分|厘|斤|升|合|铢|克|g|枚|个|条|只|片|粒)',
        # "钱半" "两半"
        r'(两|钱|分)(半)',
    ]

    for p in patterns:
        m = re.search(p, text)
        if m:
            groups = m.groups()
            if len(groups) == 2 and groups[1] == "半":
                # "钱半" = 1.5 钱
                unit = groups[0]
                return 1.5 * UNIT_TO_GRAMS.get(unit, 3.0)
            num_str, unit = groups[0], groups[1]
            num = parse_cn_number(num_str)
            grams = num * UNIT_TO_GRAMS.get(unit, 3.0)
            return grams

    return 0.0


# ── 处方提取引擎 ──────────────────────────────────────────

def extract_formulas_from_tangtou(text: str, alias_map: dict,
                                  pattern: re.Pattern) -> list[dict]:
    """从汤头歌诀/医方集解等方书中提取处方

    汤头歌诀结构：
        <目录>分类
        <篇名>方名
        属性：方剂描述（含药材、用量、主治）
    """
    formulas = []

    # 按篇名分段
    sections = re.split(r'<篇名>', text)
    current_category = ""

    for section in sections[1:]:  # 跳过第一段（序言）
        lines = section.strip().split("\n")
        if not lines:
            continue

        name = lines[0].strip()
        # 跳过目录、序言等非方剂内容
        if name in ("叙", "序", "凡例", "目录", "") or len(name) > 20:
            continue

        # 检查是否有分类信息
        for line in lines:
            if "<目录>" in line:
                cat_match = re.search(r'<目录>\s*(.+)', line)
                if cat_match:
                    cat = cat_match.group(1).strip()
                    if cat and len(cat) < 20:
                        current_category = cat

        # 合并属性内容
        content = "\n".join(lines[1:])
        attr_match = re.search(r'属性：(.+?)(?=\n<|\Z)', content, re.DOTALL)
        if not attr_match:
            continue
        attr_text = attr_match.group(1).strip()

        # 提取药材及用量
        herbs = extract_herbs_from_text(attr_text, alias_map, pattern)
        if not herbs:
            continue

        # 提取主治病症
        symptoms = extract_symptoms(attr_text)

        formulas.append({
            "name": name,
            "category": current_category,
            "herbs": herbs,
            "symptoms": symptoms,
            "source": "汤头歌诀",
            "raw_text": attr_text[:500],
        })

    return formulas


def extract_herbs_from_text(text: str, alias_map: dict,
                            pattern: re.Pattern) -> list[dict]:
    """从文本中提取药材名及用量"""
    herbs = []
    seen = set()

    matches = list(pattern.finditer(text))
    for match in matches:
        herb_name = match.group()
        std_name = alias_map.get(herb_name, herb_name)

        if std_name in seen:
            continue
        seen.add(std_name)

        # 在匹配位置附近找用量
        start = match.end()
        nearby = text[start:start + 30]
        dosage_g = parse_dosage(nearby)

        # 如果没找到用量，尝试往前找
        if dosage_g == 0:
            before = text[max(0, match.start() - 20):match.start()]
            dosage_g = parse_dosage(before)

        # 默认用量 10g（常见中药处方单味用量）
        if dosage_g == 0:
            dosage_g = 10.0

        herbs.append({
            "name": std_name,
            "original_name": herb_name,
            "dosage_g": dosage_g,
        })

    return herbs


# ── 病症提取 ──────────────────────────────────────────────

# 常见中医病症关键词
SYMPTOM_KEYWORDS = [
    # 外感病
    "感冒", "伤寒", "风寒", "风热", "暑热", "中暑", "疟疾",
    # 咳嗽/肺系
    "咳嗽", "哮喘", "气喘", "痰多", "咳血", "咯血", "肺痈", "肺痿",
    "痰饮", "胸闷",
    # 脾胃
    "腹痛", "腹泻", "泄泻", "呕吐", "恶心", "腹胀", "食欲不振",
    "消化不良", "胃痛", "胃脘痛", "噎膈", "反胃", "便秘", "痢疾",
    "脘腹胀满", "不思饮食",
    # 心系
    "心悸", "怔忡", "失眠", "不寐", "健忘", "心痛", "胸痹",
    # 肝系
    "头痛", "眩晕", "头晕", "目眩", "中风", "口眼歪斜",
    "肝阳上亢", "肝气郁结", "胁痛",
    # 肾系
    "腰痛", "遗精", "阳痿", "早泄", "尿频", "遗尿", "水肿",
    "淋证", "癃闭", "耳鸣", "耳聋",
    # 血证
    "吐血", "衄血", "便血", "尿血", "崩漏", "血瘀", "血虚",
    # 妇科
    "月经不调", "痛经", "闭经", "带下", "产后", "妊娠",
    # 皮肤
    "疮疡", "痈疽", "疔疮", "湿疹", "风疹", "瘙痒",
    # 筋骨
    "风湿", "痹证", "关节痛", "腰膝酸软", "骨蒸",
    # 五官
    "目赤", "目翳", "咽喉肿痛", "牙痛", "口疮", "口臭",
    # 虚证
    "气虚", "血虚", "阴虚", "阳虚", "虚劳", "盗汗", "自汗",
    "乏力", "倦怠",
    # 热证
    "发热", "高热", "潮热", "低热", "烦躁", "口渴",
    # 其他
    "黄疸", "积聚", "癥瘕", "痰核", "瘿瘤", "虫积",
    "疝气", "脱肛", "痔疮", "跌打损伤", "外伤", "烫伤",
    "小便不利", "大便不通", "消渴",
]

SYMPTOM_PATTERN = re.compile("|".join(re.escape(s) for s in
    sorted(SYMPTOM_KEYWORDS, key=len, reverse=True)))


def extract_symptoms(text: str) -> list[str]:
    """从文本中提取病症关键词"""
    found = set()
    # 先找 "治" "主" 后面的内容
    treat_sections = re.findall(r'[治主]([^。，]{5,80})', text)
    search_text = " ".join(treat_sections) if treat_sections else text

    for m in SYMPTOM_PATTERN.finditer(search_text):
        found.add(m.group())

    # 如果在治法段没找到，扩大到全文搜索
    if not found:
        for m in SYMPTOM_PATTERN.finditer(text):
            found.add(m.group())

    return sorted(found)


# ── 通用古籍处方提取 ──────────────────────────────────────

def extract_formulas_generic(text: str, alias_map: dict,
                             pattern: re.Pattern,
                             source: str) -> list[dict]:
    """通用处方提取（适用于大多数方书）"""
    formulas = []
    sections = re.split(r'<篇名>', text)

    for section in sections[1:]:
        lines = section.strip().split("\n")
        if not lines:
            continue

        name = lines[0].strip()
        if not name or len(name) > 30 or name in ("叙", "序", "凡例"):
            continue

        content = "\n".join(lines[1:])
        attr_match = re.search(r'属性：(.+?)(?=\n<|\Z)', content, re.DOTALL)
        if not attr_match:
            continue

        attr_text = attr_match.group(1).strip()
        if len(attr_text) < 20:
            continue

        herbs = extract_herbs_from_text(attr_text, alias_map, pattern)
        if len(herbs) < 2:  # 至少 2 味药才算处方
            continue

        symptoms = extract_symptoms(attr_text)

        formulas.append({
            "name": name,
            "category": "",
            "herbs": herbs,
            "symptoms": symptoms,
            "source": source,
            "raw_text": attr_text[:500],
        })

    return formulas


# ── 数据库表 ──────────────────────────────────────────────

def init_tcm_tables():
    """初始化中医分析相关表"""
    conn = get_connection()
    conn.executescript("""
        -- 处方表
        CREATE TABLE IF NOT EXISTS tcm_formulas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            category TEXT DEFAULT '',
            source TEXT NOT NULL,
            raw_text TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(name, source)
        );

        -- 处方药材明细表
        CREATE TABLE IF NOT EXISTS tcm_formula_herbs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            formula_id INTEGER NOT NULL,
            herb_name TEXT NOT NULL,
            original_name TEXT,
            dosage_g REAL DEFAULT 10.0,
            FOREIGN KEY (formula_id) REFERENCES tcm_formulas(id)
        );

        -- 处方-病症关联表
        CREATE TABLE IF NOT EXISTS tcm_formula_symptoms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            formula_id INTEGER NOT NULL,
            symptom TEXT NOT NULL,
            FOREIGN KEY (formula_id) REFERENCES tcm_formulas(id),
            UNIQUE(formula_id, symptom)
        );

        -- 药材名称映射表（古名 → 现代名）
        CREATE TABLE IF NOT EXISTS tcm_herb_mapping (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ancient_name TEXT NOT NULL,
            modern_name TEXT NOT NULL,
            has_price INTEGER DEFAULT 0,
            UNIQUE(ancient_name)
        );

        -- 病症治疗成本汇总视图
        CREATE VIEW IF NOT EXISTS v_symptom_cost AS
        SELECT
            fs.symptom,
            COUNT(DISTINCT f.id) as formula_count,
            GROUP_CONCAT(DISTINCT f.name) as formula_names
        FROM tcm_formula_symptoms fs
        JOIN tcm_formulas f ON f.id = fs.formula_id
        GROUP BY fs.symptom
        ORDER BY formula_count DESC;

        CREATE INDEX IF NOT EXISTS idx_formula_herbs_name
            ON tcm_formula_herbs(herb_name);
        CREATE INDEX IF NOT EXISTS idx_formula_symptoms_symptom
            ON tcm_formula_symptoms(symptom);
    """)
    conn.commit()
    conn.close()
    log.info("TCM 分析表初始化完成")


# ── 主流程 ────────────────────────────────────────────────

def run_extraction():
    """执行完整的处方提取流程"""
    init_tcm_tables()

    variety_names = get_variety_names()
    log.info("价格库共 %d 个品种", len(variety_names))

    alias_map, pattern = build_herb_matcher(variety_names)
    log.info("药材匹配器就绪，共 %d 个名称", len(alias_map))

    all_formulas = []

    # 1. 从汤头歌诀提取（结构最好）
    log.info("正在提取 汤头歌诀...")
    text = read_book("084-汤头歌诀.txt")
    if text:
        formulas = extract_formulas_from_tangtou(text, alias_map, pattern)
        all_formulas.extend(formulas)
        log.info("  汤头歌诀: %d 个处方", len(formulas))

    # 2. 从其他方书提取
    for book_file in FORMULA_BOOKS:
        if book_file == "084-汤头歌诀.txt":
            continue
        book_name = book_file.split("-", 1)[1].replace(".txt", "")
        log.info("正在提取 %s...", book_name)
        text = read_book(book_file)
        if text:
            formulas = extract_formulas_generic(text, alias_map, pattern, book_name)
            all_formulas.extend(formulas)
            log.info("  %s: %d 个处方", book_name, len(formulas))

    log.info("总计提取 %d 个处方", len(all_formulas))

    # 3. 入库
    save_formulas(all_formulas)

    # 4. 统计
    show_stats()

    return all_formulas


def save_formulas(formulas: list[dict]):
    """保存提取的处方到数据库"""
    conn = get_connection()

    # 清除旧数据（按外键依赖顺序删除）
    conn.execute("DELETE FROM tcm_formula_symptoms")
    conn.execute("DELETE FROM tcm_formula_herbs")
    conn.execute("DELETE FROM tcm_formulas")
    conn.execute("DELETE FROM tcm_herb_mapping")
    conn.commit()

    herb_mapping = {}  # 跟踪所有发现的药材名

    for f in formulas:
        # 先查是否已存在
        existing = conn.execute(
            "SELECT id FROM tcm_formulas WHERE name=? AND source=?",
            (f["name"], f["source"])
        ).fetchone()
        if existing:
            formula_id = existing["id"]
        else:
            cursor = conn.execute(
                """INSERT INTO tcm_formulas (name, category, source, raw_text)
                   VALUES (?, ?, ?, ?)""",
                (f["name"], f["category"], f["source"], f["raw_text"])
            )
            formula_id = cursor.lastrowid
            if not formula_id:
                continue

        for herb in f["herbs"]:
            conn.execute(
                """INSERT INTO tcm_formula_herbs
                   (formula_id, herb_name, original_name, dosage_g)
                   VALUES (?, ?, ?, ?)""",
                (formula_id, herb["name"], herb["original_name"], herb["dosage_g"])
            )
            herb_mapping[herb["original_name"]] = herb["name"]

        for symptom in f["symptoms"]:
            conn.execute(
                """INSERT OR IGNORE INTO tcm_formula_symptoms
                   (formula_id, symptom) VALUES (?, ?)""",
                (formula_id, symptom)
            )

    # 保存药材名称映射
    prices = get_latest_prices()
    for ancient, modern in herb_mapping.items():
        has_price = 1 if modern in prices else 0
        conn.execute(
            """INSERT OR IGNORE INTO tcm_herb_mapping
               (ancient_name, modern_name, has_price) VALUES (?, ?, ?)""",
            (ancient, modern, has_price)
        )

    conn.commit()
    conn.close()
    log.info("处方数据已保存到数据库")


def calculate_formula_cost(formula_id: int = None,
                           formula_name: str = None,
                           conn: sqlite3.Connection = None,
                           prices: dict = None) -> dict | None:
    """计算单个处方的成本

    Args:
        formula_id: 处方 ID
        formula_name: 处方名称
        conn: 可选，外部传入的数据库连接（批量计算时复用）
        prices: 可选，外部传入的价格字典（批量计算时复用）

    Returns:
        {name, herbs, total_cost_single, total_cost_course, ...}
    """
    should_close = conn is None
    if conn is None:
        conn = get_connection()
    if prices is None:
        prices = get_latest_prices()

    if formula_name:
        row = conn.execute(
            "SELECT * FROM tcm_formulas WHERE name = ? LIMIT 1",
            (formula_name,)
        ).fetchone()
    elif formula_id:
        row = conn.execute(
            "SELECT * FROM tcm_formulas WHERE id = ?",
            (formula_id,)
        ).fetchone()
    else:
        if should_close:
            conn.close()
        return None

    if not row:
        if should_close:
            conn.close()
        return None

    herbs = conn.execute(
        "SELECT * FROM tcm_formula_herbs WHERE formula_id = ?",
        (row["id"],)
    ).fetchall()

    symptoms = conn.execute(
        "SELECT symptom FROM tcm_formula_symptoms WHERE formula_id = ?",
        (row["id"],)
    ).fetchall()

    if should_close:
        conn.close()

    herb_costs = []
    total_cost = 0.0
    matched_count = 0

    for h in herbs:
        price_info = prices.get(h["herb_name"])
        if price_info:
            # 价格是元/千克，用量是克 → 单剂成本 = dosage_g / 1000 * price
            cost = h["dosage_g"] / 1000.0 * price_info["price"]
            herb_costs.append({
                "name": h["herb_name"],
                "original_name": h["original_name"],
                "dosage_g": h["dosage_g"],
                "price_per_kg": price_info["price"],
                "unit": price_info["unit"],
                "cost": round(cost, 2),
                "has_price": True,
            })
            total_cost += cost
            matched_count += 1
        else:
            herb_costs.append({
                "name": h["herb_name"],
                "original_name": h["original_name"],
                "dosage_g": h["dosage_g"],
                "price_per_kg": None,
                "unit": None,
                "cost": 0,
                "has_price": False,
            })

    return {
        "name": row["name"],
        "category": row["category"],
        "source": row["source"],
        "herbs": herb_costs,
        "symptoms": [s["symptom"] for s in symptoms],
        "total_cost_single": round(total_cost, 2),        # 单剂费用
        "total_cost_course": round(total_cost * 7, 2),     # 7天疗程
        "total_cost_month": round(total_cost * 30, 2),     # 月费用
        "herb_count": len(herbs),
        "matched_count": matched_count,
        "match_rate": round(matched_count / len(herbs) * 100, 1) if herbs else 0,
    }


def calculate_symptom_costs() -> list[dict]:
    """计算所有病症的治疗成本

    返回按病症聚合的成本数据
    """
    conn = get_connection()
    prices = get_latest_prices()

    symptoms = conn.execute("""
        SELECT DISTINCT fs.symptom,
               COUNT(DISTINCT f.id) as formula_count
        FROM tcm_formula_symptoms fs
        JOIN tcm_formulas f ON f.id = fs.formula_id
        GROUP BY fs.symptom
        HAVING formula_count >= 1
        ORDER BY formula_count DESC
    """).fetchall()

    results = []
    for s in symptoms:
        symptom = s["symptom"]

        # 获取该病症的所有处方
        formula_rows = conn.execute("""
            SELECT DISTINCT f.id, f.name, f.source
            FROM tcm_formulas f
            JOIN tcm_formula_symptoms fs ON fs.formula_id = f.id
            WHERE fs.symptom = ?
        """, (symptom,)).fetchall()

        formula_costs = []
        for fr in formula_rows:
            cost = calculate_formula_cost(formula_id=fr["id"])
            if cost and cost["total_cost_single"] > 0:
                formula_costs.append(cost)

        if not formula_costs:
            continue

        costs = [fc["total_cost_single"] for fc in formula_costs]
        avg_cost = sum(costs) / len(costs)
        min_cost = min(costs)
        max_cost = max(costs)

        results.append({
            "symptom": symptom,
            "formula_count": len(formula_costs),
            "avg_cost_single": round(avg_cost, 2),
            "min_cost_single": round(min_cost, 2),
            "max_cost_single": round(max_cost, 2),
            "avg_cost_course": round(avg_cost * 7, 2),
            "min_cost_course": round(min_cost * 7, 2),
            "max_cost_course": round(max_cost * 7, 2),
            "formulas": [{
                "name": fc["name"],
                "source": fc["source"],
                "cost_single": fc["total_cost_single"],
                "herb_count": fc["herb_count"],
                "match_rate": fc["match_rate"],
            } for fc in formula_costs],
        })

    conn.close()
    results.sort(key=lambda x: x["formula_count"], reverse=True)
    return results


def show_stats():
    """显示提取统计"""
    conn = get_connection()

    formula_count = conn.execute(
        "SELECT COUNT(*) as cnt FROM tcm_formulas"
    ).fetchone()["cnt"]

    herb_count = conn.execute(
        "SELECT COUNT(DISTINCT herb_name) FROM tcm_formula_herbs"
    ).fetchone()[0]

    symptom_count = conn.execute(
        "SELECT COUNT(DISTINCT symptom) FROM tcm_formula_symptoms"
    ).fetchone()[0]

    mapped = conn.execute(
        "SELECT COUNT(*) FROM tcm_herb_mapping WHERE has_price = 1"
    ).fetchone()[0]
    total_mapped = conn.execute(
        "SELECT COUNT(*) FROM tcm_herb_mapping"
    ).fetchone()[0]

    by_source = conn.execute("""
        SELECT source, COUNT(*) as cnt
        FROM tcm_formulas GROUP BY source ORDER BY cnt DESC
    """).fetchall()

    top_symptoms = conn.execute("""
        SELECT symptom, COUNT(*) as cnt
        FROM tcm_formula_symptoms
        GROUP BY symptom ORDER BY cnt DESC LIMIT 20
    """).fetchall()

    conn.close()

    print(f"\n{'='*60}")
    print(f"  中医处方提取统计")
    print(f"{'='*60}")
    print(f"  处方总数:       {formula_count}")
    print(f"  涉及药材:       {herb_count} 种")
    print(f"  关联病症:       {symptom_count} 种")
    print(f"  有价格药材:     {mapped}/{total_mapped} "
          f"({mapped/total_mapped*100:.1f}%)" if total_mapped else "")
    print(f"\n  按来源统计:")
    for r in by_source:
        print(f"    {r['source']:<20} {r['cnt']:>5} 个处方")
    print(f"\n  高频病症 Top 20:")
    for r in top_symptoms:
        print(f"    {r['symptom']:<12} 出现在 {r['cnt']:>4} 个处方中")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("用法:")
        print("  python tcm_analyzer.py extract    - 提取古籍处方数据")
        print("  python tcm_analyzer.py stats      - 显示统计信息")
        print("  python tcm_analyzer.py cost <方名> - 计算处方成本")
        print("  python tcm_analyzer.py symptoms   - 计算所有病症成本")
        sys.exit(0)

    cmd = sys.argv[1]

    if cmd == "extract":
        run_extraction()
    elif cmd == "stats":
        show_stats()
    elif cmd == "cost":
        if len(sys.argv) < 3:
            print("请指定方名，如: python tcm_analyzer.py cost 四君子汤")
            sys.exit(1)
        name = sys.argv[2]
        cost = calculate_formula_cost(formula_name=name)
        if cost:
            print(f"\n处方: {cost['name']} ({cost['source']})")
            print(f"病症: {', '.join(cost['symptoms'])}")
            print(f"药材 {cost['herb_count']} 味，价格匹配率 {cost['match_rate']}%")
            print(f"\n{'药材':<10} {'用量(g)':>8} {'单价(元/kg)':>12} {'单剂成本':>10}")
            print("-" * 45)
            for h in cost["herbs"]:
                price_str = f"{h['price_per_kg']:.0f}" if h["has_price"] else "无价格"
                cost_str = f"¥{h['cost']:.2f}" if h["has_price"] else "-"
                print(f"{h['name']:<10} {h['dosage_g']:>8.1f} {price_str:>12} {cost_str:>10}")
            print("-" * 45)
            print(f"{'单剂总费用':>32} ¥{cost['total_cost_single']:.2f}")
            print(f"{'7天疗程':>32} ¥{cost['total_cost_course']:.2f}")
            print(f"{'30天费用':>32} ¥{cost['total_cost_month']:.2f}")
        else:
            print(f"未找到处方: {name}")
    elif cmd == "symptoms":
        results = calculate_symptom_costs()
        print(f"\n{'病症':<14} {'处方数':>6} {'单剂均价':>10} {'单剂最低':>10} "
              f"{'单剂最高':>10} {'7天疗程均价':>12}")
        print("-" * 70)
        for r in results[:50]:
            print(f"{r['symptom']:<14} {r['formula_count']:>6} "
                  f"¥{r['avg_cost_single']:>8.2f} ¥{r['min_cost_single']:>8.2f} "
                  f"¥{r['max_cost_single']:>8.2f} ¥{r['avg_cost_course']:>10.2f}")
    else:
        print(f"未知命令: {cmd}")

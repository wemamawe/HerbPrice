"""药材价格多因子预测增强模块

在现有时序模型（Prophet + Ridge + EMA）的基础上，引入供需基本面因子：
1. 种植周期因子：不同药材的生长年限影响未来供给释放时间
2. 产区气象/灾害因子：干旱、洪涝、病虫害影响当年产量
3. 种植面积变动因子：扩种/缩种信号预示未来产量变化
4. 产新季节因子：产新期前后价格规律性波动
5. 库存周期因子：基于历史价格推断库存水平

使用方式：
    from forecast_factors import get_price_adjustment
    adj = get_price_adjustment("白术")
    # adj = {"factor": 1.05, "reason": "...", "confidence": 0.7}
    # 将 factor 乘以时序模型预测值，得到修正后的预测
"""

from datetime import datetime, date
from db import get_connection

# ═══════════════════════════════════════════════════════════════════════
# 药材种植周期数据（年）
# 生长周期越长，供给响应越滞后，价格波动周期越长
# ═══════════════════════════════════════════════════════════════════════

GROWTH_CYCLE = {
    # 1年生
    "板蓝根": 1, "荆芥": 1, "薄荷": 1, "干姜": 1, "红花": 1,
    "决明子": 1, "紫苏叶": 1, "大青叶": 1, "白芷": 1, "防风": 1,
    "菊花": 1, "金银花": 1, "半夏": 1, "麦冬": 1,

    # 2年生
    "白术": 2, "当归": 2, "党参": 2, "黄芩": 2, "柴胡": 2,
    "丹参": 2, "地黄": 2, "桔梗": 2, "知母": 2, "玄参": 2,
    "前胡": 2, "太子参": 2, "川芎": 2, "泽泻": 2, "牛膝": 2,

    # 3年生
    "黄芪": 3, "甘草": 3, "远志": 3, "独活": 3, "羌活": 3,
    "枸杞子": 3, "山茱萸": 3, "茯苓": 3, "天麻": 3, "三七": 3,
    "栀子": 3, "连翘": 3, "五味子": 3, "吴茱萸": 3,

    # 4-5年生
    "白芍": 4, "牡丹皮": 4, "黄连": 5, "厚朴": 5, "杜仲": 5,
    "人参": 5, "西洋参": 4, "黄柏": 8, "肉桂": 10,

    # 多年生/木本
    "山药": 1, "麻黄": 3, "秦艽": 4, "川贝母": 5,
    "八角茴香": 8, "附子": 1, "大枣": 3,
}

# ═══════════════════════════════════════════════════════════════════════
# 产新时间（月份）
# 产新前1-2月通常价格上升（青黄不接），产新后价格下行（新货集中上市）
# ═══════════════════════════════════════════════════════════════════════

HARVEST_MONTHS = {
    # 春季产新 (3-5月)
    "太子参": [5, 6], "浙贝母": [5], "延胡索": [5],
    "半夏": [5, 6], "川芎": [5], "泽泻": [4, 5],

    # 夏季产新 (6-8月)
    "金银花": [5, 6], "菊花": [10, 11], "红花": [6, 7],
    "薄荷": [7, 8], "荆芥": [7, 8], "夏枯草": [6],

    # 秋季产新 (9-11月)
    "白术": [10, 11], "白芍": [9, 10], "当归": [10, 11],
    "黄芪": [10, 11], "党参": [10, 11], "丹参": [10, 11],
    "白芷": [8, 9], "防风": [10, 11], "桔梗": [9, 10],
    "板蓝根": [10, 11], "地黄": [10, 11], "山药": [11, 12],
    "山茱萸": [9, 10], "枸杞子": [6, 7, 8], "五味子": [9, 10],
    "连翘": [8, 9], "玄参": [10, 11], "牛膝": [11, 12],
    "知母": [10, 11], "黄芩": [10, 11], "远志": [9, 10],
    "大黄": [10, 11], "独活": [9, 10], "羌活": [9, 10],

    # 冬季产新 (12-2月)
    "麦冬": [3, 4], "天麻": [11, 12], "茯苓": [12, 1],
    "三七": [10, 11, 12], "人参": [9, 10], "干姜": [11, 12],
    "附子": [6, 7],
}

# ═══════════════════════════════════════════════════════════════════════
# 灾害/新闻事件类型及其对价格的影响系数
# ═══════════════════════════════════════════════════════════════════════

EVENT_IMPACT = {
    # 灾害类（减产 → 价格上涨）
    "drought": {"name": "干旱", "factor_range": (1.05, 1.30), "duration_months": 3},
    "flood": {"name": "洪涝", "factor_range": (1.08, 1.40), "duration_months": 2},
    "pest": {"name": "病虫害", "factor_range": (1.03, 1.15), "duration_months": 4},
    "frost": {"name": "霜冻", "factor_range": (1.05, 1.25), "duration_months": 2},
    "hail": {"name": "冰雹", "factor_range": (1.03, 1.20), "duration_months": 1},

    # 种植面积变动（影响延后 = 生长周期）
    "area_increase": {"name": "扩种", "factor_range": (0.85, 0.95), "duration_months": 6},
    "area_decrease": {"name": "缩种", "factor_range": (1.05, 1.15), "duration_months": 6},

    # 政策/需求类
    "policy_positive": {"name": "利好政策", "factor_range": (1.02, 1.10), "duration_months": 6},
    "demand_surge": {"name": "需求激增", "factor_range": (1.05, 1.20), "duration_months": 3},
    "export_ban": {"name": "出口限制", "factor_range": (0.90, 0.97), "duration_months": 4},
}


# ═══════════════════════════════════════════════════════════════════════
# 核心函数
# ═══════════════════════════════════════════════════════════════════════

def get_growth_cycle(herb_name: str) -> int:
    """获取药材的种植周期（年）"""
    return GROWTH_CYCLE.get(herb_name, 2)


def get_harvest_months(herb_name: str) -> list[int]:
    """获取药材的产新月份"""
    return HARVEST_MONTHS.get(herb_name, [10, 11])


def calc_seasonal_factor(herb_name: str, target_date: date | None = None) -> float:
    """计算产新季节因子

    产新前1-2月: 上涨动力 (factor > 1)
    产新期间: 下行压力 (factor < 1)
    非产新期: 中性 (factor ≈ 1)

    Returns: 0.92 ~ 1.08 的调整系数
    """
    if target_date is None:
        target_date = date.today()

    harvest = get_harvest_months(herb_name)
    if not harvest:
        return 1.0

    month = target_date.month

    # 产新月份 → 下行压力
    if month in harvest:
        return 0.96

    # 产新前1个月 → 上涨动力（青黄不接）
    pre_harvest = [(h - 1) if h > 1 else 12 for h in harvest]
    if month in pre_harvest:
        return 1.04

    # 产新前2个月 → 轻微上涨
    pre_harvest_2 = [(h - 2) if h > 2 else (12 + h - 2) for h in harvest]
    if month in pre_harvest_2:
        return 1.02

    # 产新后1个月 → 继续下行
    post_harvest = [(h % 12) + 1 for h in harvest]
    if month in post_harvest:
        return 0.98

    return 1.0


def calc_supply_cycle_factor(herb_name: str) -> float:
    """基于种植周期和历史价格计算供给周期因子

    核心逻辑：
    - 查看 N 年前的价格（N = 种植周期）
    - 如果 N 年前价格高 → 当时扩种 → 现在产量释放 → 价格下行压力
    - 如果 N 年前价格低 → 当时缩种 → 现在供给不足 → 价格上行支撑

    Returns: 0.90 ~ 1.10 的调整系数
    """
    cycle = get_growth_cycle(herb_name)
    conn = get_connection()

    # 获取当前价格和 N 年前价格
    rows = conn.execute("""
        SELECT date, price FROM estimated_daily_prices
        WHERE name = ?
        ORDER BY date DESC LIMIT 1
    """, (herb_name,)).fetchall()

    if not rows:
        conn.close()
        return 1.0

    current_price = rows[0]["price"]
    current_date = rows[0]["date"]

    # N 年前的价格
    target_year = int(current_date[:4]) - cycle
    target_date = f"{target_year}{current_date[4:]}"

    past_row = conn.execute("""
        SELECT price FROM estimated_daily_prices
        WHERE name = ? AND date <= ? ORDER BY date DESC LIMIT 1
    """, (herb_name, target_date)).fetchone()

    conn.close()

    if not past_row:
        return 1.0

    past_price = past_row["price"]

    # 计算 N 年前的价格相对水平
    # 高价 → 扩种 → 现在产量大 → 下行
    # 低价 → 缩种 → 现在供给紧 → 上行
    ratio = past_price / max(current_price, 0.01)

    if ratio > 1.3:
        # N年前比现在贵30%+ → 当时高价刺激扩种 → 现在供给充裕
        return 0.95
    elif ratio > 1.1:
        return 0.98
    elif ratio < 0.7:
        # N年前比现在便宜30%+ → 当时低价导致缩种 → 现在供给偏紧
        return 1.05
    elif ratio < 0.9:
        return 1.02
    else:
        return 1.0


def calc_production_concentration_risk(herb_name: str) -> float:
    """产区集中度风险评估

    产区越集中，受单一灾害影响越大，价格波动风险越高。
    Returns: 1.0 ~ 1.15 的风险溢价系数
    """
    conn = get_connection()
    rows = conn.execute("""
        SELECT output_percent FROM herb_origins
        WHERE herb_name = ? AND annual_output_tons IS NOT NULL AND province != '进口'
        ORDER BY output_percent DESC
    """, (herb_name,)).fetchall()
    conn.close()

    if not rows:
        return 1.0

    # 最大产区占比
    top_pct = rows[0]["output_percent"] or 0
    if top_pct >= 80:
        return 1.08  # 极度集中（如三七90%在文山）
    elif top_pct >= 60:
        return 1.04  # 高度集中
    elif top_pct >= 40:
        return 1.02  # 中等集中
    return 1.0


def get_historical_weather_impact(herb_name: str) -> tuple[float, list[str]]:
    """基于近期事件 + 历史实证数据计算价格影响系数

    改进点：
    1. demand_surge 过滤：severity < 0.5 的日常行情小波动不计入
    2. 时效衰减：越老的事件权重越低（90天半衰期）
    3. 历史实证优先：同品种同类型事件有 ≥2 条历史记录时用均值
    4. area 事件与种植周期联动：延后 = 生长周期年数
    5. 上限收紧到 ±20%（避免单品种事件堆叠过度放大）

    Returns:
        (factor, reasons): 价格调整系数 + 原因列表
    """
    from datetime import timedelta
    import math

    conn = get_connection()
    today = date.today()
    reasons: list[str] = []

    # ── 1. 近6个月活跃事件（扩大窗口，配合时效衰减） ──────────────
    six_months_ago = (today - timedelta(days=180)).isoformat()
    recent_events = conn.execute("""
        SELECT event_type, severity, start_date, price_impact_pct
        FROM weather_events
        WHERE affected_herbs = ? AND start_date >= ?
        ORDER BY start_date DESC
    """, (herb_name, six_months_ago)).fetchall()

    if not recent_events:
        conn.close()
        return 1.0, []

    # ── 2. 过滤：demand_surge 低置信度 + 极低 severity ────────────
    DEMAND_SEVERITY_MIN = 0.4   # demand_surge 至少需要 0.4 严重度
    SEVERITY_MIN = 0.2          # 其他类型至少 0.2
    filtered = []
    for evt in recent_events:
        sev = evt["severity"]
        etype = evt["event_type"]
        if etype == "demand_surge" and sev < DEMAND_SEVERITY_MIN:
            continue
        if sev < SEVERITY_MIN:
            continue
        filtered.append(evt)

    if not filtered:
        conn.close()
        return 1.0, []

    # ── 3. 查询历史实证均值（同品种同类型） ───────────────────────
    historical_impacts: dict[str, float] = {}
    for evt_type in set(r["event_type"] for r in filtered):
        row = conn.execute("""
            SELECT AVG(price_impact_pct) as avg_impact, COUNT(*) as cnt
            FROM weather_events
            WHERE affected_herbs = ? AND event_type = ?
              AND price_impact_pct IS NOT NULL
              AND ABS(price_impact_pct) > 0.5   -- 排除无意义的零值
        """, (herb_name, evt_type)).fetchone()
        if row and row["avg_impact"] is not None and row["cnt"] >= 2:
            historical_impacts[evt_type] = row["avg_impact"] / 100.0

    conn.close()

    # ── 4. area 事件延迟检查（生长周期内不影响现价） ─────────────
    cycle_years = get_growth_cycle(herb_name)
    cycle_days = cycle_years * 365

    # 默认影响基准（无历史实证时使用）
    DEFAULT_IMPACTS: dict[str, float] = {
        "drought":         0.06,
        "flood":           0.08,
        "frost":           0.05,
        "pest":            0.04,
        "heat_wave":       0.03,
        "area_decrease":   0.04,
        "area_increase":  -0.04,
        "demand_surge":    0.06,
        "export_ban":     -0.04,
        "policy_positive": 0.03,
    }

    # ── 5. 逐条计算（含时效衰减） ─────────────────────────────────
    total_impact = 0.0
    used_events: list[str] = []

    for evt in filtered:
        etype = evt["event_type"]
        sev = float(evt["severity"])
        evt_date = evt["start_date"]
        days_ago = (today - date.fromisoformat(evt_date[:10])).days

        # area 事件：生长周期内效果递进，超出周期才完全生效
        if etype in ("area_increase", "area_decrease"):
            # 种植周期内按进度比例生效（0→100%）
            progress = min(days_ago / max(cycle_days, 30), 1.0)
        else:
            # 其他事件：90天半衰期时效衰减
            progress = math.exp(-days_ago / 90.0)

        # 取历史实证 or 默认值
        if etype in historical_impacts:
            base = historical_impacts[etype]
            source = "实证"
        else:
            base = DEFAULT_IMPACTS.get(etype, 0.03)
            source = "估算"

        contribution = base * sev * progress
        if abs(contribution) >= 0.005:  # 过小的贡献忽略
            total_impact += contribution
            used_events.append(
                f"{etype}({sev:.1f}严重 ×{progress:.2f}时效={contribution*100:+.1f}%[{source}])"
            )

    if not used_events:
        return 1.0, []

    # ── 6. 限制幅度 ±20% ─────────────────────────────────────────
    total_impact = max(-0.20, min(0.20, total_impact))
    factor = 1.0 + total_impact

    direction = "↑" if total_impact > 0 else "↓"
    reasons.append(
        f"近期事件综合影响 {direction}{abs(total_impact)*100:.1f}%"
        f"（{len(used_events)}条有效事件）"
    )

    return round(factor, 4), reasons


def get_price_adjustment(herb_name: str, events: list[dict] | None = None) -> dict:
    """综合计算价格调整因子

    Args:
        herb_name: 药材名称
        events: 可选的事件列表 [{"type": "drought", "severity": 0.7, "affected_pct": 0.5}]

    Returns:
        {
            "factor": float,          # 综合调整系数 (如 1.05 = 预计上涨5%)
            "seasonal": float,        # 季节因子
            "supply_cycle": float,    # 供给周期因子
            "concentration_risk": float, # 集中度风险
            "event_impact": float,    # 事件影响
            "growth_cycle_years": int,# 种植周期
            "harvest_months": list,   # 产新月份
            "reasons": list[str],     # 调整原因说明
            "confidence": float,      # 置信度 0-1
        }
    """
    reasons = []
    confidence = 0.6  # 基础置信度

    # 1) 季节因子
    seasonal = calc_seasonal_factor(herb_name)
    if seasonal > 1.02:
        reasons.append(f"临近产新季，青黄不接期价格支撑(+{(seasonal-1)*100:.1f}%)")
    elif seasonal < 0.98:
        reasons.append(f"产新期新货上市压力({(seasonal-1)*100:.1f}%)")

    # 2) 供给周期因子
    supply_cycle = calc_supply_cycle_factor(herb_name)
    cycle_years = get_growth_cycle(herb_name)
    if supply_cycle > 1.02:
        reasons.append(f"{cycle_years}年前低价期缩种，当前供给偏紧(+{(supply_cycle-1)*100:.1f}%)")
        confidence += 0.1
    elif supply_cycle < 0.98:
        reasons.append(f"{cycle_years}年前高价期扩种，当前供给释放({(supply_cycle-1)*100:.1f}%)")
        confidence += 0.1

    # 3) 产区集中度风险
    concentration = calc_production_concentration_risk(herb_name)
    if concentration > 1.03:
        reasons.append(f"产区高度集中，单一灾害风险溢价(+{(concentration-1)*100:.1f}%)")

    # 3.5) 历史气象事件实证影响（基于该品种历史数据）
    historical_weather = 1.0
    try:
        historical_weather, hw_reasons = get_historical_weather_impact(herb_name)
        if historical_weather > 1.02:
            reasons.extend(hw_reasons)
            reasons.append(f"  → 近期事件上调 +{(historical_weather-1)*100:.1f}%")
            confidence += 0.15
        elif historical_weather < 0.98:
            reasons.extend(hw_reasons)
            reasons.append(f"  → 近期事件下调 {(historical_weather-1)*100:.1f}%")
            confidence += 0.10
    except Exception:
        pass

    # 4) 事件影响（手动传入 + 自动气象检测）
    event_factor = 1.0

    # 自动气象检测
    weather_alerts = []
    try:
        from weather_monitor import WeatherMonitor
        monitor = WeatherMonitor()
        weather_result = monitor.check_herb_alerts(herb_name)
        if weather_result["overall_impact"] != 1.0:
            event_factor *= weather_result["overall_impact"]
            reasons.append(f"气象异常: {weather_result['summary'][:60]}")
            confidence += 0.15
        weather_alerts = weather_result.get("alerts", [])
    except Exception:
        pass

    # 手动传入的事件
    if events:
        for evt in events:
            evt_type = evt.get("type", "")
            severity = evt.get("severity", 0.5)  # 0-1
            affected_pct = evt.get("affected_pct", 0.3)  # 受影响产区占比

            if evt_type in EVENT_IMPACT:
                info = EVENT_IMPACT[evt_type]
                low, high = info["factor_range"]
                impact = low + (high - low) * severity * affected_pct
                event_factor *= impact
                reasons.append(f"{info['name']}影响({severity*100:.0f}%严重度, "
                               f"{affected_pct*100:.0f}%产区受影响) → +{(impact-1)*100:.1f}%")
                confidence += 0.15

    # 综合因子
    factor = seasonal * supply_cycle * concentration * historical_weather * event_factor

    # 限制范围
    factor = max(0.75, min(1.40, factor))
    confidence = min(confidence, 0.95)

    return {
        "factor": round(factor, 4),
        "seasonal": round(seasonal, 4),
        "supply_cycle": round(supply_cycle, 4),
        "concentration_risk": round(concentration, 4),
        "historical_weather": round(historical_weather, 4),
        "event_impact": round(event_factor, 4),
        "growth_cycle_years": cycle_years,
        "harvest_months": get_harvest_months(herb_name),
        "reasons": reasons,
        "confidence": round(confidence, 2),
    }


if __name__ == "__main__":
    import sys

    herbs = sys.argv[1:] if len(sys.argv) > 1 else ["白术", "白芍", "当归", "三七", "黄芪"]
    today = date.today()

    for herb in herbs:
        adj = get_price_adjustment(herb)
        print(f"\n{'='*50}")
        print(f"  {herb} | 种植周期: {adj['growth_cycle_years']}年 | "
              f"产新: {adj['harvest_months']}月")
        print(f"{'='*50}")
        print(f"  综合因子: {adj['factor']:.4f} "
              f"({'↑' if adj['factor'] > 1 else '↓'}{abs(adj['factor']-1)*100:.2f}%)")
        print(f"  季节因子: {adj['seasonal']:.4f}")
        print(f"  供给周期: {adj['supply_cycle']:.4f}")
        print(f"  集中度:   {adj['concentration_risk']:.4f}")
        print(f"  置信度:   {adj['confidence']:.2f}")
        if adj["reasons"]:
            print(f"  原因:")
            for r in adj["reasons"]:
                print(f"    • {r}")

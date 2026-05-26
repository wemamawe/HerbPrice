"""供需平衡表模块

为每个品种构建简化版年度供需平衡表：
    供给 = 基础产量 × (1 + 面积变动%) × (1 - 灾害减产%) + 进口量
    需求 = 历史消费基线 × (1 + 年增长率) ± 需求冲击
    供需缺口 → 价格影响系数

用法:
    python supply_demand.py 白术 白芍 当归
"""

import sys
import logging
from datetime import date, timedelta
from db import get_connection

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# 中药材行业年需求增长率（保守估计）
DEFAULT_DEMAND_GROWTH = 0.05  # 5%/年

# 供需缺口 → 价格影响映射
# 缺口率 > 0 表示供过于求（价格下行），< 0 表示供不应求（价格上行）
SUPPLY_DEMAND_IMPACT = [
    (-0.30, 1.30),   # 供给不足30%+ → 涨30%
    (-0.20, 1.20),   # 供给不足20% → 涨20%
    (-0.10, 1.10),   # 供给不足10% → 涨10%
    (-0.05, 1.05),   # 供给不足5% → 涨5%
    (0.05, 1.00),    # 基本平衡 → 中性
    (0.10, 0.97),    # 供过于求10% → 跌3%
    (0.20, 0.93),    # 供过于求20% → 跌7%
    (0.30, 0.88),    # 供过于求30% → 跌12%
    (0.50, 0.80),    # 供过于求50% → 跌20%
]


def get_base_production(herb_name: str) -> dict:
    """获取药材的基础产量数据

    Returns:
        {
            "total_tons": float,  # 全国总产量(吨)
            "import_tons": float, # 进口量(吨)
            "regions": [{"origin": str, "tons": float, "pct": float}]
        }
    """
    conn = get_connection()

    # 国内产量
    domestic = conn.execute("""
        SELECT origin, province, annual_output_tons, output_percent
        FROM herb_origins
        WHERE herb_name = ? AND province != '进口' AND annual_output_tons IS NOT NULL
        ORDER BY annual_output_tons DESC
    """, (herb_name,)).fetchall()

    # 进口量
    imports = conn.execute("""
        SELECT origin, annual_output_tons
        FROM herb_origins
        WHERE herb_name = ? AND province = '进口' AND annual_output_tons IS NOT NULL
    """, (herb_name,)).fetchall()

    conn.close()

    domestic_total = sum(r["annual_output_tons"] for r in domestic)
    import_total = sum(r["annual_output_tons"] for r in imports)

    regions = [
        {
            "origin": r["origin"],
            "province": r["province"],
            "tons": r["annual_output_tons"],
            "pct": r["output_percent"] or 0,
        }
        for r in domestic
    ]

    return {
        "total_domestic_tons": domestic_total,
        "import_tons": import_total,
        "total_tons": domestic_total + import_total,
        "regions": regions,
    }


def get_supply_shocks(herb_name: str, year: int | None = None) -> dict:
    """获取影响供给的事件（灾害减产、面积变动）

    Returns:
        {
            "disaster_reduction_pct": float,  # 灾害减产比例 (0-1)
            "area_change_pct": float,         # 面积变动比例 (-1~+1)
            "events": [...]
        }
    """
    if year is None:
        year = date.today().year

    conn = get_connection()
    year_start = f"{year}-01-01"
    year_end = f"{year}-12-31"

    # 查询该品种今年的灾害事件
    events = conn.execute("""
        SELECT event_type, severity, origin, detail
        FROM weather_events
        WHERE affected_herbs = ?
              AND start_date >= ? AND start_date <= ?
    """, (herb_name, year_start, year_end)).fetchall()

    conn.close()

    disaster_reduction = 0.0
    area_change = 0.0
    event_list = []

    for evt in events:
        evt_type = evt["event_type"]
        severity = evt["severity"]

        if evt_type in ("drought", "flood", "frost", "pest", "heat_wave"):
            # 灾害→减产。多个灾害叠加但不超过50%
            reduction = severity * 0.15  # 每个灾害最多减产15%
            disaster_reduction += reduction
            event_list.append({
                "type": evt_type,
                "severity": severity,
                "origin": evt["origin"],
                "reduction_pct": round(reduction * 100, 1),
            })
        elif evt_type == "area_increase":
            area_change += severity * 0.20  # 扩种信号
            event_list.append({
                "type": "area_increase",
                "severity": severity,
                "change_pct": round(severity * 20, 1),
            })
        elif evt_type == "area_decrease":
            area_change -= severity * 0.15  # 缩种信号
            event_list.append({
                "type": "area_decrease",
                "severity": severity,
                "change_pct": round(-severity * 15, 1),
            })

    # 限制范围
    disaster_reduction = min(disaster_reduction, 0.50)
    area_change = max(-0.30, min(0.50, area_change))

    return {
        "disaster_reduction_pct": round(disaster_reduction, 4),
        "area_change_pct": round(area_change, 4),
        "events": event_list,
    }


def get_demand_shocks(herb_name: str, year: int | None = None) -> dict:
    """获取需求侧冲击"""
    if year is None:
        year = date.today().year

    conn = get_connection()
    year_start = f"{year}-01-01"
    year_end = f"{year}-12-31"

    events = conn.execute("""
        SELECT event_type, severity, detail
        FROM weather_events
        WHERE affected_herbs = ?
              AND start_date >= ? AND start_date <= ?
              AND event_type IN ('demand_surge', 'policy_positive', 'export_ban')
    """, (herb_name, year_start, year_end)).fetchall()

    conn.close()

    demand_change = 0.0
    event_list = []

    for evt in events:
        if evt["event_type"] == "demand_surge":
            demand_change += evt["severity"] * 0.15
            event_list.append({"type": "demand_surge", "change_pct": round(evt["severity"] * 15, 1)})
        elif evt["event_type"] == "policy_positive":
            demand_change += evt["severity"] * 0.08
        elif evt["event_type"] == "export_ban":
            demand_change -= evt["severity"] * 0.10

    return {
        "demand_change_pct": round(max(-0.20, min(0.30, demand_change)), 4),
        "events": event_list,
    }


def gap_to_price_factor(gap_rate: float) -> float:
    """将供需缺口率转换为价格影响系数"""
    for threshold, factor in SUPPLY_DEMAND_IMPACT:
        if gap_rate <= threshold:
            return factor
    return 0.80  # 严重供过于求


def calc_supply_demand_balance(herb_name: str, year: int | None = None) -> dict:
    """计算品种的供需平衡表

    Returns:
        {
            "herb_name": str,
            "year": int,
            "supply": { ... },
            "demand": { ... },
            "balance": { ... },
            "price_factor": float,
            "direction": "up" | "down" | "neutral",
            "confidence": float,
        }
    """
    if year is None:
        year = date.today().year

    # 基础产量
    production = get_base_production(herb_name)
    if production["total_tons"] == 0:
        return {
            "herb_name": herb_name,
            "year": year,
            "error": "无产量数据",
            "price_factor": 1.0,
            "direction": "neutral",
            "confidence": 0.0,
        }

    # 供给侧冲击
    supply_shocks = get_supply_shocks(herb_name, year)

    # 需求侧冲击
    demand_shocks = get_demand_shocks(herb_name, year)

    # 计算供给
    base_supply = production["total_tons"]
    area_factor = 1.0 + supply_shocks["area_change_pct"]
    disaster_factor = 1.0 - supply_shocks["disaster_reduction_pct"]
    actual_supply = base_supply * area_factor * disaster_factor

    # 计算需求（假设基线=去年供给，即市场基本平衡）
    base_demand = base_supply  # 简化假设
    demand_growth = 1.0 + DEFAULT_DEMAND_GROWTH
    demand_shock_factor = 1.0 + demand_shocks["demand_change_pct"]
    actual_demand = base_demand * demand_growth * demand_shock_factor

    # 供需缺口
    gap = actual_supply - actual_demand
    gap_rate = gap / max(actual_demand, 1) if actual_demand > 0 else 0

    # 转换为价格因子
    price_factor = gap_to_price_factor(gap_rate)

    # 方向判断
    if price_factor > 1.03:
        direction = "up"
    elif price_factor < 0.97:
        direction = "down"
    else:
        direction = "neutral"

    # 置信度（有更多数据时置信度更高）
    confidence = 0.5
    if production["regions"]:
        confidence += 0.1
    if supply_shocks["events"]:
        confidence += 0.15
    if demand_shocks["events"]:
        confidence += 0.1
    confidence = min(confidence, 0.9)

    return {
        "herb_name": herb_name,
        "year": year,
        "supply": {
            "base_tons": round(base_supply, 0),
            "area_change_pct": supply_shocks["area_change_pct"],
            "disaster_reduction_pct": supply_shocks["disaster_reduction_pct"],
            "actual_supply_tons": round(actual_supply, 0),
            "import_tons": production["import_tons"],
            "events": supply_shocks["events"],
        },
        "demand": {
            "base_tons": round(base_demand, 0),
            "growth_rate": DEFAULT_DEMAND_GROWTH,
            "shock_pct": demand_shocks["demand_change_pct"],
            "actual_demand_tons": round(actual_demand, 0),
            "events": demand_shocks["events"],
        },
        "balance": {
            "gap_tons": round(gap, 0),
            "gap_rate": round(gap_rate, 4),
            "status": "供过于求" if gap_rate > 0.05 else ("供不应求" if gap_rate < -0.05 else "基本平衡"),
        },
        "price_factor": round(price_factor, 4),
        "direction": direction,
        "confidence": round(confidence, 2),
        "top_regions": production["regions"][:5],
    }


if __name__ == "__main__":
    herbs = sys.argv[1:] if len(sys.argv) > 1 else ["白术", "白芍", "当归", "三七", "金银花"]

    for herb in herbs:
        result = calc_supply_demand_balance(herb)
        print(f"\n{'='*60}")
        print(f"  {herb} · {result['year']}年供需平衡表")
        print(f"{'='*60}")

        if "error" in result:
            print(f"  ⚠️ {result['error']}")
            continue

        s = result["supply"]
        d = result["demand"]
        b = result["balance"]

        print(f"  【供给侧】")
        print(f"    基础产量: {s['base_tons']:,.0f} 吨")
        if s["area_change_pct"] != 0:
            print(f"    面积变动: {s['area_change_pct']:+.1%}")
        if s["disaster_reduction_pct"] > 0:
            print(f"    灾害减产: -{s['disaster_reduction_pct']:.1%}")
        print(f"    实际供给: {s['actual_supply_tons']:,.0f} 吨")
        if s["import_tons"]:
            print(f"    进口补充: {s['import_tons']:,.0f} 吨")

        print(f"  【需求侧】")
        print(f"    基础需求: {d['base_tons']:,.0f} 吨")
        print(f"    年增长率: +{d['growth_rate']:.0%}")
        if d["shock_pct"] != 0:
            print(f"    需求冲击: {d['shock_pct']:+.1%}")
        print(f"    实际需求: {d['actual_demand_tons']:,.0f} 吨")

        print(f"  【平衡】")
        print(f"    供需缺口: {b['gap_tons']:+,.0f} 吨 ({b['gap_rate']:+.1%})")
        print(f"    状态: {b['status']}")
        arrow = "↑" if result["direction"] == "up" else ("↓" if result["direction"] == "down" else "→")
        print(f"    价格因子: {result['price_factor']:.4f} {arrow}")
        print(f"    置信度: {result['confidence']:.0%}")

"""LLM 综合研判研报模块

整合所有数据源（时序预测、多因子、供需平衡、XGBoost、气象、新闻），
由 LLM 作为分析师输出结构化研判报告。

用法:
    python research_agent.py 白术
    python research_agent.py 白芍 当归 --format json
"""

import sys
import json
import logging
from datetime import date
from db import get_connection

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def gather_analysis_context(herb_name: str) -> dict:
    """收集品种的全部分析数据，作为 LLM 研判的上下文"""

    context = {"herb_name": herb_name, "analysis_date": date.today().isoformat()}

    # 1) 当前价格和趋势
    conn = get_connection()
    rows = conn.execute("""
        SELECT date, price FROM estimated_daily_prices
        WHERE name = ? ORDER BY date DESC LIMIT 365
    """, (herb_name,)).fetchall()

    if rows:
        prices = [r["price"] for r in rows]
        context["price"] = {
            "current": prices[0],
            "date": rows[0]["date"],
            "7d_ago": prices[6] if len(prices) > 6 else None,
            "30d_ago": prices[29] if len(prices) > 29 else None,
            "90d_ago": prices[89] if len(prices) > 89 else None,
            "365d_ago": prices[-1] if len(prices) >= 365 else None,
            "1y_high": max(prices),
            "1y_low": min(prices),
        }
        if context["price"]["30d_ago"]:
            context["price"]["30d_change_pct"] = round(
                (prices[0] - prices[29]) / prices[29] * 100, 1
            )
        if context["price"]["365d_ago"]:
            context["price"]["1y_change_pct"] = round(
                (prices[0] - prices[-1]) / prices[-1] * 100, 1
            )

    # 2) 产地信息
    origins = conn.execute("""
        SELECT origin, province, annual_output_tons, output_percent, is_daodi
        FROM herb_origins
        WHERE herb_name = ? AND province != '进口' AND annual_output_tons IS NOT NULL
        ORDER BY annual_output_tons DESC LIMIT 5
    """, (herb_name,)).fetchall()
    context["production"] = [dict(r) for r in origins]

    # 3) 近期事件
    events = conn.execute("""
        SELECT event_type, severity, origin, detail, start_date
        FROM weather_events
        WHERE affected_herbs = ? AND start_date >= date('now', '-180 days')
        ORDER BY start_date DESC LIMIT 10
    """, (herb_name,)).fetchall()
    context["recent_events"] = [dict(r) for r in events]

    conn.close()

    # 4) 多因子分析
    try:
        from forecast_factors import get_price_adjustment
        context["factors"] = get_price_adjustment(herb_name)
    except Exception as e:
        context["factors"] = {"error": str(e)}

    # 5) 供需平衡
    try:
        from supply_demand import calc_supply_demand_balance
        context["supply_demand"] = calc_supply_demand_balance(herb_name)
    except Exception as e:
        context["supply_demand"] = {"error": str(e)}

    # 6) XGBoost 预测
    try:
        from forecast_xgb import predict_herb
        context["xgb_prediction"] = predict_herb(herb_name)
    except Exception as e:
        context["xgb_prediction"] = {"error": str(e)}

    # 7) 气象状况
    try:
        from weather_monitor import WeatherMonitor
        monitor = WeatherMonitor()
        context["weather"] = monitor.check_herb_alerts(herb_name)
    except Exception as e:
        context["weather"] = {"error": str(e)}

    return context


RESEARCH_PROMPT = """你是一位**资深战略分析师与中药材市场深度调查专家**。风格标准：冷峻、专业、数据驱动——像麦肯锡的机密分析报告，没有情绪渲染，没有模糊表述，每个结论后面都是数字或事实。

请基于以下数据对【{herb_name}】做出深度研判分析。

## 分析数据

{context_json}

---

## 分析方法论（必须遵循）

### 第一步：多维拆解
将品种的价格走势拆解为以下维度分别分析：
- **供给侧**：产量、面积变动、种植周期位置、产区集中度风险
- **需求侧**：基础需求趋势、突发需求、季节性波动
- **库存/流通**：渠道库存水平、持货商心态
- **技术面**：价格位置（距高/低点）、趋势动量、XGBoost信号
- **事件驱动**：灾害、政策、产新、疫情

### 第二步：矛盾分析
主动识别不同信号之间的观点冲突：
- 技术面 vs 基本面方向是否一致？
- 短期信号 vs 中期逻辑是否矛盾？
- 不要做简单的信号堆砌，要分析冲突成因，给出自己的判断

### 第三步：非共识视角
找出被主流叙事忽略但数据异常的细节：
- 什么因素被市场低估/忽视？
- 有没有"反直觉但数据支撑"的结论？

### 第四步：三情景预测（乐观/基准/悲观）
不只给一个价格区间，而是给出三种情景：
- **乐观情景**：什么条件下最有利？目标价多少？
- **基准情景**：最可能的路径
- **悲观情景**：什么风险会导致最差结果？

### 第五步：批判性自审
完成分析后，切换为严苛审查者身份：
- 结论是否过于依赖单一信源/信号？
- 有无重要利益相关方视角被遗漏？
- 预测幅度是否合理？（参照历史波动率校准）

---

## 硬性约束

1. **价格区间必须合理**：30天区间至少覆盖当前价格±5%，90天至少±10%。中药材单月波动5-15%是常态。
2. **信号一致时要果断**：供需缺口、多因子、XGBoost三信号方向一致 → 不允许给 neutral。
3. **供需缺口是核心矛盾**：缺口率>10%应主导方向判断，权重>技术面。
4. **底部有支撑**：价格距1年最低点<5% → 不应大幅看跌。
5. **禁止模糊表述**：不能说"可能涨也可能跌"——必须给明确方向+置信度。
6. **执行摘要必须以数据开头**：禁止用"综合来看"、"整体而言"、"值得注意的是"开头。

---

## 输出 JSON 结构

```json
{{
  "executive_summary": "直接用数据/事实开头的一段话（80字内），禁止使用'综合来看''整体而言'",
  "direction": "up|down|neutral",
  "confidence": 0.7,
  "time_horizon": "短期(1-30天)预期",

  "scenario_analysis": {{
    "bull_case": {{
      "condition": "什么条件触发乐观情景",
      "30d_target": 数字,
      "90d_target": 数字,
      "probability": 0.3
    }},
    "base_case": {{
      "condition": "最可能的路径描述",
      "30d_target": 数字,
      "90d_target": 数字,
      "probability": 0.5
    }},
    "bear_case": {{
      "condition": "什么风险触发悲观情景",
      "30d_target": 数字,
      "90d_target": 数字,
      "probability": 0.2
    }}
  }},

  "price_range": {{
    "30d_low": 数字,
    "30d_high": 数字,
    "90d_low": 数字,
    "90d_high": 数字
  }},

  "multi_dimension_analysis": {{
    "supply_side": "供给侧核心结论（1-2句）",
    "demand_side": "需求侧核心结论（1-2句）",
    "technical": "技术面结论（价格位置+动量）",
    "event_driven": "事件驱动因素"
  }},

  "key_factors": [
    {{"factor": "因素名", "impact": "positive|negative|neutral", "weight": 0.3, "detail": "说明", "data_quality": "high|medium|low"}}
  ],

  "contrarian_view": "非共识视角——被市场忽略但有数据支撑的观点（2-3句）",

  "signal_conflicts": "信号矛盾分析——技术面vs基本面的冲突以及你的判断（2-3句）",

  "risks": ["具体风险+触发条件（非泛泛而谈）"],
  "opportunities": ["具体机会+触发条件"],

  "recommendation": "明确操作建议（分批建仓/逢高出货/严格观望/逢低补货）",
  "position_sizing": "建议仓位比例（如：可配30%仓位，分2-3次建仓）",

  "reasoning": "详细分析逻辑推导（200字内），必须说明：1)方向选择的核心依据 2)否定相反方向的理由 3)最大不确定性在哪里",

  "self_review": "自审备注——本分析的局限性是什么，哪些数据不足可能影响结论"
}}
```

## 区间校准参考
- 价格处1年低位(跌>50%): 30天 -5%~+10%, 90天 -8%~+20%
- 供需缺口>20%看涨: 30天 -3%~+15%, 90天 +5%~+30%
- 扩种压力看跌: 30天 -10%~+3%, 90天 -20%~-5%
- 基本平衡震荡: 30天 -8%~+8%, 90天 -12%~+12%
- 历史事件类比：岷县干旱→当归+30~80%，白术大规模扩种→-20~-40%
"""


def generate_research_report(herb_name: str, llm_client=None) -> dict:
    """生成品种研判报告

    Returns:
        LLM 输出的结构化研判报告
    """
    if llm_client is None:
        from llm_client import get_llm_client
        llm_client = get_llm_client()

    # 收集分析上下文
    context = gather_analysis_context(herb_name)

    # 精简上下文（移除过大的字段，避免超出 token 限制）
    slim_context = {
        "price": context.get("price"),
        "production": context.get("production", [])[:3],
        "recent_events": context.get("recent_events", [])[:5],
        "factors": {
            k: v for k, v in context.get("factors", {}).items()
            if k in ("factor", "seasonal", "supply_cycle", "concentration_risk",
                     "growth_cycle_years", "harvest_months", "reasons", "confidence")
        },
        "supply_demand": {
            k: v for k, v in context.get("supply_demand", {}).items()
            if k in ("balance", "price_factor", "direction", "confidence")
        },
        "xgb_prediction": {
            k: v for k, v in context.get("xgb_prediction", {}).items()
            if k in ("predictions", "direction")
        },
        "weather": {
            k: v for k, v in context.get("weather", {}).items()
            if k in ("overall_impact", "summary", "alerts")
        } if isinstance(context.get("weather"), dict) else {},
    }

    prompt = RESEARCH_PROMPT.format(
        herb_name=herb_name,
        context_json=json.dumps(slim_context, ensure_ascii=False, indent=2)
    )

    messages = [
        {"role": "system", "content": "你是资深战略分析师，擅长中药材市场深度调研。风格冷峻、数据驱动，像麦肯锡机密分析报告。严格输出 JSON 格式。"},
        {"role": "user", "content": prompt},
    ]

    try:
        report = llm_client.chat_json(messages, temperature=0.3, max_tokens=2000)
        report["herb_name"] = herb_name
        report["analysis_date"] = date.today().isoformat()
        report["data_context"] = slim_context
        return report
    except Exception as e:
        log.error(f"研报生成失败: {e}")
        return {
            "herb_name": herb_name,
            "error": str(e),
            "data_context": slim_context,
        }


def generate_report_fallback(herb_name: str) -> dict:
    """无 LLM 时的降级方案：基于规则生成简要研判"""
    context = gather_analysis_context(herb_name)

    # 综合方向判断
    signals = []
    factors = context.get("factors", {})
    if isinstance(factors, dict) and "factor" in factors:
        if factors["factor"] > 1.03:
            signals.append(("up", factors["factor"] - 1))
        elif factors["factor"] < 0.97:
            signals.append(("down", 1 - factors["factor"]))

    sd = context.get("supply_demand", {})
    if isinstance(sd, dict) and "direction" in sd:
        if sd["direction"] == "up":
            signals.append(("up", 0.1))
        elif sd["direction"] == "down":
            signals.append(("down", 0.1))

    xgb = context.get("xgb_prediction", {})
    if isinstance(xgb, dict) and "direction" in xgb:
        if xgb["direction"] == "up":
            signals.append(("up", 0.1))
        elif xgb["direction"] == "down":
            signals.append(("down", 0.1))

    up_score = sum(w for d, w in signals if d == "up")
    down_score = sum(w for d, w in signals if d == "down")

    if up_score > down_score + 0.05:
        direction = "up"
        summary = f"{herb_name}多因子看涨，主要受供给收缩/产区风险支撑"
    elif down_score > up_score + 0.05:
        direction = "down"
        summary = f"{herb_name}多因子看跌，主要受供给过剩/扩种压力影响"
    else:
        direction = "neutral"
        summary = f"{herb_name}多空因素交织，短期震荡为主"

    price = context.get("price", {})
    current = price.get("current", 0)

    return {
        "herb_name": herb_name,
        "analysis_date": date.today().isoformat(),
        "summary": summary,
        "direction": direction,
        "confidence": 0.5,
        "price_range": {
            "30d_low": round(current * 0.9, 2),
            "30d_high": round(current * 1.1, 2),
        },
        "key_factors": [
            {"factor": r, "impact": "positive" if "+" in r else "negative"}
            for r in factors.get("reasons", [])[:3]
        ],
        "recommendation": "观望",
        "reasoning": "基于规则生成（LLM 不可用）",
        "data_context": context,
    }


if __name__ == "__main__":
    herbs = [a for a in sys.argv[1:] if not a.startswith("--")]
    use_json = "--format" in sys.argv and "json" in sys.argv

    if not herbs:
        herbs = ["白术"]

    from llm_client import get_llm_client
    client = get_llm_client()
    llm_available = client.is_available()

    if not llm_available:
        log.warning("LLM 不可用，使用降级模式")

    for herb in herbs:
        log.info(f"正在分析: {herb}")

        if llm_available:
            report = generate_research_report(herb, client)
        else:
            report = generate_report_fallback(herb)

        if use_json:
            print(json.dumps(report, ensure_ascii=False, indent=2))
        else:
            print(f"\n{'='*60}")
            print(f"  {herb} · 综合研判报告 · {report.get('analysis_date', '')}")
            print(f"{'='*60}")
            print(f"\n  📊 总结: {report.get('summary', '暂无')}")
            d = report.get("direction", "neutral")
            arrow = {"up": "↑ 看涨", "down": "↓ 看跌", "neutral": "→ 震荡"}
            print(f"  📈 方向: {arrow.get(d, d)} (置信度:{report.get('confidence', '?')})")

            pr = report.get("price_range", {})
            if pr:
                print(f"  💰 30天区间: ¥{pr.get('30d_low', '?')} ~ ¥{pr.get('30d_high', '?')}")

            kf = report.get("key_factors", [])
            if kf:
                print(f"  🔑 关键因素:")
                for f in kf[:4]:
                    icon = "🟢" if f.get("impact") == "positive" else "🔴"
                    print(f"    {icon} {f.get('factor', f.get('detail', ''))}")

            risks = report.get("risks", [])
            if risks:
                print(f"  ⚠️ 风险: {'; '.join(risks[:3])}")

            rec = report.get("recommendation", "")
            if rec:
                print(f"  💡 建议: {rec}")

            reasoning = report.get("reasoning", "")
            if reasoning:
                print(f"\n  📝 分析逻辑:\n  {reasoning}")

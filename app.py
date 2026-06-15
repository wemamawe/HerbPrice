"""药材价格趋势图 Web 应用"""

from flask import Flask, jsonify, request, send_from_directory
from db import get_connection
from forecast import forecast_variety

app = Flask(__name__, static_folder="static")

# 预测结果缓存 {name: {result, timestamp}}
_forecast_cache: dict = {}


@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/tcm")
def tcm_page():
    return send_from_directory("static", "tcm.html")


@app.route("/<path:filename>")
def static_files(filename):
    return send_from_directory("static", filename)


@app.route("/api/varieties")
def api_varieties():
    """获取所有有估算价格数据的品种列表"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT name, COUNT(*) as cnt,
               MIN(date) as min_date, MAX(date) as max_date,
               ROUND(MIN(price), 2) as min_price,
               ROUND(MAX(price), 2) as max_price
        FROM estimated_daily_prices
        GROUP BY name
        ORDER BY name
    """).fetchall()
    conn.close()
    return jsonify([{
        "name": r["name"],
        "count": r["cnt"],
        "minDate": r["min_date"],
        "maxDate": r["max_date"],
        "minPrice": r["min_price"],
        "maxPrice": r["max_price"],
    } for r in rows])


@app.route("/api/prices")
def api_prices():
    """获取指定品种在时间范围内的价格数据"""
    name = request.args.get("name", "")
    start = request.args.get("start", "")
    end = request.args.get("end", "")

    if not name:
        return jsonify({"error": "缺少 name 参数"}), 400

    conn = get_connection()
    query = """
        SELECT date, price, source
        FROM estimated_daily_prices
        WHERE name = ?
    """
    params: list = [name]

    if start:
        query += " AND date >= ?"
        params.append(start)
    if end:
        query += " AND date <= ?"
        params.append(end)

    query += " ORDER BY date"

    rows = conn.execute(query, params).fetchall()
    conn.close()

    return jsonify({
        "name": name,
        "count": len(rows),
        "data": [{
            "date": r["date"],
            "price": round(r["price"], 2),
            "source": r["source"],
        } for r in rows],
    })


@app.route("/api/k_value")
def api_k_value():
    """获取指定品种的 K 值信息"""
    name = request.args.get("name", "")
    if not name:
        return jsonify({"error": "缺少 name 参数"}), 400

    conn = get_connection()
    row = conn.execute(
        "SELECT * FROM variety_k_values WHERE name = ?", (name,)
    ).fetchone()
    conn.close()

    if not row:
        return jsonify(None)

    return jsonify({
        "name": row["name"],
        "kValue": round(row["k_value"], 4),
        "kCv": round(row["k_cv"] * 100, 2) if row["k_cv"] else None,
        "basePrice": round(row["base_price"], 2) if row["base_price"] else None,
        "sampleCount": row["sample_count"],
    })


@app.route("/api/herb_detail")
def api_herb_detail():
    """获取品种基础信息：规格、历史高低价、产新时间、种植周期、产地"""
    name = request.args.get("name", "")
    if not name:
        return jsonify({"error": "缺少 name 参数"}), 400

    conn = get_connection()

    # 基础信息（varieties 有多行，聚合规格和产地）
    variety_rows = conn.execute(
        "SELECT standard, origin FROM varieties WHERE name = ?", (name,)
    ).fetchall()
    standards = sorted(set(r["standard"] for r in variety_rows if r["standard"]))
    origins_var = sorted(set(r["origin"] for r in variety_rows if r["origin"]))

    # 全历史最高/最低价（所有数据）
    price_stats = conn.execute("""
        SELECT ROUND(MIN(price), 2) as all_low, ROUND(MAX(price), 2) as all_high,
               MIN(date) as earliest, MAX(date) as latest,
               COUNT(*) as total_days
        FROM estimated_daily_prices WHERE name = ?
    """, (name,)).fetchone()

    # 近1年高低价
    price_1y = conn.execute("""
        SELECT ROUND(MIN(price), 2) as low_1y, ROUND(MAX(price), 2) as high_1y
        FROM estimated_daily_prices
        WHERE name = ? AND date >= date('now', '-365 days')
    """, (name,)).fetchone()

    # 历史最高价对应日期
    high_date = conn.execute("""
        SELECT date FROM estimated_daily_prices
        WHERE name = ? AND price = (SELECT MAX(price) FROM estimated_daily_prices WHERE name = ?)
        LIMIT 1
    """, (name, name)).fetchone()

    # 历史最低价对应日期
    low_date = conn.execute("""
        SELECT date FROM estimated_daily_prices
        WHERE name = ? AND price = (SELECT MIN(price) FROM estimated_daily_prices WHERE name = ?)
        LIMIT 1
    """, (name, name)).fetchone()

    # 产地信息
    origins = conn.execute("""
        SELECT origin, province, is_daodi, annual_output_tons, output_percent
        FROM herb_origins WHERE herb_name = ?
        ORDER BY COALESCE(output_percent, 0) DESC, is_daodi DESC
        LIMIT 6
    """, (name,)).fetchall()

    conn.close()

    # 产新时间和种植周期（来自 forecast_factors）
    try:
        from forecast_factors import get_harvest_months, get_growth_cycle
        harvest_months = get_harvest_months(name)
        growth_cycle = get_growth_cycle(name)
    except Exception:
        harvest_months = []
        growth_cycle = None

    month_names = ["", "一", "二", "三", "四", "五", "六", "七", "八", "九", "十", "十一", "十二"]

    has_price_data = price_stats and price_stats["total_days"] and price_stats["total_days"] > 0

    result = {
        "name": name,
        "standards": standards,
        "originsVar": origins_var,
        "allTimeHigh": price_stats["all_high"] if has_price_data else None,
        "allTimeLow": price_stats["all_low"] if has_price_data else None,
        "allTimeHighDate": high_date["date"] if high_date else None,
        "allTimeLowDate": low_date["date"] if low_date else None,
        "high1y": price_1y["high_1y"] if price_1y and price_1y["high_1y"] else None,
        "low1y": price_1y["low_1y"] if price_1y and price_1y["low_1y"] else None,
        "earliestDate": price_stats["earliest"] if has_price_data else None,
        "totalDays": price_stats["total_days"] if has_price_data else 0,
        "harvestMonths": harvest_months,
        "harvestMonthNames": [f"{month_names[m]}月" for m in harvest_months if 1 <= m <= 12],
        "growthCycleYears": growth_cycle,
        "origins": [
            {
                "origin": r["origin"],
                "province": r["province"],
                "isDaodi": bool(r["is_daodi"]),
                "outputTons": r["annual_output_tons"],
                "outputPct": r["output_percent"],
            } for r in origins
        ],
    }
    return jsonify(result)


@app.route("/api/forecast")
def api_forecast():
    """预测指定品种未来半年的价格趋势"""
    import time
    name = request.args.get("name", "")
    if not name:
        return jsonify({"error": "缺少 name 参数"}), 400

    # 缓存检查（同一品种6小时内复用）
    cache_ttl = 6 * 3600
    cached = _forecast_cache.get(name)
    if cached and (time.time() - cached["timestamp"]) < cache_ttl:
        return jsonify(cached["result"])

    try:
        result = forecast_variety(name)
        _forecast_cache[name] = {"result": result, "timestamp": time.time()}
        return jsonify(result)
    except ValueError as e:
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        return jsonify({"error": f"预测失败: {str(e)}"}), 500


@app.route("/api/forecast/factors")
def api_forecast_factors():
    """获取指定品种的预测多因子分析

    Query params:
        name: 药材名称（必填）
    """
    name = request.args.get("name", "")
    if not name:
        return jsonify({"error": "缺少 name 参数"}), 400

    try:
        from forecast_factors import get_price_adjustment
        result = get_price_adjustment(name)
        result["name"] = name
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": f"因子分析失败: {str(e)}"}), 500


@app.route("/api/weather/alerts")
def api_weather_alerts():
    """获取指定药材产区的气象异常预警

    Query params:
        name: 药材名称（必填）
    """
    name = request.args.get("name", "")
    if not name:
        return jsonify({"error": "缺少 name 参数"}), 400

    try:
        from weather_monitor import WeatherMonitor
        monitor = WeatherMonitor()
        result = monitor.check_herb_alerts(name)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": f"气象检测失败: {str(e)}"}), 500


@app.route("/api/weather/summary")
def api_weather_summary():
    """获取指定药材主要产区近7天天气概况

    Query params:
        name: 药材名称（必填）
    """
    name = request.args.get("name", "")
    if not name:
        return jsonify({"error": "缺少 name 参数"}), 400

    try:
        from weather_monitor import WeatherMonitor
        monitor = WeatherMonitor()
        result = monitor.get_production_area_weather_summary(name)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": f"天气查询失败: {str(e)}"}), 500


# ── 供需平衡 & XGBoost & LLM 研判 API ────────────────────

@app.route("/api/supply-demand")
def api_supply_demand():
    """获取品种供需平衡表"""
    name = request.args.get("name", "")
    if not name:
        return jsonify({"error": "缺少 name 参数"}), 400
    try:
        from supply_demand import calc_supply_demand_balance
        result = calc_supply_demand_balance(name)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": f"供需分析失败: {str(e)}"}), 500


@app.route("/api/forecast/xgb")
def api_forecast_xgb():
    """XGBoost 跨品种模型预测"""
    name = request.args.get("name", "")
    if not name:
        return jsonify({"error": "缺少 name 参数"}), 400
    try:
        from forecast_xgb import predict_herb
        result = predict_herb(name)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": f"XGBoost 预测失败: {str(e)}"}), 500


@app.route("/api/research")
def api_research():
    """LLM 综合研判研报"""
    name = request.args.get("name", "")
    if not name:
        return jsonify({"error": "缺少 name 参数"}), 400
    try:
        from research_agent import generate_research_report, generate_report_fallback
        from llm_client import get_llm_client
        client = get_llm_client()
        if client.is_available():
            result = generate_research_report(name, client)
        else:
            result = generate_report_fallback(name)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": f"研判生成失败: {str(e)}"}), 500


@app.route("/api/research/pdf")
def api_research_pdf():
    """生成麦肯锡风格 PDF 研判报告并下载"""
    name = request.args.get("name", "")
    if not name:
        return jsonify({"error": "缺少 name 参数"}), 400
    try:
        from research_agent import generate_research_report, generate_report_fallback
        from llm_client import get_llm_client
        client = get_llm_client()
        if client.is_available():
            report = generate_research_report(name, client)
        else:
            report = generate_report_fallback(name)

        # 生成 PDF
        from report_gen import generate_herb_report_pdf
        pdf_path = generate_herb_report_pdf(name, report)

        from flask import send_file
        return send_file(pdf_path, as_attachment=True,
                         download_name=f"{name}_研判报告.pdf",
                         mimetype="application/pdf")
    except ImportError as e:
        return jsonify({"error": f"PDF 生成依赖缺失: {str(e)}（需安装 reportlab matplotlib）"}), 500
    except Exception as e:
        return jsonify({"error": f"PDF 生成失败: {str(e)}"}), 500


@app.route("/api/news/interpret", methods=["POST"])
def api_news_interpret():
    """LLM 解读新闻文本"""
    data = request.get_json(force=True, silent=True) or {}
    text = data.get("text", "")
    if not text:
        return jsonify({"error": "缺少 text 参数"}), 400
    try:
        from llm_news_interpreter import interpret_news, save_events_to_db
        result = interpret_news(text)
        events = result.get("events", [])
        if events:
            saved = save_events_to_db(events)
            result["saved_count"] = saved
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": f"新闻解读失败: {str(e)}"}), 500


@app.route("/api/news/list")
def api_news_list():
    """获取已爬取的新闻列表

    Query params:
        herb: 药材名（可选）
        source: 来源筛选（可选）
        days: 最近N天（默认30）
        page: 页码（默认1）
        page_size: 每页条数（默认20）
    """
    from datetime import date, timedelta
    herb = request.args.get("herb", "")
    source = request.args.get("source", "")
    days = int(request.args.get("days", 30))
    page = max(1, int(request.args.get("page", 1)))
    page_size = min(100, max(1, int(request.args.get("page_size", 20))))
    cutoff = (date.today() - timedelta(days=days)).isoformat()

    try:
        conn = get_connection()

        conditions = ["pub_date >= ?"]
        params: list = [cutoff]
        if herb:
            conditions.append("(herb_names LIKE ? OR title LIKE ?)")
            params.extend([f"%{herb}%", f"%{herb}%"])
        if source:
            conditions.append("source_site LIKE ?")
            params.append(f"%{source}%")

        where = " AND ".join(conditions)
        total = conn.execute(
            f"SELECT COUNT(*) FROM herb_news WHERE {where}", params
        ).fetchone()[0]

        offset = (page - 1) * page_size
        rows = conn.execute(
            f"""SELECT id, title, url, pub_date, source_site, herb_names, regions,
                       is_processed, llm_events
                FROM herb_news WHERE {where}
                ORDER BY pub_date DESC, id DESC
                LIMIT ? OFFSET ?""",
            params + [page_size, offset]
        ).fetchall()
        conn.close()

        import json as _json
        return jsonify({
            "total": total,
            "page": page,
            "pageSize": page_size,
            "totalPages": (total + page_size - 1) // page_size,
            "news": [{
                "id": r["id"],
                "title": r["title"],
                "url": r["url"],
                "pubDate": r["pub_date"],
                "source": r["source_site"],
                "herbs": [h for h in r["herb_names"].split(",") if h],
                "regions": [rg for rg in r["regions"].split(",") if rg],
                "events": _json.loads(r["llm_events"]) if r["is_processed"] and r["llm_events"] else [],
            } for r in rows],
        })
    except Exception as e:
        return jsonify({"error": f"查询失败: {str(e)}"}), 500


@app.route("/api/news/events")
def api_news_events():
    """获取新闻解读出的市场事件（weather_events 中 LLM 来源的）

    Query params:
        herb: 药材名（可选）
        type: 事件类型（可选）
        days: 最近N天（默认90）
    """
    from datetime import date, timedelta
    herb = request.args.get("herb", "")
    event_type = request.args.get("type", "")
    days = int(request.args.get("days", 90))
    cutoff = (date.today() - timedelta(days=days)).isoformat()

    conn = get_connection()
    conditions = ["start_date >= ?", "detail LIKE '%[LLM%'"]
    params: list = [cutoff]
    if herb:
        conditions.append("affected_herbs LIKE ?")
        params.append(f"%{herb}%")
    if event_type:
        conditions.append("event_type = ?")
        params.append(event_type)

    where = " AND ".join(conditions)
    rows = conn.execute(
        f"""SELECT event_type, start_date, severity, origin, affected_herbs,
                   detail, price_impact_pct
            FROM weather_events WHERE {where}
            ORDER BY start_date DESC, severity DESC
            LIMIT 200""",
        params
    ).fetchall()
    conn.close()

    return jsonify([{
        "eventType": r["event_type"],
        "date": r["start_date"],
        "severity": r["severity"],
        "region": r["origin"],
        "herb": r["affected_herbs"],
        "summary": r["detail"].replace("[LLM|llm_news] ", "").split(" | ")[0],
        "priceImpactPct": r["price_impact_pct"],
    } for r in rows])


# ── TCM 分析 API ─────────────────────────────────────────

@app.route("/api/tcm/symptoms")
def api_tcm_symptoms():
    """获取所有病症列表及基本统计"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT fs.symptom,
               COUNT(DISTINCT f.id) as formula_count
        FROM tcm_formula_symptoms fs
        JOIN tcm_formulas f ON f.id = fs.formula_id
        GROUP BY fs.symptom
        HAVING formula_count >= 1
        ORDER BY formula_count DESC
    """).fetchall()
    conn.close()
    return jsonify([{
        "symptom": r["symptom"],
        "formulaCount": r["formula_count"],
    } for r in rows])


@app.route("/api/tcm/symptom_cost")
def api_tcm_symptom_cost():
    """计算指定病症的治疗成本

    Query params:
        symptom: 病症名称（必填）
        page: 页码，从 1 开始（默认 1）
        page_size: 每页处方数（默认 20，最大 100）
        detail: 是否返回药材明细（默认 0，设为 1 返回完整 herbs）
    """
    symptom = request.args.get("symptom", "")
    if not symptom:
        return jsonify({"error": "缺少 symptom 参数"}), 400

    page = max(1, int(request.args.get("page", 1)))
    page_size = min(100, max(1, int(request.args.get("page_size", 20))))
    show_detail = request.args.get("detail", "0") == "1"

    from tcm_analyzer import calculate_formula_cost, get_latest_prices

    conn = get_connection()
    prices = get_latest_prices()

    # 获取该病症的所有处方
    formula_rows = conn.execute("""
        SELECT DISTINCT f.id, f.name, f.source, f.category
        FROM tcm_formulas f
        JOIN tcm_formula_symptoms fs ON fs.formula_id = f.id
        WHERE fs.symptom = ?
        ORDER BY f.source, f.name
    """, (symptom,)).fetchall()

    # 批量计算所有处方成本（复用 conn 和 prices）
    formulas = []
    costs = []
    for fr in formula_rows:
        cost = calculate_formula_cost(formula_id=fr["id"], conn=conn, prices=prices)
        if cost and cost["total_cost_single"] > 0:
            formulas.append(cost)
            costs.append(cost["total_cost_single"])

    conn.close()

    if not costs:
        return jsonify({
            "symptom": symptom,
            "formulaCount": 0,
            "formulas": [],
        })

    # 按单剂成本排序
    formulas.sort(key=lambda x: x["total_cost_single"])

    # 统计
    costs_sorted = sorted(costs)
    n = len(costs_sorted)
    median = costs_sorted[n // 2] if n % 2 == 1 else (
        costs_sorted[n // 2 - 1] + costs_sorted[n // 2]) / 2
    trim = max(1, n // 10)
    trimmed = costs_sorted[trim:-trim] if n > 5 else costs_sorted
    trimmed_avg = sum(trimmed) / len(trimmed) if trimmed else median

    # 全量费用分布（用于柱状图，仅首页返回）
    cost_distribution = None
    if page == 1:
        buckets = [
            {"label": "0-5元", "min": 0, "max": 5},
            {"label": "5-10元", "min": 5, "max": 10},
            {"label": "10-20元", "min": 10, "max": 20},
            {"label": "20-50元", "min": 20, "max": 50},
            {"label": "50-100元", "min": 50, "max": 100},
            {"label": "100-200元", "min": 100, "max": 200},
            {"label": "200-500元", "min": 200, "max": 500},
            {"label": "500+元", "min": 500, "max": float("inf")},
        ]
        for b in buckets:
            b["count"] = sum(1 for c in costs if b["min"] <= c < b["max"])
        cost_distribution = [{"label": b["label"], "count": b["count"]} for b in buckets]

    # 分页
    total = len(formulas)
    total_pages = (total + page_size - 1) // page_size
    start = (page - 1) * page_size
    end = start + page_size
    page_formulas = formulas[start:end]

    # 构建处方列表（根据 detail 参数决定是否包含药材明细）
    formula_list = []
    for f in page_formulas:
        item = {
            "name": f["name"],
            "source": f["source"],
            "category": f.get("category", ""),
            "costSingle": f["total_cost_single"],
            "costCourse": f["total_cost_course"],
            "herbCount": f["herb_count"],
            "matchRate": f["match_rate"],
            "symptoms": f["symptoms"],
        }
        if show_detail:
            item["herbs"] = [{
                "name": h["name"],
                "dosageG": h["dosage_g"],
                "pricePerKg": h["price_per_kg"],
                "cost": h["cost"],
                "hasPrice": h["has_price"],
            } for h in f["herbs"]]
        formula_list.append(item)

    result = {
        "symptom": symptom,
        "formulaCount": total,
        "page": page,
        "pageSize": page_size,
        "totalPages": total_pages,
        "stats": {
            "median_single": round(median, 2),
            "avg_single": round(trimmed_avg, 2),
            "min_single": round(min(costs), 2),
            "max_single": round(max(costs), 2),
            "median_course": round(median * 7, 2),
            "avg_course": round(trimmed_avg * 7, 2),
        },
        "formulas": formula_list,
    }
    if cost_distribution is not None:
        result["costDistribution"] = cost_distribution
    return jsonify(result)


@app.route("/api/tcm/overview")
def api_tcm_overview():
    """TCM 分析总览数据"""
    from tcm_analyzer import get_latest_prices

    conn = get_connection()
    prices = get_latest_prices()

    formula_count = conn.execute(
        "SELECT COUNT(*) as cnt FROM tcm_formulas"
    ).fetchone()["cnt"]
    herb_count = conn.execute(
        "SELECT COUNT(DISTINCT herb_name) FROM tcm_formula_herbs"
    ).fetchone()[0]
    symptom_count = conn.execute(
        "SELECT COUNT(DISTINCT symptom) FROM tcm_formula_symptoms"
    ).fetchone()[0]
    source_stats = conn.execute("""
        SELECT source, COUNT(*) as cnt
        FROM tcm_formulas GROUP BY source ORDER BY cnt DESC
    """).fetchall()

    # Top 20 高频病症
    top_symptoms = conn.execute("""
        SELECT symptom, COUNT(*) as cnt
        FROM tcm_formula_symptoms
        GROUP BY symptom ORDER BY cnt DESC LIMIT 20
    """).fetchall()

    # Top 20 高频药材
    top_herbs = conn.execute("""
        SELECT herb_name, COUNT(*) as cnt
        FROM tcm_formula_herbs
        GROUP BY herb_name ORDER BY cnt DESC LIMIT 20
    """).fetchall()

    conn.close()

    return jsonify({
        "formulaCount": formula_count,
        "herbCount": herb_count,
        "symptomCount": symptom_count,
        "priceMatchCount": len(prices),
        "sources": [{"name": s["source"], "count": s["cnt"]} for s in source_stats],
        "topSymptoms": [{"name": s["symptom"], "count": s["cnt"]} for s in top_symptoms],
        "topHerbs": [{
            "name": h["herb_name"],
            "count": h["cnt"],
            "price": prices.get(h["herb_name"], {}).get("price"),
        } for h in top_herbs],
    })


# ── 产地信息 API ─────────────────────────────────────────

@app.route("/api/origins")
def api_origins():
    """获取指定药材的产地信息（含产量）

    Query params:
        name: 药材名称（必填）
    """
    name = request.args.get("name", "")
    if not name:
        return jsonify({"error": "缺少 name 参数"}), 400

    conn = get_connection()
    rows = conn.execute("""
        SELECT herb_name, origin, is_daodi, province, description, source,
               annual_output_tons, planting_area_mu, output_percent, data_year
        FROM herb_origins
        WHERE herb_name = ?
        ORDER BY annual_output_tons DESC NULLS LAST, is_daodi DESC, province, origin
    """, (name,)).fetchall()
    conn.close()

    if not rows:
        return jsonify({"name": name, "origins": []})

    origins = []
    for r in rows:
        item = {
            "origin": r["origin"],
            "isDaodi": bool(r["is_daodi"]),
            "province": r["province"],
            "description": r["description"],
            "source": r["source"],
        }
        if r["annual_output_tons"] is not None:
            item["annualOutputTons"] = r["annual_output_tons"]
        if r["planting_area_mu"] is not None:
            item["plantingAreaMu"] = r["planting_area_mu"]
        if r["output_percent"] is not None:
            item["outputPercent"] = r["output_percent"]
        if r["data_year"] is not None:
            item["dataYear"] = r["data_year"]
        origins.append(item)

    return jsonify({
        "name": name,
        "originCount": len(origins),
        "origins": origins,
    })


@app.route("/api/origins/province")
def api_origins_by_province():
    """按省份统计产地药材分布

    Query params:
        province: 省份名（可选，不传则返回所有省份统计）
    """
    conn = get_connection()
    province = request.args.get("province", "")

    if province:
        rows = conn.execute("""
            SELECT herb_name, origin, is_daodi, description,
                   annual_output_tons, planting_area_mu, output_percent, data_year
            FROM herb_origins
            WHERE province = ?
            ORDER BY annual_output_tons DESC NULLS LAST, is_daodi DESC, herb_name
        """, (province,)).fetchall()
        conn.close()

        items = []
        for r in rows:
            item = {
                "herbName": r["herb_name"],
                "origin": r["origin"],
                "isDaodi": bool(r["is_daodi"]),
                "description": r["description"],
            }
            if r["annual_output_tons"] is not None:
                item["annualOutputTons"] = r["annual_output_tons"]
            if r["planting_area_mu"] is not None:
                item["plantingAreaMu"] = r["planting_area_mu"]
            if r["output_percent"] is not None:
                item["outputPercent"] = r["output_percent"]
            if r["data_year"] is not None:
                item["dataYear"] = r["data_year"]
            items.append(item)

        return jsonify({
            "province": province,
            "herbCount": len(set(r["herb_name"] for r in rows)),
            "origins": items,
        })
    else:
        rows = conn.execute("""
            SELECT province, COUNT(DISTINCT herb_name) as herb_count,
                   COUNT(*) as record_count,
                   SUM(is_daodi) as daodi_count,
                   SUM(annual_output_tons) as total_output
            FROM herb_origins
            WHERE province != ''
            GROUP BY province
            ORDER BY herb_count DESC
        """).fetchall()
        conn.close()
        return jsonify([{
            "province": r["province"],
            "herbCount": r["herb_count"],
            "recordCount": r["record_count"],
            "daodiCount": r["daodi_count"] or 0,
            "totalOutputTons": r["total_output"],
        } for r in rows])


@app.route("/api/origins/daodi")
def api_origins_daodi():
    """获取所有道地药材产区列表（含产量）"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT herb_name, origin, province, description,
               annual_output_tons, output_percent, data_year
        FROM herb_origins
        WHERE is_daodi = 1
        ORDER BY annual_output_tons DESC NULLS LAST, province, herb_name
    """).fetchall()
    conn.close()
    return jsonify([{
        "herbName": r["herb_name"],
        "origin": r["origin"],
        "province": r["province"],
        "description": r["description"],
        "annualOutputTons": r["annual_output_tons"],
        "outputPercent": r["output_percent"],
        "dataYear": r["data_year"],
    } for r in rows])


@app.route("/api/origins/production")
def api_origins_production():
    """获取有产量数据的品种列表

    Query params:
        sort: 排序方式 tons(默认) | percent | herb
    """
    sort = request.args.get("sort", "tons")
    conn = get_connection()

    order = "annual_output_tons DESC"
    if sort == "percent":
        order = "output_percent DESC"
    elif sort == "herb":
        order = "herb_name ASC"

    rows = conn.execute(f"""
        SELECT herb_name, origin, province, description, is_daodi,
               annual_output_tons, planting_area_mu, output_percent, data_year
        FROM herb_origins
        WHERE annual_output_tons IS NOT NULL
        ORDER BY {order}
    """).fetchall()
    conn.close()

    return jsonify({
        "count": len(rows),
        "data": [{
            "herbName": r["herb_name"],
            "origin": r["origin"],
            "province": r["province"],
            "isDaodi": bool(r["is_daodi"]),
            "description": r["description"],
            "annualOutputTons": r["annual_output_tons"],
            "plantingAreaMu": r["planting_area_mu"],
            "outputPercent": r["output_percent"],
            "dataYear": r["data_year"],
        } for r in rows],
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)

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
    """计算指定病症的治疗成本"""
    symptom = request.args.get("symptom", "")
    if not symptom:
        return jsonify({"error": "缺少 symptom 参数"}), 400

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

    conn.close()

    formulas = []
    costs = []
    for fr in formula_rows:
        cost = calculate_formula_cost(formula_id=fr["id"])
        if cost and cost["total_cost_single"] > 0:
            formulas.append(cost)
            costs.append(cost["total_cost_single"])

    if not costs:
        return jsonify({
            "symptom": symptom,
            "formulaCount": 0,
            "formulas": [],
        })

    # 用中位数代替均值（减少极端值影响）
    costs_sorted = sorted(costs)
    n = len(costs_sorted)
    median = costs_sorted[n // 2] if n % 2 == 1 else (
        costs_sorted[n // 2 - 1] + costs_sorted[n // 2]) / 2

    # 排除极端值后计算均值（去掉最高和最低 10%）
    trim = max(1, n // 10)
    trimmed = costs_sorted[trim:-trim] if n > 5 else costs_sorted
    trimmed_avg = sum(trimmed) / len(trimmed) if trimmed else median

    return jsonify({
        "symptom": symptom,
        "formulaCount": len(formulas),
        "stats": {
            "median_single": round(median, 2),
            "avg_single": round(trimmed_avg, 2),
            "min_single": round(min(costs), 2),
            "max_single": round(max(costs), 2),
            "median_course": round(median * 7, 2),
            "avg_course": round(trimmed_avg * 7, 2),
        },
        "formulas": [{
            "name": f["name"],
            "source": f["source"],
            "category": f.get("category", ""),
            "costSingle": f["total_cost_single"],
            "costCourse": f["total_cost_course"],
            "herbCount": f["herb_count"],
            "matchRate": f["match_rate"],
            "symptoms": f["symptoms"],
            "herbs": [{
                "name": h["name"],
                "dosageG": h["dosage_g"],
                "pricePerKg": h["price_per_kg"],
                "cost": h["cost"],
                "hasPrice": h["has_price"],
            } for h in f["herbs"]],
        } for f in sorted(formulas, key=lambda x: x["total_cost_single"])],
    })


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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)

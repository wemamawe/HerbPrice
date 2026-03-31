"""药材价格趋势图 Web 应用"""

from flask import Flask, jsonify, request, send_from_directory
from db import get_connection
from forecast import forecast_variety, forecast_variety_timesfm, forecast_variety_ensemble

app = Flask(__name__, static_folder="static")

# 预测结果缓存 {cache_key: {result, timestamp}}
_forecast_cache: dict = {}


@app.route("/")
def index():
    return send_from_directory("static", "index.html")


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
    """预测指定品种未来半年的价格趋势

    Query params:
        name    (必填) 品种名称
        engine  (选填) classic | timesfm | ensemble（默认 ensemble）
    """
    import time
    name = request.args.get("name", "")
    engine = request.args.get("engine", "ensemble").lower()

    if not name:
        return jsonify({"error": "缺少 name 参数"}), 400
    if engine not in ("classic", "timesfm", "ensemble"):
        return jsonify({"error": "engine 参数无效，可选: classic / timesfm / ensemble"}), 400

    # 缓存键包含引擎类型；classic 缓存 6h，timesfm/ensemble 缓存 12h
    cache_ttl = 6 * 3600 if engine == "classic" else 12 * 3600
    cache_key = f"{name}:{engine}"
    cached = _forecast_cache.get(cache_key)
    if cached and (time.time() - cached["timestamp"]) < cache_ttl:
        return jsonify(cached["result"])

    try:
        if engine == "classic":
            result = forecast_variety(name)
        elif engine == "timesfm":
            result = forecast_variety_timesfm(name)
        else:
            result = forecast_variety_ensemble(name)

        result["engine"] = engine
        _forecast_cache[cache_key] = {"result": result, "timestamp": time.time()}
        return jsonify(result)

    except ValueError as e:
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        import traceback
        return jsonify({"error": f"预测失败: {str(e)}", "detail": traceback.format_exc()}), 500


@app.route("/api/model_status")
def api_model_status():
    """查询 TimesFM 模型加载状态"""
    try:
        from forecast_timesfm import is_model_loaded
        loaded = is_model_loaded()
    except Exception:
        loaded = False
    return jsonify({"timesfm_loaded": loaded})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)

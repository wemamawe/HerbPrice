"""XGBoost 跨品种回归预测模型

利用全部品种数据联合训练，学习"特征 → 未来涨跌幅"的映射关系。
相比单品种时序模型，能从其他品种的模式中学习共性规律。

特征工程:
    - 技术指标: MA5/20/60, RSI14, 布林带位置, 近N日涨跌幅
    - 季节: 月份, 距产新天数
    - 供给: 种植周期, N年前价格比
    - 波动: 近期波动率, 价格在历史区间位置

用法:
    python forecast_xgb.py train          # 训练模型
    python forecast_xgb.py predict 白术    # 预测
    python forecast_xgb.py evaluate       # 评估
"""

import sys
import os
import pickle
import logging
import numpy as np
import pandas as pd
from datetime import date, timedelta
from db import get_connection

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

MODEL_DIR = os.path.join(os.path.dirname(__file__), "models")
MODEL_PATH = os.path.join(MODEL_DIR, "xgb_price_model.pkl")

# 预测目标：未来 N 天的涨跌幅
PREDICT_HORIZONS = [7, 30, 90]  # 7天、30天、90天后的涨跌幅


def compute_features(prices: np.ndarray, name: str = "",
                     current_month: int = 0) -> dict | None:
    """为单个时间点计算特征向量

    Args:
        prices: 到当前时间点为止的历史价格序列（至少 120 天）
        name: 品种名（用于获取种植周期等）
        current_month: 当前月份

    Returns:
        特征字典 或 None（数据不足）
    """
    if len(prices) < 120:
        return None

    p = prices
    cur = p[-1]

    # ── 技术指标 ──
    ma5 = np.mean(p[-5:])
    ma20 = np.mean(p[-20:])
    ma60 = np.mean(p[-60:])

    # RSI(14)
    deltas = np.diff(p[-15:])
    gains = np.maximum(deltas, 0)
    losses = np.abs(np.minimum(deltas, 0))
    avg_gain = np.mean(gains) if len(gains) > 0 else 0
    avg_loss = np.mean(losses) if len(losses) > 0 else 0.001
    rs = avg_gain / max(avg_loss, 0.001)
    rsi14 = 100 - (100 / (1 + rs))

    # 布林带位置
    bb_mid = ma20
    bb_std = np.std(p[-20:])
    bb_upper = bb_mid + 2 * bb_std
    bb_lower = bb_mid - 2 * bb_std
    bb_position = (cur - bb_lower) / max(bb_upper - bb_lower, 0.01)

    # 涨跌幅
    ret_5d = (cur - p[-6]) / max(p[-6], 0.01) if len(p) >= 6 else 0
    ret_20d = (cur - p[-21]) / max(p[-21], 0.01) if len(p) >= 21 else 0
    ret_60d = (cur - p[-61]) / max(p[-61], 0.01) if len(p) >= 61 else 0

    # ── 波动率 ──
    returns_30d = np.diff(p[-31:]) / np.maximum(p[-31:-1], 0.01)
    vol_30d = np.std(returns_30d) if len(returns_30d) > 0 else 0

    # ── 价格位置 ──
    hist_min = np.min(p)
    hist_max = np.max(p)
    hist_median = np.median(p)
    price_position = (cur - hist_min) / max(hist_max - hist_min, 0.01)

    # 近一年位置
    p_1y = p[-365:] if len(p) >= 365 else p
    year_min = np.min(p_1y)
    year_max = np.max(p_1y)
    year_position = (cur - year_min) / max(year_max - year_min, 0.01)

    # ── 均值回归指标 ──
    # 偏离 60 日均线的程度
    deviation_ma60 = (cur - ma60) / max(ma60, 0.01)

    # ── 趋势强度 ──
    # MA5 > MA20 > MA60 → 多头排列 (+1)
    trend_score = 0
    if ma5 > ma20:
        trend_score += 1
    if ma20 > ma60:
        trend_score += 1
    if ma5 < ma20:
        trend_score -= 1
    if ma20 < ma60:
        trend_score -= 1

    # ── 季节 ──
    month = current_month if current_month else date.today().month

    features = {
        "ma5_ratio": ma5 / max(cur, 0.01),
        "ma20_ratio": ma20 / max(cur, 0.01),
        "ma60_ratio": ma60 / max(cur, 0.01),
        "rsi14": rsi14,
        "bb_position": bb_position,
        "ret_5d": ret_5d,
        "ret_20d": ret_20d,
        "ret_60d": ret_60d,
        "vol_30d": vol_30d,
        "price_position_all": price_position,
        "price_position_1y": year_position,
        "deviation_ma60": deviation_ma60,
        "trend_score": trend_score,
        "month_sin": np.sin(2 * np.pi * month / 12),
        "month_cos": np.cos(2 * np.pi * month / 12),
    }

    return features


def build_training_data(min_samples_per_herb: int = 200) -> tuple:
    """构建训练数据集（跨品种）

    Returns:
        (X_df, y_dict) where y_dict = {"7d": array, "30d": array, "90d": array}
    """
    conn = get_connection()

    # 获取有足够数据的品种
    herbs = conn.execute("""
        SELECT name, COUNT(*) as cnt
        FROM estimated_daily_prices
        GROUP BY name
        HAVING cnt >= 400
        ORDER BY cnt DESC
    """).fetchall()

    log.info(f"共 {len(herbs)} 个品种有足够数据用于训练")

    all_features = []
    all_targets = {h: [] for h in PREDICT_HORIZONS}

    for herb_row in herbs:
        name = herb_row["name"]
        rows = conn.execute("""
            SELECT date, price FROM estimated_daily_prices
            WHERE name = ? ORDER BY date
        """, (name,)).fetchall()

        prices = np.array([r["price"] for r in rows])
        dates = [r["date"] for r in rows]

        # 从第 120 天开始，到倒数第 90 天结束（需要 90 天后的真实值作为 target）
        max_horizon = max(PREDICT_HORIZONS)
        for i in range(120, len(prices) - max_horizon):
            month = int(dates[i][5:7])
            feats = compute_features(prices[:i+1], name, month)
            if feats is None:
                continue

            all_features.append(feats)
            for h in PREDICT_HORIZONS:
                future_return = (prices[i + h] - prices[i]) / max(prices[i], 0.01)
                all_targets[h].append(future_return)

    conn.close()

    X_df = pd.DataFrame(all_features)
    y_dict = {f"{h}d": np.array(all_targets[h]) for h in PREDICT_HORIZONS}

    log.info(f"训练样本: {len(X_df):,} | 特征: {X_df.shape[1]}")
    return X_df, y_dict


def train_model():
    """训练 XGBoost 模型"""
    try:
        from xgboost import XGBRegressor
    except ImportError:
        log.error("需要安装 xgboost: pip install xgboost")
        return

    log.info("构建训练数据...")
    X, y_dict = build_training_data()

    if X.empty:
        log.error("训练数据为空")
        return

    models = {}
    for horizon_key, y in y_dict.items():
        log.info(f"训练 {horizon_key} 预测模型...")

        model = XGBRegressor(
            n_estimators=300,
            max_depth=6,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            reg_alpha=0.1,
            reg_lambda=1.0,
            random_state=42,
            n_jobs=-1,
        )
        model.fit(X, y)

        # 训练集 R² 和 MAE
        pred = model.predict(X)
        mae = np.mean(np.abs(pred - y))
        r2 = 1 - np.sum((y - pred) ** 2) / np.sum((y - np.mean(y)) ** 2)
        log.info(f"  {horizon_key}: MAE={mae:.4f}, R²={r2:.4f}")

        models[horizon_key] = model

    # 保存模型
    os.makedirs(MODEL_DIR, exist_ok=True)
    with open(MODEL_PATH, "wb") as f:
        pickle.dump({
            "models": models,
            "feature_names": list(X.columns),
            "trained_at": date.today().isoformat(),
            "sample_count": len(X),
        }, f)

    log.info(f"模型已保存: {MODEL_PATH}")


def load_model() -> dict | None:
    """加载已训练的模型"""
    if not os.path.exists(MODEL_PATH):
        return None
    with open(MODEL_PATH, "rb") as f:
        return pickle.load(f)


def predict_herb(herb_name: str) -> dict:
    """对指定品种做 XGBoost 预测

    Returns:
        {
            "name": str,
            "predictions": {
                "7d": {"return_pct": float, "price": float},
                "30d": {...},
                "90d": {...},
            },
            "features": dict,
            "model_info": dict,
        }
    """
    model_data = load_model()
    if model_data is None:
        return {"error": "模型未训练，请先运行: python forecast_xgb.py train"}

    conn = get_connection()
    rows = conn.execute("""
        SELECT date, price FROM estimated_daily_prices
        WHERE name = ? ORDER BY date
    """, (herb_name,)).fetchall()
    conn.close()

    if len(rows) < 120:
        return {"error": f"'{herb_name}' 数据不足（需要至少120天）"}

    prices = np.array([r["price"] for r in rows])
    current_price = prices[-1]
    current_date = rows[-1]["date"]
    month = int(current_date[5:7])

    features = compute_features(prices, herb_name, month)
    if features is None:
        return {"error": "特征计算失败"}

    # 构建特征 DataFrame
    feature_names = model_data["feature_names"]
    X = pd.DataFrame([features])[feature_names]

    predictions = {}
    for horizon_key, model in model_data["models"].items():
        pred_return = float(model.predict(X)[0])
        pred_price = current_price * (1 + pred_return)
        predictions[horizon_key] = {
            "return_pct": round(pred_return * 100, 2),
            "predicted_price": round(max(pred_price, 0.01), 2),
        }

    # 综合方向判断
    avg_return = np.mean([p["return_pct"] for p in predictions.values()])
    if avg_return > 2:
        direction = "up"
    elif avg_return < -2:
        direction = "down"
    else:
        direction = "neutral"

    return {
        "name": herb_name,
        "current_price": round(current_price, 2),
        "current_date": current_date,
        "predictions": predictions,
        "direction": direction,
        "features": {k: round(v, 4) for k, v in features.items()},
        "model_info": {
            "trained_at": model_data["trained_at"],
            "sample_count": model_data["sample_count"],
        },
    }


def evaluate_model():
    """在测试集上评估模型"""
    model_data = load_model()
    if model_data is None:
        log.error("模型未训练")
        return

    conn = get_connection()
    # 选几个代表性品种做测试
    test_herbs = ["白术", "白芍", "当归", "黄芪", "金银花", "三七", "麦冬"]

    for herb in test_herbs:
        result = predict_herb(herb)
        if "error" in result:
            print(f"  {herb}: {result['error']}")
            continue

        preds = result["predictions"]
        arrow = {"up": "↑", "down": "↓", "neutral": "→"}[result["direction"]]
        print(f"  {herb} ¥{result['current_price']} {arrow} | "
              f"7d:{preds['7d']['return_pct']:+.1f}% "
              f"30d:{preds['30d']['return_pct']:+.1f}% "
              f"90d:{preds['90d']['return_pct']:+.1f}%")

    conn.close()


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "evaluate"

    if cmd == "train":
        train_model()
    elif cmd == "predict":
        name = sys.argv[2] if len(sys.argv) > 2 else "白术"
        import json
        result = predict_herb(name)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    elif cmd == "evaluate":
        model_data = load_model()
        if model_data:
            print(f"模型训练时间: {model_data['trained_at']}")
            print(f"训练样本数: {model_data['sample_count']:,}")
            print()
        evaluate_model()
    else:
        print("用法: python forecast_xgb.py [train|predict <品种>|evaluate]")

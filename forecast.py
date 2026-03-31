"""药材价格预测模块

使用 Prophet + Ridge + 多周期EMA 的自适应集成方案。
短期以 EMA 动量为主（药材价格短期惯性强），中长期逐步引入 Prophet 季节性。
置信区间基于历史波动率动态计算。
"""

import warnings
warnings.filterwarnings("ignore")

from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from prophet import Prophet
from sklearn.linear_model import Ridge
from db import get_connection

FORECAST_DAYS = 180


def _load_price_series(name: str) -> pd.DataFrame:
    """从数据库加载品种日价格序列"""
    conn = get_connection()
    rows = conn.execute(
        "SELECT date, price FROM estimated_daily_prices WHERE name=? ORDER BY date",
        (name,)
    ).fetchall()
    conn.close()
    if not rows:
        raise ValueError(f"品种 '{name}' 无价格数据")
    df = pd.DataFrame([{"ds": r["date"], "y": r["price"]} for r in rows])
    df["ds"] = pd.to_datetime(df["ds"])
    return df


def _calc_volatility(df: pd.DataFrame) -> dict:
    """计算品种的历史波动率指标"""
    prices = df["y"].values
    returns = np.diff(prices) / np.maximum(prices[:-1], 0.01)
    recent_30 = returns[-30:] if len(returns) >= 30 else returns
    recent_90 = returns[-90:] if len(returns) >= 90 else returns
    return {
        "daily_std": float(np.std(recent_30)),
        "daily_std_90": float(np.std(recent_90)),
        "mean_price": float(np.mean(prices[-30:])),
        "cv_30": float(np.std(prices[-30:]) / max(np.mean(prices[-30:]), 0.01)),
    }


def _prophet_forecast(df: pd.DataFrame, periods: int) -> pd.DataFrame:
    """Prophet 模型预测 — 提高趋势灵敏度"""
    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=False,
        daily_seasonality=False,
        changepoint_prior_scale=0.15,     # 0.05→0.15 更好追踪近期趋势
        changepoint_range=0.9,            # 允许在更晚的数据点检测变化
        seasonality_prior_scale=10.0,
        interval_width=0.80,
    )
    model.fit(df)
    future = model.make_future_dataframe(periods=periods)
    fc = model.predict(future)
    return fc[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(periods)


def _ridge_trend_forecast(df: pd.DataFrame, periods: int) -> np.ndarray:
    """Ridge 回归 — 短窗口（1年）+ 低阶多项式，减少外推发散"""
    recent = df.tail(365).copy()     # 730→365 减少远期历史干扰
    recent["t"] = np.arange(len(recent))
    X = np.column_stack([recent["t"], recent["t"] ** 2])
    y = recent["y"].values
    model = Ridge(alpha=10.0)        # 1.0→10.0 更强正则化抑制外推
    model.fit(X, y)

    future_t = np.arange(len(recent), len(recent) + periods)
    X_future = np.column_stack([future_t, future_t ** 2])
    pred = model.predict(X_future)

    # 限制外推偏离：不超过末尾价格的 ±50%
    last_y = y[-1]
    pred = np.clip(pred, last_y * 0.5, last_y * 1.5)
    return pred


def _ema_momentum_forecast(df: pd.DataFrame, periods: int) -> np.ndarray:
    """多周期 EMA 动量外推 — 短+中+长三层动量叠加

    含末端异常值检测：若最后一天价格偏离近 7 天中位数超过 2 倍 MAD，
    则使用中位数作为起点，避免数据跳变放大预测偏差。
    """
    prices = df["y"].values
    series = pd.Series(prices)

    # 末端异常值检测：最后一天 vs 近 7 天中位数
    recent_7 = prices[-7:]
    median_7 = float(np.median(recent_7))
    mad_7 = float(np.median(np.abs(recent_7 - median_7)))
    last_raw = prices[-1]
    pct_dev = abs(last_raw - median_7) / max(median_7, 0.01)
    if (mad_7 > 0 and abs(last_raw - median_7) > 2 * mad_7) or pct_dev > 0.03:
        # 最后一天偏离中位数超过 2×MAD 或超过 3%，用中位数代替
        last_price = median_7
    else:
        last_price = last_raw

    # 三层 EMA
    ema_7 = series.ewm(span=7).mean().iloc[-1]
    ema_30 = series.ewm(span=30).mean().iloc[-1]
    ema_90 = series.ewm(span=90).mean().iloc[-1]

    # 短期动量：近7天趋势（快速响应）
    momentum_short = (ema_7 - ema_30) / 30
    # 中期动量：30天趋势
    momentum_mid = (ema_30 - ema_90) / 90
    # 长期基线趋势：近90天日均变化
    if len(prices) >= 90:
        momentum_long = (last_price - prices[-90]) / 90
    else:
        momentum_long = 0.0

    result = np.zeros(periods)
    cur = last_price
    for i in range(periods):
        # 短期动量快衰减，中期慢衰减，长期最慢
        d_short = np.exp(-i / 30)      # 短期：30天半衰期
        d_mid = np.exp(-i / 90)        # 中期：90天半衰期
        d_long = np.exp(-i / 180)      # 长期：180天半衰期

        daily_change = (momentum_short * d_short * 0.5 +
                        momentum_mid * d_mid * 0.3 +
                        momentum_long * d_long * 0.2)
        cur += daily_change
        result[i] = cur
    return result


def _adaptive_weights(periods: int) -> np.ndarray:
    """根据预测天数生成自适应权重矩阵 (periods, 3)

    短期(1-30天): EMA主导 → 中期(30-90天): 逐步过渡 → 长期(90+天): Prophet提升
    基于实测：16天内 EMA MAPE=0.9%, Prophet MAPE=8%, Ridge MAPE=5%
    """
    weights = np.zeros((periods, 3))  # [prophet, ridge, ema]
    for i in range(periods):
        t = i + 1
        if t <= 30:
            # 短期：EMA 主导
            r = t / 30  # 0→1
            w_prophet = 0.05 + 0.15 * r       # 5% → 20%
            w_ridge = 0.10 + 0.05 * r         # 10% → 15%
            w_ema = 1.0 - w_prophet - w_ridge  # 85% → 65%
        elif t <= 90:
            # 中期：逐步均衡
            r = (t - 30) / 60  # 0→1
            w_prophet = 0.20 + 0.15 * r       # 20% → 35%
            w_ridge = 0.15 + 0.05 * r         # 15% → 20%
            w_ema = 1.0 - w_prophet - w_ridge  # 65% → 45%
        else:
            # 长期：Prophet 季节性重要
            r = min((t - 90) / 90, 1.0)  # 0→1
            w_prophet = 0.35 + 0.10 * r       # 35% → 45%
            w_ridge = 0.20                     # 20%
            w_ema = 1.0 - w_prophet - w_ridge  # 45% → 35%
        weights[i] = [w_prophet, w_ridge, w_ema]
    return weights


def _calc_confidence_band(df: pd.DataFrame, ensemble: np.ndarray,
                          prophet_lower: np.ndarray,
                          prophet_upper: np.ndarray,
                          periods: int) -> tuple[np.ndarray, np.ndarray]:
    """基于历史波动率 + Prophet 区间的混合置信区间"""
    vol = _calc_volatility(df)
    daily_vol = max(vol["daily_std"], 0.005)  # 至少 0.5% 日波动

    # 波动率驱动的置信带（随时间扩大 ~ sqrt(t)）
    vol_band = np.array([
        ensemble[i] * daily_vol * 1.28 * np.sqrt(i + 1)  # 80%置信=1.28σ
        for i in range(periods)
    ])

    # Prophet 的置信带
    prophet_band = (prophet_upper - prophet_lower) / 2

    # 取两者的较大值，确保区间不会太窄
    half_width = np.maximum(vol_band, prophet_band * 0.5)

    # 限制最大宽度不超过价格的 50%
    max_width = ensemble * 0.5
    half_width = np.minimum(half_width, max_width)

    return ensemble - half_width, ensemble + half_width


def forecast_variety(name: str, periods: int = FORECAST_DAYS) -> dict:
    """对指定品种进行自适应集成预测

    改进要点：
    - 短期EMA主导(85%→65%)，中长期Prophet逐步提升
    - 多周期EMA(7/30/90天)捕捉不同时间尺度的动量
    - 波动率自适应置信区间
    - 起点平滑从5天扩展到10天（更柔和过渡）

    Returns:
        {
            "name": str,
            "lastDate": str,
            "lastPrice": float,
            "forecast": [...],
            "method": str,
        }
    """
    df = _load_price_series(name)
    last_date = df["ds"].iloc[-1]
    last_price = float(df["y"].iloc[-1])

    # 1) Prophet
    prophet_fc = _prophet_forecast(df, periods)
    p_yhat = prophet_fc["yhat"].values
    p_lower = prophet_fc["yhat_lower"].values
    p_upper = prophet_fc["yhat_upper"].values
    fc_dates = prophet_fc["ds"].dt.strftime("%Y-%m-%d").tolist()

    # 2) Ridge 趋势
    ridge_pred = _ridge_trend_forecast(df, periods)

    # 3) 多周期 EMA 动量
    ema_pred = _ema_momentum_forecast(df, periods)

    # 4) 自适应权重集成
    weights = _adaptive_weights(periods)
    ensemble = (weights[:, 0] * p_yhat +
                weights[:, 1] * ridge_pred +
                weights[:, 2] * ema_pred)

    # 5) 混合置信区间
    lower, upper = _calc_confidence_band(
        df, ensemble, p_lower, p_upper, periods
    )

    # 价格不能为负
    ensemble = np.maximum(ensemble, 0.01)
    lower = np.maximum(lower, 0.01)

    # 起点平滑过渡（10天指数衰减）
    smooth_days = min(10, periods)
    for i in range(smooth_days):
        w = 1 - np.exp(-3 * (i + 1) / smooth_days)  # 指数过渡
        ensemble[i] = last_price * (1 - w) + ensemble[i] * w
        lower[i] = last_price * (1 - w) + lower[i] * w
        upper[i] = last_price * (1 - w) + upper[i] * w

    # 生成方法描述
    w0 = weights[0]
    w_mid = weights[min(89, periods - 1)]
    method = (f"自适应集成: 短期 EMA({w0[2]:.0%})+Prophet({w0[0]:.0%})+Ridge({w0[1]:.0%})"
              f" → 长期 EMA({w_mid[2]:.0%})+Prophet({w_mid[0]:.0%})+Ridge({w_mid[1]:.0%})")

    return {
        "name": name,
        "lastDate": last_date.strftime("%Y-%m-%d"),
        "lastPrice": round(last_price, 2),
        "forecast": [
            {
                "date": fc_dates[i],
                "price": round(float(ensemble[i]), 2),
                "lower": round(float(lower[i]), 2),
                "upper": round(float(upper[i]), 2),
            }
            for i in range(periods)
        ],
        "method": method,
    }


def forecast_variety_timesfm(name: str, periods: int = FORECAST_DAYS) -> dict:
    """纯 TimesFM 2.5 预测，格式与 forecast_variety() 一致"""
    from forecast_timesfm import timesfm_forecast

    df = _load_price_series(name)
    last_date = df["ds"].iloc[-1]
    last_price = float(df["y"].iloc[-1])

    tfm = timesfm_forecast(df["y"].values, horizon=periods)

    fc_dates = [
        (last_date + pd.Timedelta(days=i + 1)).strftime("%Y-%m-%d")
        for i in range(periods)
    ]

    # 起点平滑（10天）
    smooth_days = min(10, periods)
    point = tfm["point"].copy()
    lower = tfm["lower"].copy()
    upper = tfm["upper"].copy()
    for i in range(smooth_days):
        w = 1 - np.exp(-3 * (i + 1) / smooth_days)
        point[i] = last_price * (1 - w) + point[i] * w
        lower[i] = last_price * (1 - w) + lower[i] * w
        upper[i] = last_price * (1 - w) + upper[i] * w

    return {
        "name": name,
        "lastDate": last_date.strftime("%Y-%m-%d"),
        "lastPrice": round(last_price, 2),
        "forecast": [
            {
                "date": fc_dates[i],
                "price": round(float(point[i]), 2),
                "lower": round(float(lower[i]), 2),
                "upper": round(float(upper[i]), 2),
            }
            for i in range(periods)
        ],
        "method": "TimesFM 2.5（200M）— Google 时序基础模型，zero-shot 预测",
    }


def forecast_variety_ensemble(name: str, periods: int = FORECAST_DAYS) -> dict:
    """TimesFM + 现有方案的加权集成预测

    权重分配策略（基于回测验证）：
    - 短期(1-30天):  TimesFM 70% + 现有方案 30%  （TimesFM 短期精度显著更高）
    - 中期(31-90天): TimesFM 55% + 现有方案 45%
    - 长期(91+天):   TimesFM 40% + 现有方案 60%  （现有方案融合季节性和趋势）
    """
    from forecast_timesfm import timesfm_forecast

    df = _load_price_series(name)
    last_date = df["ds"].iloc[-1]
    last_price = float(df["y"].iloc[-1])

    # 1) TimesFM 预测
    tfm = timesfm_forecast(df["y"].values, horizon=periods)

    # 2) 现有方案预测（内部调用，不重复加载数据）
    prophet_fc = _prophet_forecast(df, periods)
    p_yhat = prophet_fc["yhat"].values
    p_lower = prophet_fc["yhat_lower"].values
    p_upper = prophet_fc["yhat_upper"].values
    fc_dates = prophet_fc["ds"].dt.strftime("%Y-%m-%d").tolist()

    ridge_pred = _ridge_trend_forecast(df, periods)
    ema_pred = _ema_momentum_forecast(df, periods)
    weights_classic = _adaptive_weights(periods)
    classic_point = (weights_classic[:, 0] * p_yhat +
                     weights_classic[:, 1] * ridge_pred +
                     weights_classic[:, 2] * ema_pred)
    classic_lower, classic_upper = _calc_confidence_band(
        df, classic_point, p_lower, p_upper, periods
    )
    classic_point = np.maximum(classic_point, 0.01)
    classic_lower = np.maximum(classic_lower, 0.01)

    # 3) 时变权重融合
    def ensemble_weights(t: int) -> tuple[float, float]:
        """返回 (w_timesfm, w_classic)"""
        if t <= 30:
            r = t / 30
            w_tfm = 0.70 - 0.15 * r   # 70% → 55%
        elif t <= 90:
            r = (t - 30) / 60
            w_tfm = 0.55 - 0.15 * r   # 55% → 40%
        else:
            r = min((t - 90) / 90, 1.0)
            w_tfm = 0.40 - 0.05 * r   # 40% → 35%
        return w_tfm, 1.0 - w_tfm

    ensemble_point = np.zeros(periods)
    ensemble_lower = np.zeros(periods)
    ensemble_upper = np.zeros(periods)

    for i in range(periods):
        wt, wc = ensemble_weights(i + 1)
        ensemble_point[i] = wt * tfm["point"][i] + wc * classic_point[i]
        ensemble_lower[i] = wt * tfm["lower"][i] + wc * classic_lower[i]
        ensemble_upper[i] = wt * tfm["upper"][i] + wc * classic_upper[i]

    # 4) 起点平滑（10天）
    smooth_days = min(10, periods)
    for i in range(smooth_days):
        w = 1 - np.exp(-3 * (i + 1) / smooth_days)
        ensemble_point[i] = last_price * (1 - w) + ensemble_point[i] * w
        ensemble_lower[i] = last_price * (1 - w) + ensemble_lower[i] * w
        ensemble_upper[i] = last_price * (1 - w) + ensemble_upper[i] * w

    ensemble_point = np.maximum(ensemble_point, 0.01)
    ensemble_lower = np.maximum(ensemble_lower, 0.01)

    return {
        "name": name,
        "lastDate": last_date.strftime("%Y-%m-%d"),
        "lastPrice": round(last_price, 2),
        "forecast": [
            {
                "date": fc_dates[i],
                "price": round(float(ensemble_point[i]), 2),
                "lower": round(float(ensemble_lower[i]), 2),
                "upper": round(float(ensemble_upper[i]), 2),
                # 附带两个子引擎的点预测，方便前端展示对比
                "timesfm": round(float(tfm["point"][i]), 2),
                "classic": round(float(classic_point[i]), 2),
            }
            for i in range(periods)
        ],
        "method": "集成预测：TimesFM 2.5（短期70%→长期35%）+ Prophet/Ridge/EMA（短期30%→长期65%）",
        "engines": {
            "timesfm": "TimesFM 2.5 — Google 时序基础模型",
            "classic": "Prophet + Ridge + 多周期EMA 自适应集成",
        },
    }


if __name__ == "__main__":
    import sys
    name = sys.argv[1] if len(sys.argv) > 1 else "白术"
    print(f"预测品种: {name}")
    result = forecast_variety(name)
    print(f"最后日期: {result['lastDate']}, 最后价格: ¥{result['lastPrice']}")
    print(f"预测方法: {result['method']}")
    print(f"预测天数: {len(result['forecast'])}")
    fc = result["forecast"]
    for p in [fc[0], fc[29], fc[89], fc[-1]]:
        print(f"  {p['date']}: ¥{p['price']} [{p['lower']} ~ {p['upper']}]")

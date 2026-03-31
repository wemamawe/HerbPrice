"""TimesFM 2.5 预测模块

单例惰性加载，避免每次请求重新初始化模型（首次加载约 10s）。
提供统一的 timesfm_forecast() 接口，与 forecast.py 数据格式对齐。
"""

import os
import warnings
warnings.filterwarnings("ignore")

import numpy as np

os.environ.setdefault("HF_ENDPOINT", "https://hf-mirror.com")

_model = None
_compile_horizon = None   # 记录上次 compile 的 horizon，避免重复 compile


def _get_model():
    global _model
    if _model is not None:
        return _model
    import timesfm
    _model = timesfm.TimesFM_2p5_200M_torch.from_pretrained(
        "google/timesfm-2.5-200m-pytorch"
    )
    return _model


def timesfm_forecast(prices: np.ndarray, horizon: int = 180) -> dict:
    """用 TimesFM 2.5 对价格序列做预测

    Args:
        prices:  历史价格 numpy 数组（float32/float64 均可）
        horizon: 预测天数，最大 1024

    Returns:
        {
            "point":  ndarray(horizon,)   — 点预测
            "lower":  ndarray(horizon,)   — ~10th 分位（悲观）
            "upper":  ndarray(horizon,)   — ~90th 分位（乐观）
        }
    """
    global _compile_horizon
    import timesfm

    model = _get_model()

    # 仅当 horizon 变化时重新 compile（compile 代价较高）
    if _compile_horizon != horizon:
        model.compile(
            timesfm.ForecastConfig(
                max_context=min(len(prices), 1024),
                max_horizon=horizon,
                normalize_inputs=True,
                use_continuous_quantile_head=True,
                force_flip_invariance=True,
                infer_is_positive=True,
                fix_quantile_crossing=True,
            )
        )
        _compile_horizon = horizon

    point_fc, quantile_fc = model.forecast(
        horizon=horizon,
        inputs=[prices.astype(np.float32)],
    )

    point = np.array(point_fc[0])[:horizon]
    lower = np.array(quantile_fc[0])[:horizon, 1]   # ~10th percentile
    upper = np.array(quantile_fc[0])[:horizon, -1]  # ~90th percentile

    # 价格不能为负
    point = np.maximum(point, 0.01)
    lower = np.maximum(lower, 0.01)
    upper = np.maximum(upper, 0.01)

    return {"point": point, "lower": lower, "upper": upper}


def is_model_loaded() -> bool:
    """是否已加载模型（用于健康检查）"""
    return _model is not None

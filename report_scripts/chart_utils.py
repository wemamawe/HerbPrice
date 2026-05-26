#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
chart_utils.py — 麦肯锡风格图表工具库 v1.0
提供 8 种预置图表类型，支持品牌色系切换，输出 ReportLab RLImage 对象。

使用方式：
    from chart_utils import ChartFactory, load_brand_colors

    colors = load_brand_colors("yoga")   # 或 mckinsey / accenture / goldman / minimal
    cf = ChartFactory(colors)

    img = cf.bar(data, title="市场份额", xlabel="公司", ylabel="市场份额(%)")
    img = cf.line(data, title="增长趋势")
    img = cf.waterfall(data, title="价值拆解")
    img = cf.radar(data, labels, title="能力矩阵")
    img = cf.bubble(data, title="市场矩阵")
    img = cf.heatmap(matrix, row_labels, col_labels, title="风险热力")
    img = cf.quadrant(items, title="优先级四象限")
    img = cf.pie(data, title="占比分布")
"""

import io
import struct
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import yaml
import os
from reportlab.lib.units import cm
from reportlab.platypus import Image as RLImage

# ─── 路径常量 ─────────────────────────────────────────────────────────────────
SKILL_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STYLE_CONFIG = os.path.join(SKILL_DIR, "assets", "style_config.yaml")
FONT_DIR     = os.path.join(SKILL_DIR, "assets", "fonts")

# ─── 品牌色加载 ───────────────────────────────────────────────────────────────

def load_brand_colors(brand_key: str = None) -> dict:
    """从 style_config.yaml 加载指定品牌色系。"""
    with open(STYLE_CONFIG, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    if brand_key is None:
        brand_key = cfg.get("active_brand", "yoga")

    presets = cfg.get("brand_presets", {})
    brand   = presets.get(brand_key, presets.get("yoga", {}))
    colors  = brand.get("colors", {})

    return {
        "primary":      colors.get("primary",      "#0A2240"),
        "secondary":    colors.get("secondary",     "#007B8A"),
        "accent":       colors.get("accent",        "#00A0B0"),
        "body_text":    colors.get("body_text",     "#4A4A4A"),
        "caption_text": colors.get("caption_text",  "#9B9B9B"),
        "light_bg":     colors.get("light_bg",      "#F5F9FA"),
        "border":       colors.get("border",        "#D0E4E8"),
    }


def _brand_palette(colors: dict, n: int = 6) -> list:
    """生成品牌渐变色列表（n 个颜色）。"""
    base_colors = [
        colors["secondary"],
        colors["primary"],
        colors["accent"],
        "#4A90A4",
        "#8AB8C2",
        "#C5DCE0",
    ]
    return (base_colors * ((n // len(base_colors)) + 1))[:n]


# ─── PNG 宽高读取（防图表变形）────────────────────────────────────────────────

def _fig_to_rl_image(fig, width_cm: float = 14) -> RLImage:
    """
    将 matplotlib figure 转为 ReportLab RLImage，从 PNG 文件头读取真实像素比。
    防止 bbox_inches='tight' 裁剪导致的图表变形。
    """
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(16)
    png_w = struct.unpack(">I", buf.read(4))[0]
    png_h = struct.unpack(">I", buf.read(4))[0]
    aspect = png_h / png_w
    buf.seek(0)
    w_pt = width_cm * cm
    h_pt = w_pt * aspect
    img = RLImage(buf, width=w_pt, height=h_pt)
    img.hAlign = "CENTER"
    return img


def _apply_mckinsey_style(ax, colors: dict):
    """应用麦肯锡简洁风格到坐标轴。"""
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(colors["border"])
    ax.spines["bottom"].set_color(colors["border"])
    ax.tick_params(colors=colors["body_text"], labelsize=8)
    ax.grid(axis="y", color=colors["border"], linestyle="--", linewidth=0.5, alpha=0.7)
    ax.set_facecolor(colors["light_bg"])


# ─── ChartFactory ─────────────────────────────────────────────────────────────

class ChartFactory:
    """麦肯锡风格图表工厂，输出 ReportLab RLImage 对象。"""

    def __init__(self, colors: dict = None, brand_key: str = None):
        if colors:
            self.colors = colors
        else:
            self.colors = load_brand_colors(brand_key)

    # ── 1. 柱状图 ─────────────────────────────────────────────────────────────
    def bar(self, data: dict, title: str = "", xlabel: str = "",
            ylabel: str = "", width_cm: float = 14, horizontal: bool = False) -> RLImage:
        """
        柱状图。
        data: {"标签1": 值1, "标签2": 值2, ...}
        """
        labels = list(data.keys())
        values = list(data.values())
        palette = _brand_palette(self.colors, len(labels))

        fig, ax = plt.subplots(figsize=(width_cm * 0.394, 4))
        fig.patch.set_facecolor("white")

        if horizontal:
            bars = ax.barh(labels, values, color=palette, height=0.6)
            ax.set_xlabel(ylabel, fontsize=8, color=self.colors["body_text"])
            for bar_obj, val in zip(bars, values):
                ax.text(val + max(values) * 0.01, bar_obj.get_y() + bar_obj.get_height() / 2,
                        f"{val:,.0f}", va="center", fontsize=7.5, color=self.colors["body_text"])
        else:
            bars = ax.bar(labels, values, color=palette, width=0.6)
            ax.set_xlabel(xlabel, fontsize=8, color=self.colors["body_text"])
            ax.set_ylabel(ylabel, fontsize=8, color=self.colors["body_text"])
            for bar_obj, val in zip(bars, values):
                ax.text(bar_obj.get_x() + bar_obj.get_width() / 2, val + max(values) * 0.01,
                        f"{val:,.0f}", ha="center", fontsize=7.5, color=self.colors["body_text"])

        _apply_mckinsey_style(ax, self.colors)
        if title:
            ax.set_title(title, fontsize=10, color=self.colors["primary"],
                         fontweight="bold", pad=10)
        fig.tight_layout()
        return _fig_to_rl_image(fig, width_cm)

    # ── 2. 折线图 ─────────────────────────────────────────────────────────────
    def line(self, data: dict, title: str = "", xlabel: str = "",
             ylabel: str = "", width_cm: float = 14) -> RLImage:
        """
        折线图（支持多条线）。
        data: {"系列1": [v1, v2, ...], ...} 或 {"x轴标签": 单值列表} 时传 x_labels
        支持: data = {"系列A": {"x": [...], "y": [...]}, ...}
        """
        fig, ax = plt.subplots(figsize=(width_cm * 0.394, 4))
        fig.patch.set_facecolor("white")
        palette = _brand_palette(self.colors, len(data))

        for idx, (series_name, series_data) in enumerate(data.items()):
            color = palette[idx % len(palette)]
            if isinstance(series_data, dict):
                x_vals = series_data.get("x", list(range(len(series_data.get("y", [])))))
                y_vals = series_data.get("y", [])
            else:
                x_vals = list(range(len(series_data)))
                y_vals = series_data
            ax.plot(x_vals, y_vals, color=color, marker="o", linewidth=2,
                    markersize=4, label=series_name)

        _apply_mckinsey_style(ax, self.colors)
        ax.set_xlabel(xlabel, fontsize=8, color=self.colors["body_text"])
        ax.set_ylabel(ylabel, fontsize=8, color=self.colors["body_text"])
        if len(data) > 1:
            ax.legend(fontsize=7.5, framealpha=0.8)
        if title:
            ax.set_title(title, fontsize=10, color=self.colors["primary"],
                         fontweight="bold", pad=10)
        fig.tight_layout()
        return _fig_to_rl_image(fig, width_cm)

    # ── 3. 饼图 ───────────────────────────────────────────────────────────────
    def pie(self, data: dict, title: str = "", width_cm: float = 10) -> RLImage:
        """饼图。data: {"标签": 值, ...}"""
        labels = list(data.keys())
        values = list(data.values())
        palette = _brand_palette(self.colors, len(labels))

        fig, ax = plt.subplots(figsize=(width_cm * 0.394, width_cm * 0.394 * 0.75))
        fig.patch.set_facecolor("white")

        wedges, texts, autotexts = ax.pie(
            values, labels=labels, colors=palette,
            autopct="%1.1f%%", startangle=90,
            textprops={"fontsize": 8, "color": self.colors["body_text"]},
            wedgeprops={"edgecolor": "white", "linewidth": 1.5}
        )
        for autotext in autotexts:
            autotext.set_color("white")
            autotext.set_fontsize(7.5)

        ax.set_facecolor("white")
        if title:
            ax.set_title(title, fontsize=10, color=self.colors["primary"],
                         fontweight="bold", pad=10)
        fig.tight_layout()
        return _fig_to_rl_image(fig, width_cm)

    # ── 4. 瀑布图（麦肯锡标志性）──────────────────────────────────────────────
    def waterfall(self, data: dict, title: str = "", ylabel: str = "",
                  width_cm: float = 14) -> RLImage:
        """
        瀑布图（价值拆解）。
        data: {"起始": 100, "+增长A": 30, "-损耗B": -20, "结果": None}
        若值为 None，自动计算累计值作为总计柱。
        """
        labels = list(data.keys())
        values_raw = list(data.values())

        # 计算累计
        running = 0
        bottoms = []
        heights = []
        is_total = []

        for i, (lbl, val) in enumerate(zip(labels, values_raw)):
            if val is None:  # 总计柱
                bottoms.append(0)
                heights.append(running)
                is_total.append(True)
            else:
                if val >= 0:
                    bottoms.append(running)
                else:
                    bottoms.append(running + val)
                heights.append(abs(val))
                running += val
                is_total.append(False)

        fig, ax = plt.subplots(figsize=(width_cm * 0.394, 4))
        fig.patch.set_facecolor("white")

        for i, (bottom, height, total_flag) in enumerate(zip(bottoms, heights, is_total)):
            if total_flag:
                color = self.colors["primary"]
            elif values_raw[i] is not None and values_raw[i] >= 0:
                color = self.colors["secondary"]
            else:
                color = "#E05A5A"

            bar = ax.bar(i, height, bottom=bottom, color=color, width=0.6,
                         edgecolor="white", linewidth=0.5)

            # 连接线
            if i > 0 and not is_total[i - 1]:
                prev_top = bottoms[i - 1] + heights[i - 1] if values_raw[i - 1] and values_raw[i - 1] >= 0 \
                    else bottoms[i - 1]
                ax.plot([i - 0.7, i - 0.3], [prev_top, prev_top],
                        color=self.colors["caption_text"], linewidth=0.8, linestyle="--")

            # 数值标注
            display_val = heights[i] if total_flag else values_raw[i]
            y_pos = bottom + height + max(heights) * 0.01
            prefix = "+" if (display_val and display_val > 0 and not total_flag) else ""
            ax.text(i, y_pos, f"{prefix}{display_val:,.0f}",
                    ha="center", fontsize=7.5, color=self.colors["body_text"])

        ax.set_xticks(range(len(labels)))
        ax.set_xticklabels(labels, rotation=15, ha="right", fontsize=8)
        _apply_mckinsey_style(ax, self.colors)
        ax.set_ylabel(ylabel, fontsize=8, color=self.colors["body_text"])
        if title:
            ax.set_title(title, fontsize=10, color=self.colors["primary"],
                         fontweight="bold", pad=10)
        fig.tight_layout()
        return _fig_to_rl_image(fig, width_cm)

    # ── 5. 雷达图 ─────────────────────────────────────────────────────────────
    def radar(self, data: dict, labels: list, title: str = "",
              width_cm: float = 10) -> RLImage:
        """
        雷达图（多维能力对比）。
        data: {"系列A": [v1,v2,...], "系列B": [v1,v2,...]}
        labels: 维度名称列表（与值列表等长）
        """
        N = len(labels)
        angles = np.linspace(0, 2 * np.pi, N, endpoint=False).tolist()
        angles += angles[:1]

        fig, ax = plt.subplots(figsize=(width_cm * 0.394, width_cm * 0.394),
                               subplot_kw=dict(polar=True))
        fig.patch.set_facecolor("white")
        ax.set_facecolor(self.colors["light_bg"])
        palette = _brand_palette(self.colors, len(data))

        for idx, (series_name, values) in enumerate(data.items()):
            vals = list(values) + [values[0]]
            color = palette[idx % len(palette)]
            ax.plot(angles, vals, color=color, linewidth=2, label=series_name)
            ax.fill(angles, vals, color=color, alpha=0.15)

        ax.set_xticks(angles[:-1])
        ax.set_xticklabels(labels, fontsize=8, color=self.colors["body_text"])
        ax.tick_params(colors=self.colors["caption_text"], labelsize=7)
        ax.grid(color=self.colors["border"], linestyle="--", linewidth=0.5)
        ax.spines["polar"].set_color(self.colors["border"])

        if len(data) > 1:
            ax.legend(loc="upper right", bbox_to_anchor=(1.3, 1.1), fontsize=7.5)
        if title:
            ax.set_title(title, fontsize=10, color=self.colors["primary"],
                         fontweight="bold", pad=15)
        fig.tight_layout()
        return _fig_to_rl_image(fig, width_cm)

    # ── 6. 气泡矩阵 ───────────────────────────────────────────────────────────
    def bubble(self, data: list, title: str = "",
               xlabel: str = "市场吸引力", ylabel: str = "竞争地位",
               width_cm: float = 13) -> RLImage:
        """
        气泡矩阵（市场吸引力 vs 竞争地位）。
        data: [{"name": "公司A", "x": 0.7, "y": 0.8, "size": 100, "color": None}, ...]
        x/y 范围 0-1，size 为气泡大小（相对值）
        """
        fig, ax = plt.subplots(figsize=(width_cm * 0.394, width_cm * 0.394 * 0.8))
        fig.patch.set_facecolor("white")
        ax.set_facecolor(self.colors["light_bg"])
        palette = _brand_palette(self.colors, len(data))

        for idx, item in enumerate(data):
            color = item.get("color") or palette[idx % len(palette)]
            size  = item.get("size", 100)
            ax.scatter(item["x"], item["y"], s=size, color=color, alpha=0.7,
                       edgecolors="white", linewidth=1.5)
            ax.annotate(item["name"],
                        xy=(item["x"], item["y"]),
                        xytext=(5, 5), textcoords="offset points",
                        fontsize=7.5, color=self.colors["body_text"])

        # 四象限分割线
        ax.axhline(0.5, color=self.colors["border"], linestyle="--", linewidth=0.8)
        ax.axvline(0.5, color=self.colors["border"], linestyle="--", linewidth=0.8)
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.set_xlabel(xlabel, fontsize=8, color=self.colors["body_text"])
        ax.set_ylabel(ylabel, fontsize=8, color=self.colors["body_text"])
        _apply_mckinsey_style(ax, self.colors)
        if title:
            ax.set_title(title, fontsize=10, color=self.colors["primary"],
                         fontweight="bold", pad=10)
        fig.tight_layout()
        return _fig_to_rl_image(fig, width_cm)

    # ── 7. 热力图（风险矩阵）─────────────────────────────────────────────────
    def heatmap(self, matrix: list, row_labels: list, col_labels: list,
                title: str = "", width_cm: float = 13) -> RLImage:
        """
        热力图。
        matrix: 二维列表 [[v11, v12...], [v21, v22...], ...]
        """
        data_np = np.array(matrix, dtype=float)
        fig, ax = plt.subplots(figsize=(width_cm * 0.394, len(row_labels) * 0.6 + 1.5))
        fig.patch.set_facecolor("white")

        # 使用品牌色系渐变
        from matplotlib.colors import LinearSegmentedColormap
        brand_cmap = LinearSegmentedColormap.from_list(
            "brand",
            [self.colors["light_bg"], self.colors["secondary"], self.colors["primary"]]
        )
        im = ax.imshow(data_np, cmap=brand_cmap, aspect="auto")

        ax.set_xticks(range(len(col_labels)))
        ax.set_yticks(range(len(row_labels)))
        ax.set_xticklabels(col_labels, fontsize=8, color=self.colors["body_text"], rotation=30, ha="right")
        ax.set_yticklabels(row_labels, fontsize=8, color=self.colors["body_text"])

        # 数值标注
        vmax = data_np.max() if data_np.max() != 0 else 1
        for i in range(len(row_labels)):
            for j in range(len(col_labels)):
                text_color = "white" if data_np[i, j] > vmax * 0.5 else self.colors["body_text"]
                ax.text(j, i, f"{data_np[i, j]:.1f}",
                        ha="center", va="center", fontsize=7.5, color=text_color)

        plt.colorbar(im, ax=ax, shrink=0.8)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        if title:
            ax.set_title(title, fontsize=10, color=self.colors["primary"],
                         fontweight="bold", pad=10)
        fig.tight_layout()
        return _fig_to_rl_image(fig, width_cm)

    # ── 8. 四象限优先级矩阵 ───────────────────────────────────────────────────
    def quadrant(self, items: list, title: str = "",
                 xlabel: str = "执行难度 (低 → 高)",
                 ylabel: str = "战略价值 (低 → 高)",
                 quadrant_labels: list = None,
                 width_cm: float = 13) -> RLImage:
        """
        四象限优先级矩阵。
        items: [{"name": "举措A", "x": 0.3, "y": 0.8, "priority": "high"}, ...]
        x/y 范围 0-1
        quadrant_labels: ["立即执行", "战略投入", "评估优化", "低优先级"]（顺序：左上/右上/左下/右下）
        """
        if quadrant_labels is None:
            quadrant_labels = ["立即执行", "战略投入", "评估优化", "低优先级"]

        fig, ax = plt.subplots(figsize=(width_cm * 0.394, width_cm * 0.394 * 0.85))
        fig.patch.set_facecolor("white")

        # 背景四象限着色
        alpha = 0.08
        ax.fill_between([0, 0.5], [0.5, 0.5], [1, 1], color=self.colors["secondary"], alpha=alpha)
        ax.fill_between([0.5, 1], [0.5, 0.5], [1, 1], color=self.colors["primary"], alpha=alpha)
        ax.fill_between([0, 0.5], [0, 0], [0.5, 0.5], color=self.colors["caption_text"], alpha=alpha)
        ax.fill_between([0.5, 1], [0, 0], [0.5, 0.5], color=self.colors["caption_text"], alpha=alpha * 0.5)

        # 象限标签
        quad_positions = [(0.25, 0.92), (0.75, 0.92), (0.25, 0.08), (0.75, 0.08)]
        for pos, label in zip(quad_positions, quadrant_labels):
            ax.text(pos[0], pos[1], label, ha="center", fontsize=7,
                    color=self.colors["caption_text"], style="italic",
                    transform=ax.transAxes)

        # 数据点
        priority_colors = {
            "high":   self.colors["primary"],
            "medium": self.colors["secondary"],
            "low":    self.colors["caption_text"],
        }
        for item in items:
            color = priority_colors.get(item.get("priority", "medium"), self.colors["secondary"])
            ax.scatter(item["x"], item["y"], s=80, color=color, zorder=5,
                       edgecolors="white", linewidth=1.5)
            ax.annotate(item["name"],
                        xy=(item["x"], item["y"]),
                        xytext=(6, 4), textcoords="offset points",
                        fontsize=7.5, color=self.colors["body_text"])

        # 分割线
        ax.axhline(0.5, color=self.colors["border"], linestyle="-", linewidth=1)
        ax.axvline(0.5, color=self.colors["border"], linestyle="-", linewidth=1)
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.set_xlabel(xlabel, fontsize=8, color=self.colors["body_text"])
        ax.set_ylabel(ylabel, fontsize=8, color=self.colors["body_text"])
        _apply_mckinsey_style(ax, self.colors)
        ax.grid(False)

        if title:
            ax.set_title(title, fontsize=10, color=self.colors["primary"],
                         fontweight="bold", pad=10)
        fig.tight_layout()
        return _fig_to_rl_image(fig, width_cm)

    # ── 自动推荐图表类型 ──────────────────────────────────────────────────────
    @staticmethod
    def recommend_chart_type(data_description: str) -> str:
        """
        根据数据描述关键词自动推荐图表类型。
        返回: 'bar' | 'line' | 'pie' | 'waterfall' | 'radar' | 'bubble' | 'heatmap' | 'quadrant'
        """
        desc_lower = data_description.lower()
        if any(k in desc_lower for k in ["趋势", "增长", "变化", "历年", "时间序列", "年均"]):
            return "line"
        elif any(k in desc_lower for k in ["占比", "份额", "分布", "比例", "构成"]):
            return "pie"
        elif any(k in desc_lower for k in ["拆解", "贡献", "瀑布", "来源于", "驱动"]):
            return "waterfall"
        elif any(k in desc_lower for k in ["能力", "维度", "综合评分", "多维", "雷达"]):
            return "radar"
        elif any(k in desc_lower for k in ["矩阵", "定位", "竞争地位", "市场吸引"]):
            return "bubble"
        elif any(k in desc_lower for k in ["风险", "热力", "评分矩阵", "相关性"]):
            return "heatmap"
        elif any(k in desc_lower for k in ["优先级", "四象限", "执行难度", "战略"]):
            return "quadrant"
        else:
            return "bar"  # 默认柱状图

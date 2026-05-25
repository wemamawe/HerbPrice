"""气象数据采集模块 - 自动关联药材产区

数据源: Open-Meteo (完全免费，无需API Key)
- 历史气象: https://archive-api.open-meteo.com/v1/archive
- 天气预报: https://api.open-meteo.com/v1/forecast

功能:
1. 获取各产区的历史/实时气象数据（降雨量、温度、干旱指数）
2. 自动检测气象异常（干旱、洪涝、极端高温/低温）
3. 将气象异常转化为价格影响因子

使用:
    from weather_monitor import WeatherMonitor
    monitor = WeatherMonitor()
    alerts = monitor.check_herb_alerts("当归")
"""

import requests
import time
import logging
from datetime import date, datetime, timedelta
from db import get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

OPEN_METEO_ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"
OPEN_METEO_FORECAST = "https://api.open-meteo.com/v1/forecast"

# 产区城市经纬度（复用 origin_crawler 中的坐标）
CITY_COORDS = {
    "岷县": (34.43, 104.04), "陇西": (35.00, 104.63), "渭源": (35.13, 104.21),
    "亳州": (33.84, 115.78), "磐安": (29.05, 120.45), "文山": (23.37, 104.24),
    "中江": (31.03, 104.68), "菏泽": (35.23, 115.48), "平邑": (35.51, 117.64),
    "都江堰": (30.99, 103.62), "三台": (31.09, 105.09), "江油": (31.78, 104.75),
    "焦作": (35.22, 113.24), "南阳": (33.00, 112.53), "中卫": (37.50, 105.20),
    "罗平": (24.88, 104.31), "抚松": (42.34, 127.45), "文登": (37.19, 122.05),
    "永福": (24.98, 109.98), "新会": (22.46, 113.03), "阳春": (22.17, 111.79),
    "玉林": (22.65, 110.15), "东阿": (36.33, 116.25), "石柱": (30.00, 108.11),
    "利川": (30.29, 108.94), "施秉": (27.03, 108.12), "赤峰": (42.26, 118.89),
    "围场": (41.94, 117.76), "罗田": (30.78, 115.40), "邵东": (27.26, 111.74),
    "靖宇": (42.39, 126.81), "西和": (34.01, 105.30), "龙山": (29.46, 109.44),
    "兰州": (36.06, 103.83), "大理": (25.59, 100.23), "恩施": (30.30, 109.49),
}

# 气象异常阈值
THRESHOLDS = {
    "drought": {
        "desc": "干旱",
        "condition": "30天累计降雨量 < 历史同期50%",
        "rain_30d_ratio": 0.5,  # 低于历史同期50%
    },
    "flood": {
        "desc": "洪涝",
        "condition": "7天累计降雨量 > 历史同期200%",
        "rain_7d_ratio": 2.0,  # 高于历史同期200%
    },
    "heat_wave": {
        "desc": "高温热害",
        "condition": "连续5天最高温度 > 35℃",
        "max_temp": 35,
        "consecutive_days": 5,
    },
    "frost": {
        "desc": "霜冻",
        "condition": "最低温度 < 0℃（生长季）",
        "min_temp": 0,
    },
}


class WeatherMonitor:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "HerbPrice/1.0"})

    def fetch_weather(self, lat: float, lon: float,
                      start_date: str, end_date: str) -> dict | None:
        """获取指定位置的历史气象数据

        Returns:
            {
                "dates": [...],
                "temp_max": [...],
                "temp_min": [...],
                "temp_mean": [...],
                "precipitation": [...],  # 日降雨量mm
                "et0": [...],  # 参考蒸散量
            }
        """
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date,
            "end_date": end_date,
            "daily": "temperature_2m_max,temperature_2m_min,temperature_2m_mean,"
                     "precipitation_sum,et0_fao_evapotranspiration",
            "timezone": "Asia/Shanghai",
        }
        try:
            resp = self.session.get(OPEN_METEO_ARCHIVE, params=params, timeout=15)
            data = resp.json()
            if "daily" not in data:
                return None
            daily = data["daily"]
            return {
                "dates": daily["time"],
                "temp_max": daily["temperature_2m_max"],
                "temp_min": daily["temperature_2m_min"],
                "temp_mean": daily["temperature_2m_mean"],
                "precipitation": daily["precipitation_sum"],
                "et0": daily.get("et0_fao_evapotranspiration", []),
            }
        except Exception as e:
            log.warning("获取气象数据失败 (%.2f, %.2f): %s", lat, lon, e)
            return None

    def fetch_forecast(self, lat: float, lon: float, days: int = 7) -> dict | None:
        """获取未来天气预报"""
        params = {
            "latitude": lat,
            "longitude": lon,
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
            "timezone": "Asia/Shanghai",
            "forecast_days": days,
        }
        try:
            resp = self.session.get(OPEN_METEO_FORECAST, params=params, timeout=15)
            data = resp.json()
            if "daily" not in data:
                return None
            daily = data["daily"]
            return {
                "dates": daily["time"],
                "temp_max": daily["temperature_2m_max"],
                "temp_min": daily["temperature_2m_min"],
                "precipitation": daily["precipitation_sum"],
            }
        except Exception as e:
            log.warning("获取天气预报失败: %s", e)
            return None

    def detect_anomalies(self, weather: dict, historical_avg: dict | None = None) -> list[dict]:
        """检测气象异常事件

        Args:
            weather: fetch_weather 返回的数据
            historical_avg: 历史同期平均值 {"rain_30d_avg": float, "temp_avg": float}

        Returns: [{"type": "drought", "severity": 0.7, "detail": "..."}]
        """
        alerts = []
        precip = weather["precipitation"]
        temp_max = weather["temp_max"]
        temp_min = weather["temp_min"]

        # 过滤 None 值
        precip = [p if p is not None else 0 for p in precip]
        temp_max = [t for t in temp_max if t is not None]
        temp_min = [t for t in temp_min if t is not None]

        # 1) 干旱检测：近30天降雨
        rain_30d = sum(precip[-30:]) if len(precip) >= 30 else sum(precip)
        if historical_avg and historical_avg.get("rain_30d_avg"):
            ratio = rain_30d / max(historical_avg["rain_30d_avg"], 1)
            if ratio < 0.5:
                severity = min(1.0, (0.5 - ratio) / 0.3)
                alerts.append({
                    "type": "drought",
                    "severity": round(severity, 2),
                    "detail": f"近30天降雨{rain_30d:.1f}mm，仅为历史同期{ratio*100:.0f}%",
                    "rain_30d": rain_30d,
                    "ratio": round(ratio, 2),
                })
        elif rain_30d < 20:  # 绝对值判断（30天<20mm即干旱）
            severity = min(1.0, (20 - rain_30d) / 15)
            alerts.append({
                "type": "drought",
                "severity": round(severity, 2),
                "detail": f"近30天降雨仅{rain_30d:.1f}mm，显著偏少",
                "rain_30d": rain_30d,
            })

        # 2) 洪涝检测：近7天降雨
        rain_7d = sum(precip[-7:]) if len(precip) >= 7 else sum(precip)
        if rain_7d > 150:  # 7天>150mm
            severity = min(1.0, (rain_7d - 150) / 200)
            alerts.append({
                "type": "flood",
                "severity": round(severity, 2),
                "detail": f"近7天降雨{rain_7d:.1f}mm，洪涝风险",
                "rain_7d": rain_7d,
            })

        # 3) 高温检测
        if temp_max:
            consecutive_hot = 0
            max_consecutive = 0
            for t in temp_max[-14:]:
                if t > 35:
                    consecutive_hot += 1
                    max_consecutive = max(max_consecutive, consecutive_hot)
                else:
                    consecutive_hot = 0
            if max_consecutive >= 5:
                severity = min(1.0, (max_consecutive - 5) / 5 + 0.5)
                alerts.append({
                    "type": "heat_wave",
                    "severity": round(severity, 2),
                    "detail": f"连续{max_consecutive}天最高温超35℃",
                })

        # 4) 霜冻检测（生长季4-10月）
        today = date.today()
        if 4 <= today.month <= 10 and temp_min:
            frost_days = sum(1 for t in temp_min[-7:] if t < 0)
            if frost_days > 0:
                severity = min(1.0, frost_days / 3)
                alerts.append({
                    "type": "frost",
                    "severity": round(severity, 2),
                    "detail": f"生长季出现{frost_days}天霜冻",
                })

        return alerts

    def check_herb_alerts(self, herb_name: str) -> dict:
        """检查指定药材所有产区的气象异常

        Returns:
            {
                "herb": str,
                "check_date": str,
                "alerts": [
                    {"origin": "岷县", "province": "甘肃", "anomalies": [...]}
                ],
                "overall_impact": float,  # 综合影响因子
                "summary": str,
            }
        """
        conn = get_connection()
        rows = conn.execute("""
            SELECT origin, province, output_percent
            FROM herb_origins
            WHERE herb_name = ? AND province != '进口'
                  AND annual_output_tons IS NOT NULL
            ORDER BY annual_output_tons DESC
        """, (herb_name,)).fetchall()
        conn.close()

        if not rows:
            return {"herb": herb_name, "alerts": [], "overall_impact": 1.0,
                    "summary": "无产区数据", "check_date": str(date.today())}

        today = date.today()
        start = (today - timedelta(days=30)).isoformat()
        end = today.isoformat()

        all_alerts = []
        total_impact = 0.0
        total_weight = 0.0

        for row in rows:
            origin = row["origin"]
            pct = row["output_percent"] or 10

            coord = CITY_COORDS.get(origin)
            if not coord:
                continue

            lat, lon = coord
            weather = self.fetch_weather(lat, lon, start, end)
            if not weather:
                continue

            anomalies = self.detect_anomalies(weather)
            if anomalies:
                all_alerts.append({
                    "origin": origin,
                    "province": row["province"],
                    "outputPercent": pct,
                    "anomalies": anomalies,
                })
                # 按产区占比加权计算影响
                for a in anomalies:
                    impact = self._anomaly_to_impact(a)
                    total_impact += impact * (pct / 100)
                    total_weight += pct / 100

            time.sleep(0.3)  # 限速

        overall = 1.0 + total_impact if total_weight > 0 else 1.0
        overall = max(0.8, min(1.4, overall))

        summary_parts = []
        for alert in all_alerts:
            for a in alert["anomalies"]:
                summary_parts.append(f"{alert['origin']}({alert['province']}): {a['detail']}")

        return {
            "herb": herb_name,
            "check_date": str(today),
            "alerts": all_alerts,
            "overall_impact": round(overall, 4),
            "summary": "; ".join(summary_parts) if summary_parts else "各产区气象正常",
        }

    def _anomaly_to_impact(self, anomaly: dict) -> float:
        """将异常转换为价格影响百分比"""
        t = anomaly["type"]
        s = anomaly["severity"]
        if t == "drought":
            return 0.05 + 0.20 * s  # 5%~25% 上涨
        elif t == "flood":
            return 0.08 + 0.25 * s  # 8%~33% 上涨
        elif t == "heat_wave":
            return 0.03 + 0.12 * s  # 3%~15% 上涨
        elif t == "frost":
            return 0.05 + 0.15 * s  # 5%~20% 上涨
        return 0.0

    def get_production_area_weather_summary(self, herb_name: str) -> dict:
        """获取药材各产区的近期天气概况（用于前端展示）"""
        conn = get_connection()
        rows = conn.execute("""
            SELECT origin, province, annual_output_tons, output_percent
            FROM herb_origins
            WHERE herb_name = ? AND province != '进口'
                  AND annual_output_tons IS NOT NULL
            ORDER BY annual_output_tons DESC LIMIT 5
        """, (herb_name,)).fetchall()
        conn.close()

        today = date.today()
        start = (today - timedelta(days=7)).isoformat()
        end = today.isoformat()

        areas = []
        for row in rows:
            origin = row["origin"]
            coord = CITY_COORDS.get(origin)
            if not coord:
                continue

            lat, lon = coord
            weather = self.fetch_weather(lat, lon, start, end)
            if not weather:
                continue

            precip = [p for p in weather["precipitation"] if p is not None]
            temps = [t for t in weather["temp_mean"] if t is not None]

            areas.append({
                "origin": origin,
                "province": row["province"],
                "outputPercent": row["output_percent"],
                "rain7d": round(sum(precip), 1),
                "tempAvg": round(sum(temps) / len(temps), 1) if temps else None,
                "tempMax": max(weather["temp_max"]) if weather["temp_max"] else None,
                "tempMin": min(weather["temp_min"]) if weather["temp_min"] else None,
            })
            time.sleep(0.3)

        return {
            "herb": herb_name,
            "period": f"{start} ~ {end}",
            "areas": areas,
        }


# ═══════════════════════════════════════════════════════════════════════
# 新闻/产地快讯采集（中药材天地网产地快讯）
# ═══════════════════════════════════════════════════════════════════════

class NewsMonitor:
    """从中药材天地网产地快讯自动识别灾害和种植变动信息"""

    NEWS_URL = "https://www.zyctd.com/hqInfo--9.html"
    KEYWORDS_DISASTER = ["干旱", "旱情", "洪涝", "水灾", "暴雨", "冰雹",
                         "霜冻", "冻害", "病虫害", "虫害", "减产", "绝收"]
    KEYWORDS_PLANTING = ["扩种", "增种", "新种", "种植面积增加", "大面积种植",
                         "缩种", "弃种", "改种", "面积减少", "种植积极性不高"]

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36"
        })

    def fetch_news(self, keyword: str = "", page: int = 1) -> list[dict]:
        """从中药材天地网获取产地快讯"""
        try:
            from bs4 import BeautifulSoup
            url = f"https://www.zyctd.com/hqInfo--9--{page}.html"
            if keyword:
                url = f"https://www.zyctd.com/search.html?keyword={keyword}&type=news"

            resp = self.session.get(url, timeout=15)
            resp.encoding = "utf-8"
            soup = BeautifulSoup(resp.text, "lxml")

            news = []
            for item in soup.select(".news-list li, .search-list li, .list-item"):
                title_el = item.select_one("a")
                time_el = item.select_one(".time, .date, span")
                if title_el:
                    title = title_el.get_text(strip=True)
                    href = title_el.get("href", "")
                    pub_time = time_el.get_text(strip=True) if time_el else ""
                    news.append({
                        "title": title,
                        "url": href,
                        "time": pub_time,
                    })
            return news
        except Exception as e:
            log.warning("获取新闻失败: %s", e)
            return []

    def detect_events_from_news(self, herb_name: str) -> list[dict]:
        """从新闻中自动识别与指定药材相关的灾害/种植变动事件

        Returns: [{"type": "drought", "severity": 0.5, "source": "...", "title": "..."}]
        """
        events = []

        # 搜索该药材相关新闻
        news_list = self.fetch_news(keyword=herb_name)
        if not news_list:
            return events

        for news in news_list[:20]:
            title = news["title"]

            # 检测灾害关键词
            for kw in self.KEYWORDS_DISASTER:
                if kw in title and herb_name in title:
                    event_type = self._map_keyword_to_type(kw)
                    events.append({
                        "type": event_type,
                        "severity": 0.5,
                        "source": "zyctd_news",
                        "title": title,
                        "url": news.get("url", ""),
                        "time": news.get("time", ""),
                    })
                    break

            # 检测种植面积变动
            for kw in self.KEYWORDS_PLANTING:
                if kw in title and herb_name in title:
                    is_increase = kw in ["扩种", "增种", "新种", "种植面积增加", "大面积种植"]
                    events.append({
                        "type": "area_increase" if is_increase else "area_decrease",
                        "severity": 0.5,
                        "source": "zyctd_news",
                        "title": title,
                        "url": news.get("url", ""),
                        "time": news.get("time", ""),
                    })
                    break

        return events

    def _map_keyword_to_type(self, keyword: str) -> str:
        mapping = {
            "干旱": "drought", "旱情": "drought",
            "洪涝": "flood", "水灾": "flood", "暴雨": "flood",
            "冰雹": "hail",
            "霜冻": "frost", "冻害": "frost",
            "病虫害": "pest", "虫害": "pest",
            "减产": "drought", "绝收": "flood",
        }
        return mapping.get(keyword, "drought")


if __name__ == "__main__":
    import sys

    herb = sys.argv[1] if len(sys.argv) > 1 else "当归"
    print(f"检查 {herb} 产区气象状况...\n")

    monitor = WeatherMonitor()
    result = monitor.check_herb_alerts(herb)

    print(f"检查日期: {result['check_date']}")
    print(f"综合影响: {result['overall_impact']:.4f}")
    print(f"摘要: {result['summary']}")

    if result["alerts"]:
        print(f"\n异常预警:")
        for alert in result["alerts"]:
            print(f"  {alert['origin']}({alert['province']}) 占比{alert['outputPercent']}%:")
            for a in alert["anomalies"]:
                print(f"    ⚠️  {a['type']} (严重度{a['severity']:.0%}): {a['detail']}")
    else:
        print("\n✓ 各产区气象正常，无异常预警")

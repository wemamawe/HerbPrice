"""历史气象数据批量采集与异常事件检测

功能：
1. 拉取过去5年各核心产区的月度气象数据
2. 检测历史上的气象异常事件（干旱、洪涝、高温、霜冻）
3. 与同期价格波动进行关联分析
4. 存入数据库供预测模型使用

数据源: Open-Meteo Historical API (免费无限制)
"""

import requests
import time
import logging
from datetime import date, datetime
from db import get_connection, init_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

OPEN_METEO_ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"

# 核心产区坐标 (lat, lon)
CORE_ORIGINS = {
    "岷县": (34.43, 104.04, "甘肃", ["当归", "党参"]),
    "陇西": (35.00, 104.63, "甘肃", ["黄芪", "党参"]),
    "亳州": (33.84, 115.78, "安徽", ["白芍", "白术", "白芷", "牡丹皮"]),
    "磐安": (29.05, 120.45, "浙江", ["白术", "浙贝母", "延胡索"]),
    "文山": (23.37, 104.24, "云南", ["三七"]),
    "中江": (31.03, 104.68, "四川", ["白芍", "丹参"]),
    "菏泽": (35.23, 115.48, "山东", ["白芍", "牡丹皮"]),
    "平邑": (35.51, 117.64, "山东", ["金银花"]),
    "都江堰": (30.99, 103.62, "四川", ["川芎"]),
    "三台": (31.09, 105.09, "四川", ["麦冬"]),
    "焦作": (35.22, 113.24, "河南", ["地黄", "山药", "牛膝", "菊花"]),
    "中卫": (37.50, 105.20, "宁夏", ["枸杞子"]),
    "罗平": (24.88, 104.31, "云南", ["干姜"]),
    "抚松": (42.34, 127.45, "吉林", ["人参", "西洋参"]),
    "文登": (37.19, 122.05, "山东", ["西洋参"]),
    "永福": (24.98, 109.98, "广西", ["罗汉果"]),
    "新会": (22.46, 113.03, "广东", ["陈皮"]),
    "玉林": (22.65, 110.15, "广西", ["肉桂", "八角茴香"]),
    "东阿": (36.33, 116.25, "山东", ["阿胶"]),
    "利川": (30.29, 108.94, "湖北", ["黄连", "大黄"]),
    "石柱": (30.00, 108.11, "重庆", ["黄连"]),
    "赤峰": (42.26, 118.89, "内蒙古", ["黄芪", "防风", "甘草", "苍术"]),
    "南阳": (33.00, 112.53, "河南", ["山茱萸"]),
    "靖宇": (42.39, 126.81, "吉林", ["五味子", "人参"]),
    "罗田": (30.78, 115.40, "湖北", ["茯苓", "天麻"]),
    "邵东": (27.26, 111.74, "湖南", ["玉竹", "玄参", "百合"]),
    "龙山": (29.46, 109.44, "湖南", ["百合"]),
    "兰州": (36.06, 103.83, "甘肃", ["百合"]),
    "封丘": (35.04, 114.42, "河南", ["金银花"]),
    "渭源": (35.13, 104.21, "甘肃", ["党参", "黄芪", "当归"]),
}


def fetch_yearly_weather(lat: float, lon: float, year: int) -> dict | None:
    """获取指定位置一年的日气象数据"""
    start = f"{year}-01-01"
    end = f"{year}-12-31"
    # 如果是当年，截止到昨天
    today = date.today()
    if year == today.year:
        end = (today.replace(day=1) if today.day == 1
               else date(today.year, today.month, today.day - 1)).isoformat()

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start,
        "end_date": end,
        "daily": "temperature_2m_max,temperature_2m_min,temperature_2m_mean,"
                 "precipitation_sum",
        "timezone": "Asia/Shanghai",
    }
    try:
        resp = requests.get(OPEN_METEO_ARCHIVE, params=params, timeout=30)
        data = resp.json()
        if "daily" not in data:
            return None
        return data["daily"]
    except Exception as e:
        log.warning("获取 %d 年数据失败 (%.2f,%.2f): %s", year, lat, lon, e)
        return None


def aggregate_monthly(daily: dict) -> list[dict]:
    """将日数据聚合为月度数据"""
    from collections import defaultdict
    months = defaultdict(lambda: {
        "rain": [], "temp_avg": [], "temp_max": [], "temp_min": [],
        "hot_days": 0, "frost_days": 0, "rain_days": 0,
    })

    dates = daily["time"]
    precip = daily["precipitation_sum"]
    t_max = daily["temperature_2m_max"]
    t_min = daily["temperature_2m_min"]
    t_mean = daily["temperature_2m_mean"]

    for i, d in enumerate(dates):
        m = int(d[5:7])
        p = precip[i] if precip[i] is not None else 0
        tmax = t_max[i]
        tmin = t_min[i]
        tmean = t_mean[i]

        months[m]["rain"].append(p)
        if tmean is not None:
            months[m]["temp_avg"].append(tmean)
        if tmax is not None:
            months[m]["temp_max"].append(tmax)
            if tmax > 35:
                months[m]["hot_days"] += 1
        if tmin is not None:
            months[m]["temp_min"].append(tmin)
            if tmin < 0:
                months[m]["frost_days"] += 1
        if p > 0.1:
            months[m]["rain_days"] += 1

    result = []
    for m in sorted(months.keys()):
        data = months[m]
        result.append({
            "month": m,
            "rain_total": round(sum(data["rain"]), 1),
            "temp_avg": round(sum(data["temp_avg"]) / len(data["temp_avg"]), 1) if data["temp_avg"] else None,
            "temp_max": round(max(data["temp_max"]), 1) if data["temp_max"] else None,
            "temp_min": round(min(data["temp_min"]), 1) if data["temp_min"] else None,
            "rain_days": data["rain_days"],
            "hot_days": data["hot_days"],
            "frost_days": data["frost_days"],
        })
    return result


def detect_monthly_anomalies(origin: str, province: str, year: int,
                             monthly_data: list[dict],
                             historical_avgs: dict | None = None) -> list[dict]:
    """从月度数据中检测异常事件"""
    events = []

    for m in monthly_data:
        month = m["month"]

        # 干旱：月降雨 < 20mm（生长季4-10月）
        if 4 <= month <= 10 and m["rain_total"] < 15:
            severity = min(1.0, (15 - m["rain_total"]) / 12)
            events.append({
                "type": "drought",
                "start_date": f"{year}-{month:02d}-01",
                "severity": round(severity, 2),
                "detail": f"{month}月降雨仅{m['rain_total']}mm，严重偏少",
            })

        # 洪涝：月降雨 > 300mm
        if m["rain_total"] > 300:
            severity = min(1.0, (m["rain_total"] - 300) / 200)
            events.append({
                "type": "flood",
                "start_date": f"{year}-{month:02d}-01",
                "severity": round(severity, 2),
                "detail": f"{month}月降雨{m['rain_total']}mm，洪涝风险",
            })

        # 高温：月内>35℃超过10天
        if m["hot_days"] >= 10:
            severity = min(1.0, (m["hot_days"] - 10) / 10 + 0.5)
            events.append({
                "type": "heat_wave",
                "start_date": f"{year}-{month:02d}-01",
                "severity": round(severity, 2),
                "detail": f"{month}月有{m['hot_days']}天超35℃",
            })

        # 霜冻（生长季4-10月出现）
        if 4 <= month <= 10 and m["frost_days"] > 0:
            severity = min(1.0, m["frost_days"] / 5)
            events.append({
                "type": "frost",
                "start_date": f"{year}-{month:02d}-01",
                "severity": round(severity, 2),
                "detail": f"{month}月生长季出现{m['frost_days']}天霜冻",
            })

    return events


def calc_price_impact(herb_name: str, event_date: str) -> float | None:
    """计算事件发生后的实际价格变动百分比"""
    conn = get_connection()
    # 事件发生时的价格
    row_before = conn.execute("""
        SELECT price FROM estimated_daily_prices
        WHERE name = ? AND date <= ? ORDER BY date DESC LIMIT 1
    """, (herb_name, event_date)).fetchone()

    if not row_before:
        conn.close()
        return None

    # 事件后3个月的价格
    event_dt = datetime.strptime(event_date, "%Y-%m-%d")
    after_date = (event_dt + __import__("datetime").timedelta(days=90)).strftime("%Y-%m-%d")
    row_after = conn.execute("""
        SELECT price FROM estimated_daily_prices
        WHERE name = ? AND date >= ? ORDER BY date ASC LIMIT 1
    """, (herb_name, after_date)).fetchone()

    conn.close()

    if not row_after:
        return None

    change_pct = (row_after["price"] - row_before["price"]) / row_before["price"] * 100
    return round(change_pct, 2)


def backfill_weather_data(years: int = 5):
    """批量拉取历史气象数据并存入数据库"""
    init_db()
    conn = get_connection()

    # 添加新表（如果不存在）
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS weather_monthly (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            origin TEXT NOT NULL, year INTEGER NOT NULL, month INTEGER NOT NULL,
            lat REAL NOT NULL, lon REAL NOT NULL,
            rain_total REAL, temp_avg REAL, temp_max REAL, temp_min REAL,
            rain_days INTEGER, hot_days INTEGER, frost_days INTEGER,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(origin, year, month)
        );
        CREATE TABLE IF NOT EXISTS weather_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            origin TEXT NOT NULL, province TEXT NOT NULL DEFAULT '',
            event_type TEXT NOT NULL, start_date TEXT NOT NULL, end_date TEXT,
            severity REAL NOT NULL DEFAULT 0.5, detail TEXT NOT NULL DEFAULT '',
            affected_herbs TEXT NOT NULL DEFAULT '', price_impact_pct REAL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
    """)
    conn.commit()

    current_year = date.today().year
    start_year = current_year - years

    total_origins = len(CORE_ORIGINS)
    total_requests = 0
    total_events = 0

    for idx, (origin, (lat, lon, province, herbs)) in enumerate(CORE_ORIGINS.items()):
        log.info("[%d/%d] %s (%s) - %s", idx + 1, total_origins, origin, province,
                 ", ".join(herbs))

        for year in range(start_year, current_year + 1):
            # 检查是否已有数据
            existing = conn.execute(
                "SELECT COUNT(*) as cnt FROM weather_monthly WHERE origin=? AND year=?",
                (origin, year)
            ).fetchone()["cnt"]

            if existing >= 12 and year < current_year:
                continue  # 历史完整年跳过

            daily = fetch_yearly_weather(lat, lon, year)
            total_requests += 1

            if not daily:
                log.warning("  %d年数据获取失败", year)
                continue

            monthly = aggregate_monthly(daily)

            # 存入月度数据
            for m in monthly:
                conn.execute("""
                    INSERT OR REPLACE INTO weather_monthly
                    (origin, year, month, lat, lon, rain_total, temp_avg,
                     temp_max, temp_min, rain_days, hot_days, frost_days)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (origin, year, m["month"], lat, lon,
                      m["rain_total"], m["temp_avg"], m["temp_max"],
                      m["temp_min"], m["rain_days"], m["hot_days"], m["frost_days"]))

            # 检测异常事件
            events = detect_monthly_anomalies(origin, province, year, monthly)
            for evt in events:
                # 计算对相关药材的实际价格影响
                for herb in herbs:
                    impact = calc_price_impact(herb, evt["start_date"])
                    conn.execute("""
                        INSERT OR IGNORE INTO weather_events
                        (origin, province, event_type, start_date, severity,
                         detail, affected_herbs, price_impact_pct)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (origin, province, evt["type"], evt["start_date"],
                          evt["severity"], evt["detail"], herb, impact))
                    total_events += 1

            conn.commit()
            time.sleep(0.5)  # 限速

    conn.close()
    log.info("完成! API请求: %d, 异常事件: %d", total_requests, total_events)


def show_stats():
    """显示历史气象数据统计"""
    conn = get_connection()

    monthly_count = conn.execute("SELECT COUNT(*) FROM weather_monthly").fetchone()[0]
    origins = conn.execute("SELECT COUNT(DISTINCT origin) FROM weather_monthly").fetchone()[0]
    events_count = conn.execute("SELECT COUNT(*) FROM weather_events").fetchone()[0]

    print(f"\n{'='*60}")
    print(f"  历史气象数据统计")
    print(f"{'='*60}")
    print(f"  月度气象记录: {monthly_count}")
    print(f"  覆盖产区: {origins}")
    print(f"  异常事件: {events_count}")
    print(f"{'='*60}\n")

    # 各类异常事件统计
    rows = conn.execute("""
        SELECT event_type, COUNT(*) as cnt,
               ROUND(AVG(severity), 2) as avg_sev,
               ROUND(AVG(price_impact_pct), 2) as avg_impact
        FROM weather_events
        GROUP BY event_type ORDER BY cnt DESC
    """).fetchall()
    if rows:
        print("异常事件类型统计:")
        print(f"  {'类型':<12} {'次数':>4} {'平均严重度':>8} {'平均价格影响%':>12}")
        print(f"  {'-'*40}")
        for r in rows:
            impact = f"{r['avg_impact']:+.1f}%" if r['avg_impact'] else "N/A"
            print(f"  {r['event_type']:<12} {r['cnt']:>4} {r['avg_sev']:>8.2f} {impact:>12}")

    # 最近的异常事件
    print(f"\n最近异常事件 (前10):")
    rows = conn.execute("""
        SELECT origin, province, event_type, start_date, severity,
               detail, affected_herbs, price_impact_pct
        FROM weather_events ORDER BY start_date DESC LIMIT 10
    """).fetchall()
    for r in rows:
        impact = f" → 价格{r['price_impact_pct']:+.1f}%" if r['price_impact_pct'] else ""
        print(f"  {r['start_date']} {r['origin']}({r['province']}) "
              f"{r['event_type']}(严重度{r['severity']:.0%}) "
              f"影响{r['affected_herbs']}{impact}")
        print(f"    {r['detail']}")

    conn.close()


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("用法:")
        print("  python weather_backfill.py run [years]  - 拉取N年历史数据(默认5)")
        print("  python weather_backfill.py stats        - 显示统计")
        sys.exit(0)

    cmd = sys.argv[1]
    if cmd == "run":
        years = int(sys.argv[2]) if len(sys.argv) > 2 else 5
        backfill_weather_data(years)
        show_stats()
    elif cmd == "stats":
        show_stats()
    else:
        print(f"未知命令: {cmd}")

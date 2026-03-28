"""康美中药网价格指数爬虫 + 历史价格估算

通过爬取指数站 13 年的指数数据，结合近一年实际价格拟合 K 值，
反推出每日平均价格并存入数据库。

核心公式：Index(t) = K × AvgPrice(t)
反推价格：AvgPrice(t) = Index(t) / K
"""

import requests
import re
import time
import json
import logging
import statistics
from datetime import datetime
from bs4 import BeautifulSoup

from db import (
    get_connection, init_db,
    upsert_index_variety, bulk_insert_daily_index,
    upsert_k_value, bulk_upsert_estimated_prices,
    insert_crawl_log,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

INDEX_BASE = "https://cnkmprice.kmzyw.com.cn"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Referer": f"{INDEX_BASE}/",
}
REQUEST_INTERVAL = 0.3

# 品种分类编码
CATEGORIES = {
    "A": "根及根茎类", "B": "果实种子类", "C": "全草类",
    "D": "花类/孢子类", "E": "叶类", "F": "皮类",
    "G": "茎木类", "H": "藤木/树脂类", "I": "菌藻类",
    "J": "动物类", "K": "矿物类", "L": "其他加工类",
}


class IndexCrawler:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # ── 品种列表 ─────────────────────────────────────────

    def fetch_variety_codes(self) -> dict[str, str]:
        """获取全部品种 code -> name 映射"""
        all_varieties: dict[str, str] = {}
        for prefix, cat_name in CATEGORIES.items():
            try:
                resp = self.session.get(
                    f"{INDEX_BASE}/bwindex.html",
                    params={"code": f"{prefix}-000"},
                    timeout=15,
                )
                resp.encoding = "utf-8"
                links = re.findall(
                    r'pzpage\.html\?code=([A-Z]-\d+)["\'][^>]*>([^<]+)',
                    resp.text,
                )
                for code, name in links:
                    all_varieties[code] = name.strip()
                count = len([c for c in all_varieties if c.startswith(f"{prefix}-")])
                log.info("%s (%s): %d 个品种", prefix, cat_name, count)
                time.sleep(REQUEST_INTERVAL)
            except Exception as e:
                log.warning("获取分类 %s 失败: %s", prefix, e)

        log.info("共获取 %d 个指数品种", len(all_varieties))
        return all_varieties

    # ── 指数数据 ─────────────────────────────────────────

    def fetch_index_data(self, code: str, exp_class: int = 2) -> list[tuple[str, float]] | None:
        """获取品种的全量指数历史数据

        返回 [(date_str, index_value), ...]
        """
        try:
            resp = self.session.post(
                f"{INDEX_BASE}/pageIndex.action",
                data={
                    "loadType": 0,
                    "code": code,
                    "expClass": exp_class,
                    "publish_type": 0,
                },
                timeout=30,
            )
            data = resp.json()
            if not data.get("success"):
                return None

            raw = data.get("listjson", [])
            if not raw:
                return None

            records = []
            for ts, value in raw:
                date_str = datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d")
                records.append((date_str, float(value)))
            return records
        except Exception as e:
            log.warning("获取指数数据失败 %s: %s", code, e)
            return None

    def get_exp_class(self, code: str) -> int:
        """获取品种页面中的 expClass 值"""
        try:
            resp = self.session.get(
                f"{INDEX_BASE}/pzpage.html",
                params={"code": code},
                timeout=10,
            )
            resp.encoding = "utf-8"
            m = re.search(r"var\s+expClass\s*=\s*['\"]?(\d+)", resp.text)
            if m:
                return int(m.group(1))
        except Exception:
            pass
        return 2  # 默认值

    # ── 主流程 ───────────────────────────────────────────

    def crawl_index(self, skip_existing: bool = True):
        """爬取所有品种的指数数据"""
        init_db()
        conn = get_connection()

        # 获取品种列表
        variety_codes = self.fetch_variety_codes()
        if not variety_codes:
            log.error("未获取到品种列表")
            conn.close()
            return

        # 过滤已有数据的品种
        if skip_existing:
            existing = set()
            rows = conn.execute(
                "SELECT code FROM index_varieties WHERE id IN "
                "(SELECT DISTINCT index_variety_id FROM daily_index)"
            ).fetchall()
            existing = {r["code"] for r in rows}
            todo = {k: v for k, v in variety_codes.items() if k not in existing}
            log.info("跳过已有数据 %d 个，待爬取 %d 个", len(existing), len(todo))
        else:
            todo = variety_codes

        total = len(todo)
        success_count = 0
        fail_count = 0
        total_records = 0
        consecutive_fails = 0

        for i, (code, name) in enumerate(sorted(todo.items())):
            log.info("[%d/%d] %s (%s)", i + 1, total, name, code)

            # 获取 expClass
            exp_class = self.get_exp_class(code)
            time.sleep(REQUEST_INTERVAL)

            # 获取指数数据
            records = self.fetch_index_data(code, exp_class)
            time.sleep(REQUEST_INTERVAL)

            if records:
                try:
                    variety_id = upsert_index_variety(conn, code, name, exp_class)
                    bulk_insert_daily_index(conn, variety_id, records)
                    conn.commit()
                    success_count += 1
                    total_records += len(records)
                    consecutive_fails = 0
                    if (i + 1) % 10 == 0:
                        log.info("  -> %d 条, 范围 %s ~ %s",
                                 len(records), records[0][0], records[-1][0])
                except Exception as e:
                    conn.rollback()
                    fail_count += 1
                    log.error("  保存失败: %s", e)
            else:
                fail_count += 1
                consecutive_fails += 1
                log.warning("  无数据")
                if consecutive_fails >= 10:
                    log.warning("连续 %d 次失败，暂停 30 秒...", consecutive_fails)
                    time.sleep(30)
                    consecutive_fails = 0

        insert_crawl_log(conn, None, "index_crawl", "ok",
                         f"成功{success_count} 失败{fail_count} 共{total_records}条指数记录")
        conn.commit()
        conn.close()
        log.info("指数爬取完成: 成功 %d, 失败 %d, 共 %d 条记录",
                 success_count, fail_count, total_records)

    def compute_prices(self, min_overlap: int = 30, max_cv: float = 0.10):
        """计算K值并生成估算历史价格

        Args:
            min_overlap: 要求的最少重叠天数
            max_cv: K值的最大变异系数（超过则认为不可靠）
        """
        init_db()
        conn = get_connection()

        # 获取所有指数品种
        index_varieties = conn.execute(
            """SELECT iv.id, iv.code, iv.name,
                      COUNT(di.id) as index_count,
                      MIN(di.date) as index_start,
                      MAX(di.date) as index_end
               FROM index_varieties iv
               JOIN daily_index di ON di.index_variety_id = iv.id
               GROUP BY iv.id
               ORDER BY iv.name"""
        ).fetchall()

        log.info("共 %d 个有指数数据的品种", len(index_varieties))

        success_count = 0
        skip_count = 0
        fail_count = 0

        for iv in index_varieties:
            name = iv["name"]
            code = iv["code"]
            iv_id = iv["id"]

            # 查找主站中同名品种的实际价格（可能有多个规格）
            actual_prices = conn.execute(
                """SELECT dp.date, AVG(dp.price) as avg_price
                   FROM daily_prices dp
                   JOIN varieties v ON v.id = dp.variety_id
                   WHERE v.name = ?
                   GROUP BY dp.date
                   ORDER BY dp.date""",
                (name,)
            ).fetchall()

            if not actual_prices:
                skip_count += 1
                continue

            actual_map = {r["date"]: r["avg_price"] for r in actual_prices}
            actual_start = min(actual_map.keys())
            actual_end = max(actual_map.keys())

            # 获取指数数据
            index_records = conn.execute(
                """SELECT date, index_value FROM daily_index
                   WHERE index_variety_id = ? ORDER BY date""",
                (iv_id,)
            ).fetchall()
            index_map = {r["date"]: r["index_value"] for r in index_records}

            # 计算重叠期的 K 值
            k_values = []
            for date_str, avg_price in actual_map.items():
                if avg_price > 0 and date_str in index_map:
                    idx_val = index_map[date_str]
                    if idx_val > 0:
                        k_values.append(idx_val / avg_price)

            if len(k_values) < min_overlap:
                skip_count += 1
                log.debug("  %s: 重叠 %d 天不足（最少 %d）",
                          name, len(k_values), min_overlap)
                continue

            k_mean = statistics.mean(k_values)
            k_std = statistics.stdev(k_values) if len(k_values) > 1 else 0
            k_cv = k_std / k_mean if k_mean > 0 else float("inf")

            # 检查指数是否为常量（如党参）
            idx_vals = [index_map[d] for d in index_map]
            idx_unique = len(set(idx_vals))
            if idx_unique <= 3:
                skip_count += 1
                log.debug("  %s: 指数几乎不变(%d 个唯一值)，跳过", name, idx_unique)
                continue

            if k_cv > max_cv:
                skip_count += 1
                log.debug("  %s: K值 CV=%.2f%% 过大，跳过", name, k_cv * 100)
                continue

            base_price = 1000 / k_mean
            log.info("  %s: K=%.4f, CV=%.2f%%, 基期均价=%.2f, 重叠=%d天",
                     name, k_mean, k_cv * 100, base_price, len(k_values))

            # 保存 K 值
            upsert_k_value(conn, name, k_mean, k_cv, base_price,
                           len(k_values), code)

            # 生成估算价格
            records: list[tuple[str, float, str]] = []
            for date_str in sorted(index_map.keys()):
                if date_str in actual_map:
                    # 有实际价格，用实际价格
                    records.append((date_str, actual_map[date_str], "actual"))
                else:
                    # 用指数反推
                    estimated = index_map[date_str] / k_mean
                    records.append((date_str, round(estimated, 2), "estimated"))

            # 补充实际价格中没有指数覆盖的日期
            for date_str, avg_price in actual_map.items():
                if date_str not in index_map:
                    records.append((date_str, avg_price, "actual"))

            bulk_upsert_estimated_prices(conn, name, records)
            conn.commit()
            success_count += 1

            actual_count = sum(1 for _, _, s in records if s == "actual")
            estimated_count = sum(1 for _, _, s in records if s == "estimated")
            log.info("    -> 总 %d 天 (实际 %d + 估算 %d), 范围 %s ~ %s",
                     len(records), actual_count, estimated_count,
                     records[0][0], records[-1][0])

        insert_crawl_log(conn, None, "compute_prices", "ok",
                         f"成功{success_count} 跳过{skip_count} 失败{fail_count}")
        conn.commit()
        conn.close()
        log.info("价格计算完成: 成功 %d, 跳过 %d, 失败 %d",
                 success_count, skip_count, fail_count)

    def show_stats(self):
        """显示指数数据和估算价格统计"""
        conn = get_connection()

        idx_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM index_varieties"
        ).fetchone()["cnt"]
        idx_data_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM daily_index"
        ).fetchone()["cnt"]
        k_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM variety_k_values"
        ).fetchone()["cnt"]
        est_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM estimated_daily_prices"
        ).fetchone()["cnt"]
        est_actual = conn.execute(
            "SELECT COUNT(*) as cnt FROM estimated_daily_prices WHERE source='actual'"
        ).fetchone()["cnt"]
        est_estimated = conn.execute(
            "SELECT COUNT(*) as cnt FROM estimated_daily_prices WHERE source='estimated'"
        ).fetchone()["cnt"]

        print(f"\n{'='*65}")
        print(f"  指数品种数:       {idx_count}")
        print(f"  指数日数据:       {idx_data_count}")
        print(f"  已计算K值品种:    {k_count}")
        print(f"  估算价格记录:     {est_count} (实际 {est_actual} + 估算 {est_estimated})")
        print(f"{'='*65}")

        if k_count > 0:
            rows = conn.execute(
                """SELECT kv.name, kv.k_value, kv.k_cv, kv.base_price,
                          kv.sample_count, kv.index_variety_code,
                          COUNT(ep.id) as price_count,
                          MIN(ep.date) as start_date,
                          MAX(ep.date) as end_date
                   FROM variety_k_values kv
                   LEFT JOIN estimated_daily_prices ep ON ep.name = kv.name
                   GROUP BY kv.name
                   ORDER BY kv.name
                   LIMIT 30"""
            ).fetchall()
            print(f"\n{'品种':<8} {'K值':>8} {'CV%':>6} {'基期价':>8} "
                  f"{'重叠天':>6} {'记录数':>6} {'范围':<25}")
            print("-" * 80)
            for r in rows:
                cv_pct = f"{r['k_cv']*100:.1f}" if r["k_cv"] else "-"
                date_range = f"{r['start_date'] or '-'} ~ {r['end_date'] or '-'}"
                print(f"{r['name']:<8} {r['k_value']:>8.2f} {cv_pct:>6} "
                      f"{r['base_price']:>8.2f} {r['sample_count']:>6} "
                      f"{r['price_count']:>6} {date_range:<25}")

        conn.close()


if __name__ == "__main__":
    import sys

    crawler = IndexCrawler()

    if len(sys.argv) < 2:
        print("用法:")
        print("  python index_crawler.py index        - 爬取全部品种指数数据(跳过已有)")
        print("  python index_crawler.py index_all    - 爬取全部品种指数数据(覆盖)")
        print("  python index_crawler.py compute      - 计算K值并生成估算价格")
        print("  python index_crawler.py all          - 爬取指数 + 计算价格")
        print("  python index_crawler.py stats        - 显示统计信息")
        sys.exit(0)

    cmd = sys.argv[1]

    if cmd == "index":
        crawler.crawl_index(skip_existing=True)
        crawler.show_stats()
    elif cmd == "index_all":
        crawler.crawl_index(skip_existing=False)
        crawler.show_stats()
    elif cmd == "compute":
        crawler.compute_prices()
        crawler.show_stats()
    elif cmd == "all":
        crawler.crawl_index(skip_existing=True)
        crawler.compute_prices()
        crawler.show_stats()
    elif cmd == "stats":
        init_db()
        crawler.show_stats()
    else:
        print(f"未知命令: {cmd}")

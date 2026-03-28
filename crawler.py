"""康美中药网价格数据爬虫

JSP 接口使用 POST 请求，参数为:
  name     - 药材名称 (encodeURI)
  standard - 规格 (encodeURI)
  origin   - 产地 (encodeURI)
  site     - 市场 (encodeURI)
"""

import requests
import time
import re
import logging
from datetime import datetime
from urllib.parse import quote
from bs4 import BeautifulSoup

from db import (
    get_connection, init_db, upsert_variety, bulk_insert_daily_prices,
    upsert_price_compare, insert_crawl_log, get_variety_count,
    get_daily_price_count,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_URL = "https://www.kmzyw.com.cn"
JSP_BASE = f"{BASE_URL}/jiage/resouces/jsp"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Referer": f"{BASE_URL}/jiage/",
    "X-Requested-With": "XMLHttpRequest",
}
REQUEST_INTERVAL = 0.5  # 秒
LINK_PATTERN = re.compile(r"history_price_(\d{4})-(\d{4})-(\d{2})\.html")


def safe_float(val) -> float:
    if val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


class Crawler:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # ── 品种列表爬取 ──────────────────────────────────────────

    def fetch_variety_links(self) -> list[dict]:
        """从价格涨跌页遍历所有分页，提取品种信息。
        返回 [{name, standard, origin, price, p1, p2, p3}, ...]
        """
        ranking_url = f"{BASE_URL}/jiage/price_ranking.html"
        seen = {}  # (name, standard, origin) -> dict

        log.info("正在获取品种列表（价格涨跌页）...")
        resp = self.session.get(ranking_url, params={"pageNum": 1}, timeout=30)
        resp.encoding = "utf-8"
        soup = BeautifulSoup(resp.text, "lxml")

        total_pages = 1
        page_match = re.search(r"共\s*(\d+)\s*页", soup.get_text())
        if page_match:
            total_pages = int(page_match.group(1))
        log.info("总共 %d 页", total_pages)

        self._parse_ranking_page(soup, seen)
        log.info("第 1/%d 页: 累计 %d 个品种", total_pages, len(seen))

        for page in range(2, total_pages + 1):
            time.sleep(0.3)
            try:
                resp = self.session.get(
                    ranking_url, params={"pageNum": page}, timeout=15,
                )
                resp.encoding = "utf-8"
                soup = BeautifulSoup(resp.text, "lxml")
                self._parse_ranking_page(soup, seen)
                if page % 10 == 0:
                    log.info("第 %d/%d 页: 累计 %d 个品种",
                             page, total_pages, len(seen))
            except Exception as e:
                log.warning("第 %d 页获取失败: %s", page, e)

        varieties = sorted(seen.values(), key=lambda x: (x["p1"], x["p2"], x["p3"]))
        log.info("共发现 %d 个品种规格", len(varieties))
        return varieties

    def _parse_ranking_page(self, soup: BeautifulSoup, seen: dict):
        """解析涨跌排行页的表格行。
        表格列: [0]=品名 [1]=规格 [2]=产地 [3]=价格 [4-7]=涨跌 [8]=历史链接
        """
        for a_tag in soup.find_all("a", href=LINK_PATTERN):
            tr = a_tag.find_parent("tr")
            if not tr:
                continue
            tds = tr.find_all("td")
            if len(tds) < 4:
                continue

            match = LINK_PATTERN.search(a_tag.get("href", ""))
            if not match:
                continue

            name = tds[0].get_text(strip=True)
            standard = tds[1].get_text(strip=True) if len(tds) > 1 else ""
            origin = tds[2].get_text(strip=True) if len(tds) > 2 else ""
            price_text = tds[3].get_text(strip=True) if len(tds) > 3 else ""

            key = (name, standard, origin)
            if key not in seen:
                seen[key] = {
                    "name": name,
                    "standard": standard,
                    "origin": origin,
                    "price": safe_float(price_text),
                    "p1": match.group(1),
                    "p2": match.group(2),
                    "p3": match.group(3),
                }

    # ── JSP 价格接口 (POST + encodeURI) ──────────────────────

    def _make_post_data(self, name: str, standard: str, origin: str,
                        market: str = "亳州") -> dict:
        return {
            "name": quote(name, encoding="utf-8"),
            "standard": quote(standard, encoding="utf-8"),
            "origin": quote(origin, encoding="utf-8"),
            "site": quote(market, encoding="utf-8"),
        }

    def fetch_daily_prices(self, name: str, standard: str,
                           origin: str, market: str = "亳州") -> dict | None:
        """获取历史价格走势 (POST 方式)"""
        url = f"{JSP_BASE}/price_history_todaylist.jsp"
        try:
            resp = self.session.post(
                url, data=self._make_post_data(name, standard, origin, market),
                timeout=15,
            )
            data = resp.json()
            if not data.get("success") and not data.get("data"):
                return None
            return data
        except Exception as e:
            log.warning("历史价格请求失败 %s(%s/%s): %s", name, standard, origin, e)
            return None

    def fetch_price_compare(self, name: str, standard: str,
                            origin: str, market: str = "亳州") -> dict | None:
        """获取今日价格对比 (POST 方式)"""
        url = f"{JSP_BASE}/price_history_todaypricecompare.jsp"
        try:
            resp = self.session.post(
                url, data=self._make_post_data(name, standard, origin, market),
                timeout=15,
            )
            data = resp.json()
            if str(data.get("success")).lower() != "true":
                return None
            return data
        except Exception as e:
            log.warning("价格对比请求失败 %s(%s/%s): %s", name, standard, origin, e)
            return None

    # ── 数据落库 ─────────────────────────────────────────────

    def save_variety_data(self, conn, variety_info: dict,
                          daily_data: dict | None,
                          compare_data: dict | None) -> bool:
        """将一个品种的数据保存到数据库"""
        name = variety_info["name"]
        standard = variety_info["standard"]
        origin = variety_info["origin"]
        market = variety_info.get("market", "亳州")
        p1 = variety_info.get("p1", "")
        p2 = variety_info.get("p2", "")
        p3 = variety_info.get("p3", "")

        current_price = None
        if daily_data and daily_data.get("price"):
            current_price = safe_float(daily_data["price"])

        unit = daily_data.get("measureunit", "元/千克") if daily_data else "元/千克"

        variety_id = upsert_variety(
            conn, name, standard, origin, market, p1, p2, p3, unit, current_price,
        )

        # 保存日价格
        price_count = 0
        if daily_data and daily_data.get("data"):
            records = []
            for ts, price in daily_data["data"]:
                date_str = datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d")
                records.append((date_str, float(price)))
            bulk_insert_daily_prices(conn, variety_id, records)
            price_count = len(records)

        # 保存价格对比
        if compare_data and compare_data.get("newdate"):
            snapshot_date = compare_data["newdate"]
            upsert_price_compare(
                conn, variety_id, snapshot_date,
                new_price=safe_float(compare_data.get("newprice")),
                week_change=safe_float(compare_data.get("wprice")),
                week_change_pct=safe_float(compare_data.get("wpricefloat")),
                month_change=safe_float(compare_data.get("yprice")),
                month_change_pct=safe_float(compare_data.get("ypricefloat")),
                year_change=safe_float(compare_data.get("nprice")),
                year_change_pct=safe_float(compare_data.get("npricefloat")),
            )

        insert_crawl_log(conn, variety_id, "crawl", "ok",
                         f"日价格 {price_count} 条")
        return True

    # ── 主流程 ───────────────────────────────────────────────

    def crawl_varieties(self):
        """爬取品种列表并保存到数据库"""
        init_db()
        varieties = self.fetch_variety_links()
        if not varieties:
            log.error("未获取到品种列表")
            return []

        conn = get_connection()
        for v in varieties:
            upsert_variety(
                conn, v["name"], v["standard"], v["origin"],
                "亳州", v["p1"], v["p2"], v["p3"],
                current_price=v.get("price"),
            )
        conn.commit()
        conn.close()
        log.info("品种列表已保存，共 %d 个", len(varieties))
        return varieties

    def crawl_prices(self, limit: int | None = None, skip_existing: bool = True):
        """爬取所有品种的价格数据"""
        init_db()
        conn = get_connection()

        if get_variety_count(conn) == 0:
            conn.close()
            self.crawl_varieties()
            conn = get_connection()

        if skip_existing:
            rows = conn.execute(
                """SELECT v.id, v.name, v.standard, v.origin, v.market,
                          v.p1, v.p2, v.p3
                   FROM varieties v
                   WHERE v.id NOT IN (
                       SELECT DISTINCT variety_id FROM daily_prices
                   )
                   ORDER BY v.p1, v.p2, v.p3"""
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT id, name, standard, origin, market, p1, p2, p3
                   FROM varieties ORDER BY p1, p2, p3"""
            ).fetchall()

        total = len(rows)
        if limit:
            rows = rows[:limit]

        log.info("待爬取 %d/%d 个品种", len(rows), total)

        success_count = 0
        fail_count = 0

        for i, row in enumerate(rows):
            name, standard, origin = row["name"], row["standard"], row["origin"]
            market = row["market"]
            log.info("[%d/%d] %s (%s/%s) %s",
                     i + 1, len(rows), name, standard, origin, market)

            daily = self.fetch_daily_prices(name, standard, origin, market)
            time.sleep(REQUEST_INTERVAL)

            compare = self.fetch_price_compare(name, standard, origin, market)
            time.sleep(REQUEST_INTERVAL)

            variety_info = {
                "name": name, "standard": standard, "origin": origin,
                "market": market, "p1": row["p1"], "p2": row["p2"], "p3": row["p3"],
            }

            has_data = (daily and daily.get("data")) or (compare and compare.get("newdate"))

            if has_data:
                try:
                    self.save_variety_data(conn, variety_info, daily, compare)
                    conn.commit()
                    success_count += 1
                    data_len = len(daily["data"]) if daily and daily.get("data") else 0
                    price = daily.get("price", "?") if daily else "?"
                    log.info("  -> %s元 (%d 条记录)", price, data_len)
                except Exception as e:
                    conn.rollback()
                    fail_count += 1
                    log.error("  ✗ 保存失败: %s", e)
                    insert_crawl_log(conn, None, "crawl", "error",
                                     f"{name}({standard}/{origin}): {e}")
                    conn.commit()
            else:
                fail_count += 1
                log.warning("  ✗ 无数据")
                insert_crawl_log(conn, None, "crawl", "no_data",
                                 f"{name}({standard}/{origin})")
                conn.commit()

        conn.close()
        log.info("爬取完成: 成功 %d, 失败 %d, 总计 %d",
                 success_count, fail_count, success_count + fail_count)

    def incremental_update(self):
        """增量更新：对所有品种重新拉取日价格，新数据自动去重入库。

        与 crawl_prices 的区别：
        - 不跳过已有数据的品种（每次都拉全量364天，靠 INSERT OR IGNORE 去重）
        - 只请求 todaylist 接口（不请求 pricecompare，减少请求量）
        - 连续失败达到阈值时暂停等待（应对限流）
        """
        init_db()
        conn = get_connection()

        if get_variety_count(conn) == 0:
            conn.close()
            log.error("数据库无品种数据，请先执行 crawl 命令")
            return

        rows = conn.execute(
            """SELECT id, name, standard, origin, market, p1, p2, p3
               FROM varieties ORDER BY p1, p2, p3"""
        ).fetchall()

        total = len(rows)
        log.info("增量更新: 共 %d 个品种", total)

        success_count = 0
        fail_count = 0
        new_records = 0
        consecutive_fails = 0
        MAX_CONSECUTIVE_FAILS = 10

        for i, row in enumerate(rows):
            name, standard, origin = row["name"], row["standard"], row["origin"]
            market = row["market"]
            variety_id = row["id"]

            if (i + 1) % 50 == 0 or i == 0:
                log.info("[%d/%d] %s (%s/%s)",
                         i + 1, total, name, standard, origin)

            daily = self.fetch_daily_prices(name, standard, origin, market)
            time.sleep(REQUEST_INTERVAL)

            if daily and daily.get("data"):
                try:
                    # 统计新增条数
                    before = conn.execute(
                        "SELECT COUNT(*) as cnt FROM daily_prices WHERE variety_id=?",
                        (variety_id,)
                    ).fetchone()["cnt"]

                    records = []
                    for ts, price in daily["data"]:
                        date_str = datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d")
                        records.append((date_str, float(price)))
                    bulk_insert_daily_prices(conn, variety_id, records)

                    # 更新当前价格
                    if daily.get("price"):
                        conn.execute(
                            """UPDATE varieties SET current_price=?, updated_at=datetime('now')
                               WHERE id=?""",
                            (safe_float(daily["price"]), variety_id)
                        )

                    conn.commit()

                    after = conn.execute(
                        "SELECT COUNT(*) as cnt FROM daily_prices WHERE variety_id=?",
                        (variety_id,)
                    ).fetchone()["cnt"]
                    added = after - before
                    new_records += added
                    success_count += 1
                    consecutive_fails = 0

                    if added > 0 and (i + 1) % 50 == 0:
                        log.info("  新增 %d 条价格记录", added)

                except Exception as e:
                    conn.rollback()
                    fail_count += 1
                    log.error("  保存失败 %s: %s", name, e)
            else:
                fail_count += 1
                consecutive_fails += 1

                if consecutive_fails >= MAX_CONSECUTIVE_FAILS:
                    log.warning("连续 %d 次失败，暂停 60 秒...",
                                MAX_CONSECUTIVE_FAILS)
                    time.sleep(60)
                    consecutive_fails = 0

        insert_crawl_log(conn, None, "incremental_update", "ok",
                         f"成功{success_count} 失败{fail_count} 新增{new_records}条")
        conn.commit()
        conn.close()

        log.info("增量更新完成: 成功 %d, 失败 %d, 新增 %d 条记录",
                 success_count, fail_count, new_records)

    def show_stats(self):
        """显示数据库统计"""
        conn = get_connection()
        variety_count = get_variety_count(conn)
        price_count = get_daily_price_count(conn)
        compare_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM price_compare"
        ).fetchone()["cnt"]

        print(f"\n{'='*60}")
        print(f"  品种数量:     {variety_count}")
        print(f"  日价格记录:   {price_count}")
        print(f"  价格对比记录: {compare_count}")
        print(f"  数据库路径:   {DB_PATH}")
        print(f"{'='*60}\n")

        rows = conn.execute(
            """SELECT v.name, v.standard, v.origin, v.current_price,
                      v.p1, v.p2, v.p3,
                      COUNT(dp.id) as price_count,
                      MAX(dp.date) as latest_date
               FROM varieties v
               LEFT JOIN daily_prices dp ON dp.variety_id = v.id
               GROUP BY v.id
               ORDER BY price_count DESC
               LIMIT 20"""
        ).fetchall()

        if rows:
            print(f"{'品种':<8} {'规格':<8} {'产地':<6} {'价格':>8} "
                  f"{'编码':<15} {'记录数':>6} {'最新日期':<12}")
            print("-" * 75)
            for r in rows:
                code = f"{r['p1']}-{r['p2']}-{r['p3']}"
                price_str = f"{r['current_price']:.2f}" if r['current_price'] else "-"
                print(f"{r['name']:<8} {r['standard']:<8} "
                      f"{r['origin']:<6} {price_str:>8} "
                      f"{code:<15} {r['price_count']:>6} "
                      f"{r['latest_date'] or '-':<12}")

        conn.close()


from db import DB_PATH  # noqa: E402

if __name__ == "__main__":
    import sys

    crawler = Crawler()

    if len(sys.argv) < 2:
        print("用法:")
        print("  python crawler.py varieties   - 爬取品种列表")
        print("  python crawler.py test [N]    - 测试爬取前 N 个品种 (默认5)")
        print("  python crawler.py crawl       - 爬取全部品种价格（跳过已有）")
        print("  python crawler.py update      - 增量更新所有品种的最新价格")
        print("  python crawler.py stats       - 显示数据统计")
        sys.exit(0)

    cmd = sys.argv[1]

    if cmd == "varieties":
        crawler.crawl_varieties()
        crawler.show_stats()
    elif cmd == "test":
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 5
        crawler.crawl_prices(limit=limit, skip_existing=False)
        crawler.show_stats()
    elif cmd == "crawl":
        crawler.crawl_prices()
        crawler.show_stats()
    elif cmd == "update":
        crawler.incremental_update()
        crawler.show_stats()
    elif cmd == "stats":
        init_db()
        crawler.show_stats()
    else:
        print(f"未知命令: {cmd}")

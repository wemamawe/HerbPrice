"""中药材新闻多源爬取模块

数据源（均为公开免费）:
1. 中药材天地网产地快讯  https://www.zyctd.com/hqInfo--9.html
2. 中药材天地网行情分析  https://www.zyctd.com/hqInfo--1.html
3. 康美中药网资讯        https://www.kmzyw.com.cn/zixun/
4. 中国中药材网新闻      http://www.zyzyw.com.cn/
5. 农业农村部自然灾害公告 https://www.moa.gov.cn/
6. 中国气象局灾害预警    https://www.cma.gov.cn/

用法:
    python news_crawler.py run           # 全量爬取+LLM解读+入库
    python news_crawler.py fetch         # 只爬取，打印数据不入库
    python news_crawler.py stats         # 显示数据库事件统计
    python news_crawler.py --herb 当归   # 只爬取指定品种
    python news_crawler.py --days 7      # 只抓最近7天
"""

import re
import sys
import time
import json
import hashlib
import logging
from datetime import date, datetime, timedelta
from typing import Optional

import requests
from bs4 import BeautifulSoup

from db import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════
# 配置：重点品种和产区关键词
# ══════════════════════════════════════════════════════════════════

# 从数据库获取的 Top 品种（也可手动维护）
TOP_HERBS = [
    "当归", "白术", "白芍", "黄芪", "党参", "三七", "金银花",
    "麦冬", "枸杞", "川芎", "半夏", "茯苓", "甘草", "丹参",
    "连翘", "板蓝根", "山药", "红花", "牛膝", "桔梗",
    "柴胡", "防风", "苍术", "白芷", "独活", "羌活",
    "薏苡仁", "泽泻", "车前子", "黄连", "黄柏", "地黄",
    "知母", "贝母", "天麻", "人参", "西洋参", "石斛",
    "罗汉果", "砂仁", "豆蔻", "木香", "厚朴", "香附",
]

# 主要产区（用于产地新闻搜索）
TOP_ORIGINS = [
    "亳州", "岷县", "陇西", "渭源", "文山", "三台", "平邑",
    "菏泽", "焦作", "南阳", "中卫", "磐安", "都江堰",
    "赤峰", "恩施", "施秉", "东阿", "永福", "石柱",
]

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9",
}
REQUEST_TIMEOUT = 15
REQUEST_INTERVAL = 1.0  # 请求间隔（秒）


# ══════════════════════════════════════════════════════════════════
# 数据库：新闻去重表 & 入库函数
# ══════════════════════════════════════════════════════════════════

def ensure_news_table():
    """确保 herb_news 表存在（首次运行时创建）"""
    conn = get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS herb_news (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            url_hash    TEXT NOT NULL UNIQUE,       -- URL 的 MD5，用于去重
            title       TEXT NOT NULL,
            url         TEXT NOT NULL DEFAULT '',
            pub_date    TEXT NOT NULL DEFAULT '',   -- 发布日期 YYYY-MM-DD
            source_site TEXT NOT NULL DEFAULT '',   -- 来源网站
            herb_names  TEXT NOT NULL DEFAULT '',   -- 涉及的药材（逗号分隔）
            regions     TEXT NOT NULL DEFAULT '',   -- 涉及的产区（逗号分隔）
            content     TEXT NOT NULL DEFAULT '',   -- 正文摘要（前500字）
            llm_events  TEXT NOT NULL DEFAULT '[]', -- LLM 解读结果 JSON
            is_processed INTEGER NOT NULL DEFAULT 0, -- 是否已用 LLM 解读
            created_at  TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_herb_news_pub_date
            ON herb_news(pub_date DESC);
        CREATE INDEX IF NOT EXISTS idx_herb_news_processed
            ON herb_news(is_processed, pub_date);
    """)
    conn.commit()
    conn.close()


def url_hash(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()


def is_already_fetched(url: str) -> bool:
    conn = get_connection()
    row = conn.execute(
        "SELECT id FROM herb_news WHERE url_hash = ?", (url_hash(url),)
    ).fetchone()
    conn.close()
    return row is not None


def save_news(items: list[dict]) -> int:
    """批量保存新闻到 herb_news 表，返回新增数量"""
    conn = get_connection()
    count = 0
    for item in items:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO herb_news
                (url_hash, title, url, pub_date, source_site, herb_names, regions, content)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                url_hash(item.get("url", item["title"])),
                item["title"],
                item.get("url", ""),
                item.get("pub_date", ""),
                item.get("source_site", ""),
                ",".join(item.get("herb_names", [])),
                ",".join(item.get("regions", [])),
                item.get("content", "")[:1000],
            ))
            if conn.execute("SELECT changes()").fetchone()[0]:
                count += 1
        except Exception as e:
            log.warning(f"新闻入库失败: {e} | {item.get('title', '')[:50]}")
    conn.commit()
    conn.close()
    return count


def get_unprocessed_news(limit: int = 50) -> list[dict]:
    """获取未被 LLM 解读的新闻"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT id, title, url, pub_date, source_site, content, herb_names, regions
        FROM herb_news
        WHERE is_processed = 0
        ORDER BY pub_date DESC, id DESC
        LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def mark_news_processed(news_id: int, llm_events_json: str):
    conn = get_connection()
    conn.execute("""
        UPDATE herb_news SET is_processed = 1, llm_events = ? WHERE id = ?
    """, (llm_events_json, news_id))
    conn.commit()
    conn.close()


# ══════════════════════════════════════════════════════════════════
# 爬虫：各数据源
# ══════════════════════════════════════════════════════════════════

class BaseCrawler:
    """爬虫基类"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(REQUEST_HEADERS)

    def get(self, url: str, **kwargs) -> Optional[BeautifulSoup]:
        try:
            resp = self.session.get(url, timeout=REQUEST_TIMEOUT, **kwargs)
            resp.encoding = resp.apparent_encoding or "utf-8"
            return BeautifulSoup(resp.text, "lxml")
        except Exception as e:
            log.warning(f"GET {url} 失败: {e}")
            return None

    def extract_date(self, text: str) -> str:
        """从字符串中提取 YYYY-MM-DD 格式日期"""
        # 匹配各种日期格式
        patterns = [
            r"(\d{4})-(\d{1,2})-(\d{1,2})",
            r"(\d{4})年(\d{1,2})月(\d{1,2})日",
            r"(\d{4})\.(\d{1,2})\.(\d{1,2})",
        ]
        for pat in patterns:
            m = re.search(pat, text)
            if m:
                y, mo, d = m.group(1), m.group(2), m.group(3)
                return f"{y}-{int(mo):02d}-{int(d):02d}"
        return date.today().isoformat()

    def detect_herbs(self, text: str) -> list[str]:
        """从文本中识别涉及的药材名"""
        return [h for h in TOP_HERBS if h in text]

    def detect_regions(self, text: str) -> list[str]:
        """从文本中识别涉及的产区"""
        return [r for r in TOP_ORIGINS if r in text]


class ZyctdNewsCrawler(BaseCrawler):
    """中药材天地网 - 各栏目新闻列表

    分类 ID:
      200 = 市场快讯
      201 = 产地快讯
      202 = 品种分析
      203 = 市场点评
      204 = 综合资讯
    """

    BASE = "https://www.zyctd.com"
    # (分类ID, 分类名)
    CATEGORIES = [
        (200, "市场快讯"),
        (201, "产地快讯"),
        (202, "品种分析"),
        (203, "市场点评"),
    ]

    def fetch_list(self, cat_id: int, cat_name: str, pages: int = 2) -> list[dict]:
        items = []
        for page in range(1, pages + 1):
            url = f"{self.BASE}/zixun/{cat_id}"
            if page > 1:
                url = f"{self.BASE}/zixun/{cat_id}?page={page}"
            soup = self.get(url)
            if not soup:
                continue

            # 找所有 /zixun/{cat_id}/ 下的文章链接
            for a in soup.find_all("a", href=True):
                href = a["href"]
                title = a.get_text(strip=True)
                # 文章链接格式: /zixun/{cat_id}/{article_id}.html
                if (f"/zixun/{cat_id}/" in href
                        and href.endswith(".html")
                        and len(title) >= 8):
                    if not href.startswith("http"):
                        href = self.BASE + href
                    items.append({
                        "title": title,
                        "url": href,
                        "pub_date": date.today().isoformat(),
                        "source_site": f"zyctd_{cat_name}",
                        "herb_names": self.detect_herbs(title),
                        "regions": self.detect_regions(title),
                        "content": "",
                    })
            time.sleep(REQUEST_INTERVAL)

        log.info(f"[zyctd][{cat_name}] 获取 {len(items)} 条")
        return items

    def fetch_detail(self, item: dict) -> dict:
        """爬取文章正文"""
        if not item.get("url") or is_already_fetched(item["url"]):
            return item
        soup = self.get(item["url"])
        if not soup:
            return item

        # 找发布日期
        for sel in [".pub-date", ".article-date", ".time", "time", ".date"]:
            el = soup.select_one(sel)
            if el:
                item["pub_date"] = self.extract_date(el.get_text())
                break

        # zyctd 正文：收集所有内容长度 > 30 的 p 标签文本
        # （排除声明/版权/联系类段落）
        exclude_kw = ["声明", "版权", "著作权", "未经", "联系电话", "客服邮箱",
                      "互联网药品", "备案", "Copyright", "ICP"]
        paras = []
        for p in soup.find_all("p"):
            txt = p.get_text(strip=True)
            if len(txt) >= 30 and not any(kw in txt for kw in exclude_kw):
                paras.append(txt)

        if paras:
            item["content"] = " ".join(paras)[:800]
            all_text = item["title"] + " " + item["content"]
            item["herb_names"] = list(set(
                item.get("herb_names", []) + self.detect_herbs(all_text)
            ))
            item["regions"] = list(set(
                item.get("regions", []) + self.detect_regions(all_text)
            ))
        return item

    def search(self, keyword: str, pages: int = 1) -> list[dict]:
        """按关键词搜索"""
        items = []
        for page in range(1, pages + 1):
            url = f"{self.BASE}/zixun/?q={requests.utils.quote(keyword)}"
            if page > 1:
                url += f"&page={page}"
            soup = self.get(url)
            if not soup:
                continue
            for a in soup.find_all("a", href=True):
                href = a["href"]
                title = a.get_text(strip=True)
                if ("/zixun/" in href and href.endswith(".html")
                        and len(title) >= 8):
                    if not href.startswith("http"):
                        href = self.BASE + href
                    items.append({
                        "title": title,
                        "url": href,
                        "pub_date": date.today().isoformat(),
                        "source_site": "zyctd_search",
                        "herb_names": self.detect_herbs(title + " " + keyword),
                        "regions": self.detect_regions(title + " " + keyword),
                        "content": "",
                    })
            time.sleep(REQUEST_INTERVAL)
        return items

    def crawl(self, pages: int = 2, fetch_detail: bool = True) -> list[dict]:
        all_items = []
        for cat_id, cat_name in self.CATEGORIES:
            items = self.fetch_list(cat_id, cat_name, pages=pages)
            if fetch_detail:
                for item in items[:15]:  # 每类最多抓15篇正文
                    item = self.fetch_detail(item)
                    time.sleep(REQUEST_INTERVAL * 0.5)
            all_items.extend(items)
        return all_items


class KmzywCrawler(BaseCrawler):
    """康美中药网 - 市场快讯/产地快讯/品种分析

    URL 格式:
      市场快讯: /channel_10100/
      产地快讯: /channel_10200/
      品种分析: /channel_17100/
      文章:     /news/{date}/{id}.html
    """

    BASE = "https://www.kmzyw.com.cn"
    CHANNELS = [
        ("/channel_10100/", "市场快讯"),
        ("/channel_10200/", "产地快讯"),
        ("/channel_17100/", "品种分析"),
    ]

    def fetch_channel(self, path: str, name: str, pages: int = 2) -> list[dict]:
        items = []
        for page in range(1, pages + 1):
            url = f"{self.BASE}{path}" if page == 1 else f"{self.BASE}{path}?page={page}"
            soup = self.get(url)
            if not soup:
                continue

            for a in soup.find_all("a", href=True):
                href = a["href"]
                title = a.get_text(strip=True)
                # 文章链接: /news/20260527/xxx.html
                if "/news/" in href and href.endswith(".html") and len(title) >= 8:
                    if not href.startswith("http"):
                        href = self.BASE + href
                    # 从 URL 中提取日期
                    m = re.search(r"/news/(\d{4})(\d{2})(\d{2})/", href)
                    pub_date = f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else date.today().isoformat()
                    items.append({
                        "title": title,
                        "url": href,
                        "pub_date": pub_date,
                        "source_site": f"kmzyw_{name}",
                        "herb_names": self.detect_herbs(title),
                        "regions": self.detect_regions(title),
                        "content": "",
                    })

            time.sleep(REQUEST_INTERVAL)

        log.info(f"[kmzyw][{name}] 获取 {len(items)} 条")
        return items

    def fetch_detail(self, item: dict) -> dict:
        """爬取康美文章正文"""
        if not item.get("url") or is_already_fetched(item["url"]):
            return item
        soup = self.get(item["url"])
        if not soup:
            return item

        # kmzyw 正文：聚合有效 p 标签
        exclude_kw = ["以上就是", "更多中药材", "康美中药网", "声明", "版权",
                      "联系我们", "ICP", "备案"]
        paras = []
        for p in soup.find_all("p"):
            txt = p.get_text(strip=True)
            if len(txt) >= 20 and not any(kw in txt for kw in exclude_kw):
                paras.append(txt)

        if paras:
            item["content"] = " ".join(paras)[:800]
            all_text = item["title"] + " " + item["content"]
            item["herb_names"] = list(set(
                item.get("herb_names", []) + self.detect_herbs(all_text)
            ))
            item["regions"] = list(set(
                item.get("regions", []) + self.detect_regions(all_text)
            ))
        return item

    def crawl(self, pages: int = 2) -> list[dict]:
        all_items = []
        for path, name in self.CHANNELS:
            items = self.fetch_channel(path, name, pages=pages)
            for item in items[:15]:
                item = self.fetch_detail(item)
                time.sleep(REQUEST_INTERVAL * 0.5)
            all_items.extend(items)
        return all_items


class DisasterNewsCrawler(BaseCrawler):
    """农业农村部自然灾害 & 中国气象局灾害预警（只抓近期，不频繁）"""

    MOA_URL = "https://www.moa.gov.cn/govpublic/ZZYGLS/"  # 种植业管理司
    CMA_URL = "https://www.cma.gov.cn/2011xzt/2011xzsj/"

    def crawl_moa(self, pages: int = 2) -> list[dict]:
        """农业农村部 - 种植管理相关公告"""
        items = []
        for page in range(1, pages + 1):
            url = f"{self.MOA_URL}index{page}.htm" if page > 1 else self.MOA_URL
            soup = self.get(url)
            if not soup:
                continue

            for a in soup.select("ul.list li a, .article-list a, .news-list a"):
                title = a.get_text(strip=True)
                href = a.get("href", "")
                if not href.startswith("http"):
                    href = "https://www.moa.gov.cn" + href.lstrip(".")
                if len(title) < 8:
                    continue
                # 只保留与农业灾害/中药材相关的
                keywords = ["中药", "药材", "旱情", "洪涝", "灾害", "减产", "种植", "农业"]
                if not any(kw in title for kw in keywords):
                    continue
                items.append({
                    "title": title,
                    "url": href,
                    "pub_date": date.today().isoformat(),
                    "source_site": "moa_gov",
                    "herb_names": self.detect_herbs(title),
                    "regions": self.detect_regions(title),
                    "content": "",
                })
            time.sleep(REQUEST_INTERVAL)

        log.info(f"[moa] 共获取 {len(items)} 条")
        return items


class BaiduNewsSpider(BaseCrawler):
    """百度新闻搜索 - 针对重点品种/产区进行搜索"""

    SEARCH_URL = "https://www.baidu.com/s?wd={query}&rn=10&tn=news&ie=utf-8"

    def search(self, query: str) -> list[dict]:
        """搜索百度新闻"""
        url = self.SEARCH_URL.format(query=requests.utils.quote(query))
        # 百度需要特殊 headers
        headers = {
            **REQUEST_HEADERS,
            "Referer": "https://www.baidu.com/",
        }
        try:
            resp = self.session.get(url, timeout=REQUEST_TIMEOUT, headers=headers)
            resp.encoding = "utf-8"
            soup = BeautifulSoup(resp.text, "lxml")
        except Exception as e:
            log.warning(f"百度搜索失败: {e}")
            return []

        items = []
        # 提取搜索结果
        for item in soup.select("div.result, .c-container, .news-item"):
            a_tag = item.select_one("h3 a, .c-title a, a")
            if not a_tag:
                continue
            title = a_tag.get_text(strip=True)
            href = a_tag.get("href", "")
            desc_el = item.select_one(".c-abstract, .c-span9, p")
            desc = desc_el.get_text(strip=True) if desc_el else ""
            if len(title) < 5:
                continue
            items.append({
                "title": title,
                "url": href,
                "pub_date": self.extract_date(item.get_text()),
                "source_site": "baidu_news",
                "herb_names": self.detect_herbs(title + " " + desc),
                "regions": self.detect_regions(title + " " + desc),
                "content": desc[:500],
            })
        return items


# ══════════════════════════════════════════════════════════════════
# LLM 解读 & 事件入库
# ══════════════════════════════════════════════════════════════════

def process_unprocessed_news(batch_size: int = 30) -> int:
    """批量用 LLM 解读未处理的新闻，返回成功入库的事件数"""
    from llm_client import get_llm_client
    from llm_news_interpreter import interpret_news, save_events_to_db

    client = get_llm_client()
    if not client.is_available():
        log.warning("LLM 不可用，跳过解读步骤")
        return 0

    news_list = get_unprocessed_news(limit=batch_size)
    if not news_list:
        log.info("没有待处理的新闻")
        return 0

    log.info(f"开始 LLM 解读 {len(news_list)} 条新闻...")
    total_events = 0

    for news in news_list:
        text = news["title"]
        if news.get("content"):
            text = text + "\n" + news["content"][:400]

        result = interpret_news(text, client)
        events = result.get("events", [])

        # 保存解读结果
        mark_news_processed(news["id"], json.dumps(events, ensure_ascii=False))

        if events:
            saved = save_events_to_db(events, news_url=news.get("url", ""))
            total_events += saved
            if saved > 0:
                log.info(
                    f"  [{news['source_site']}] {news['title'][:45]}... "
                    f"→ {len(events)} 事件，入库 {saved}"
                )
            else:
                # 记录未入库原因（药材名不匹配）
                herb_names = [e.get("herb_name", "") for e in events]
                log.debug(
                    f"  [跳过] {news['title'][:45]}... "
                    f"药材名: {herb_names}（不在herb_origins中）"
                )

        time.sleep(0.5)  # 避免 LLM 限流

    log.info(f"LLM 解读完成，共入库 {total_events} 个事件")
    return total_events


# ══════════════════════════════════════════════════════════════════
# 主流程
# ══════════════════════════════════════════════════════════════════

def run_full_crawl(
    herbs: Optional[list[str]] = None,
    days: int = 30,
    llm_interpret: bool = True,
) -> dict:
    """全量爬取流程

    Args:
        herbs: 指定品种列表（None = 爬全部）
        days: 只保留最近 N 天的新闻
        llm_interpret: 是否调用 LLM 解读

    Returns:
        {"fetched": N, "new": M, "events": K}
    """
    ensure_news_table()
    cutoff = (date.today() - timedelta(days=days)).isoformat()

    all_items: list[dict] = []

    # 1. 中药材天地网全量列表
    log.info("=== 爬取中药材天地网各栏目 ===")
    zyctd = ZyctdNewsCrawler()
    items = zyctd.crawl(pages=2, fetch_detail=True)
    all_items.extend(items)

    # 2. 按重点品种搜索（zyctd）
    log.info("=== 按品种搜索（天地网）===")
    search_targets = herbs if herbs else TOP_HERBS[:20]
    for herb in search_targets:
        results = zyctd.search(herb, pages=1)
        all_items.extend(results)
        time.sleep(REQUEST_INTERVAL)

    # 3. 按重点产区搜索（zyctd）
    log.info("=== 按产区搜索（天地网）===")
    for origin in TOP_ORIGINS[:12]:
        results = zyctd.search(origin + " 药材", pages=1)
        all_items.extend(results)
        time.sleep(REQUEST_INTERVAL)

    # 4. 康美中药网
    log.info("=== 爬取康美中药网 ===")
    kmzyw = KmzywCrawler()
    items = kmzyw.crawl(pages=2)
    all_items.extend(items)

    # 5. 农业农村部
    log.info("=== 爬取农业农村部 ===")
    disaster = DisasterNewsCrawler()
    items = disaster.crawl_moa(pages=1)
    all_items.extend(items)

    # 6. 百度新闻搜索（针对灾害事件）
    log.info("=== 百度新闻搜索 ===")
    baidu = BaiduNewsSpider()
    disaster_queries = [
        "中药材产区 旱情 2026",
        "中药材 洪涝 灾害 2026",
        "中药材 扩种 减产 2026",
        "中药材 政策 补贴 2026",
        "中药材 出口 政策 2026",
    ]
    for query in disaster_queries:
        results = baidu.search(query)
        all_items.extend(results)
        time.sleep(REQUEST_INTERVAL)

    # 去重（按 URL）
    seen_urls: set[str] = set()
    unique_items: list[dict] = []
    for item in all_items:
        key = item.get("url") or item["title"]
        if key not in seen_urls:
            seen_urls.add(key)
            unique_items.append(item)

    # 过滤太老的新闻
    filtered = [
        item for item in unique_items
        if not item.get("pub_date") or item["pub_date"] >= cutoff
    ]

    log.info(f"共获取 {len(all_items)} 条原始数据 → 去重后 {len(unique_items)} 条 → 时间过滤后 {len(filtered)} 条")

    # 入库
    new_count = save_news(filtered)
    log.info(f"新增 {new_count} 条新闻")

    # LLM 解读
    event_count = 0
    if llm_interpret:
        event_count = process_unprocessed_news(batch_size=50)

    return {
        "fetched": len(filtered),
        "new": new_count,
        "events": event_count,
    }


def print_stats():
    """打印数据库中新闻和事件的统计信息"""
    ensure_news_table()
    conn = get_connection()

    # 新闻统计
    total = conn.execute("SELECT COUNT(*) FROM herb_news").fetchone()[0]
    processed = conn.execute(
        "SELECT COUNT(*) FROM herb_news WHERE is_processed = 1"
    ).fetchone()[0]
    recent = conn.execute(
        "SELECT COUNT(*) FROM herb_news WHERE pub_date >= ?",
        ((date.today() - timedelta(days=30)).isoformat(),)
    ).fetchone()[0]

    print(f"\n{'='*50}")
    print(f"herb_news 表: {total} 条 | 已解读: {processed} | 近30天: {recent}")

    # 来源分布
    sources = conn.execute(
        "SELECT source_site, COUNT(*) FROM herb_news GROUP BY source_site ORDER BY 2 DESC"
    ).fetchall()
    print("\n来源分布:")
    for s in sources:
        print(f"  {s[0]:30} {s[1]}")

    # 事件统计
    events = conn.execute("SELECT COUNT(*) FROM weather_events").fetchone()[0]
    print(f"\nweather_events 表: {events} 条")

    by_type = conn.execute(
        "SELECT event_type, COUNT(*) FROM weather_events GROUP BY event_type ORDER BY 2 DESC"
    ).fetchall()
    print("\n事件类型分布:")
    for e in by_type:
        print(f"  {e[0]:25} {e[1]}")

    # 最近入库的事件
    recent_events = conn.execute("""
        SELECT event_type, start_date, severity, affected_herbs, detail
        FROM weather_events
        ORDER BY created_at DESC LIMIT 10
    """).fetchall()
    print(f"\n{'='*50}")
    print("最近入库的10条事件:")
    for e in recent_events:
        print(f"  [{e[1]}] {e[0]:15} sev={e[2]:.1f} herb={e[3]} | {e[4][:60]}")

    conn.close()


# ══════════════════════════════════════════════════════════════════
# CLI 入口
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="中药材新闻多源爬取")
    parser.add_argument("cmd", nargs="?", default="run",
                        choices=["run", "fetch", "stats", "llm"],
                        help="run=全量爬取+解读, fetch=只爬取, stats=统计, llm=只跑LLM解读")
    parser.add_argument("--herb", nargs="+", help="指定品种（可多个）")
    parser.add_argument("--days", type=int, default=90,
                        help="只保留最近N天的新闻（默认90）")
    parser.add_argument("--no-llm", action="store_true",
                        help="跳过LLM解读步骤")
    parser.add_argument("--batch", type=int, default=50,
                        help="LLM 单批处理数量（默认50）")
    args = parser.parse_args()

    if args.cmd == "stats":
        print_stats()

    elif args.cmd == "llm":
        ensure_news_table()
        count = process_unprocessed_news(batch_size=args.batch)
        print(f"LLM 解读完成，入库事件: {count}")

    elif args.cmd == "fetch":
        ensure_news_table()
        result = run_full_crawl(
            herbs=args.herb,
            days=args.days,
            llm_interpret=False,
        )
        print(f"\n爬取完成: 获取 {result['fetched']} 条，新增 {result['new']} 条")
        print_stats()

    else:  # run
        ensure_news_table()
        result = run_full_crawl(
            herbs=args.herb,
            days=args.days,
            llm_interpret=not args.no_llm,
        )
        print(f"\n全流程完成:")
        print(f"  爬取新闻: {result['fetched']} 条（新增 {result['new']} 条）")
        print(f"  入库事件: {result['events']} 条")
        print_stats()

"""SQLite 数据库管理模块"""

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "price_index.db")


def get_connection() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """初始化数据库表结构"""
    conn = get_connection()
    conn.executescript("""
        -- 品种表：由 name+standard+origin 唯一标识
        CREATE TABLE IF NOT EXISTS varieties (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            standard TEXT NOT NULL DEFAULT '',
            origin TEXT NOT NULL DEFAULT '',
            market TEXT NOT NULL DEFAULT '亳州',
            p1 TEXT NOT NULL DEFAULT '',
            p2 TEXT NOT NULL DEFAULT '',
            p3 TEXT NOT NULL DEFAULT '',
            measure_unit TEXT NOT NULL DEFAULT '元/千克',
            current_price REAL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(name, standard, origin, market)
        );

        -- 日价格历史表
        CREATE TABLE IF NOT EXISTS daily_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            variety_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            price REAL NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (variety_id) REFERENCES varieties(id),
            UNIQUE(variety_id, date)
        );

        -- 价格对比快照表
        CREATE TABLE IF NOT EXISTS price_compare (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            variety_id INTEGER NOT NULL,
            snapshot_date TEXT NOT NULL,
            new_price REAL,
            week_change REAL,
            week_change_pct REAL,
            month_change REAL,
            month_change_pct REAL,
            year_change REAL,
            year_change_pct REAL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (variety_id) REFERENCES varieties(id),
            UNIQUE(variety_id, snapshot_date)
        );

        -- 爬取日志表
        CREATE TABLE IF NOT EXISTS crawl_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            variety_id INTEGER,
            action TEXT NOT NULL,
            status TEXT NOT NULL,
            message TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        -- 指数品种表：指数站的品种编码映射
        CREATE TABLE IF NOT EXISTS index_varieties (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            exp_class INTEGER NOT NULL DEFAULT 2,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        -- 品种指数日数据表（13年历史）
        CREATE TABLE IF NOT EXISTS daily_index (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            index_variety_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            index_value REAL NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (index_variety_id) REFERENCES index_varieties(id),
            UNIQUE(index_variety_id, date)
        );

        -- 品种K值表：记录每个品种的 K = Index / AvgPrice 转换系数
        CREATE TABLE IF NOT EXISTS variety_k_values (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            k_value REAL NOT NULL,
            k_cv REAL,
            base_price REAL,
            sample_count INTEGER,
            index_variety_code TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        -- 估算历史价格表：通过指数反推的每日均价
        CREATE TABLE IF NOT EXISTS estimated_daily_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            date TEXT NOT NULL,
            price REAL NOT NULL,
            source TEXT NOT NULL DEFAULT 'estimated',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(name, date)
        );

        CREATE INDEX IF NOT EXISTS idx_daily_prices_variety_date
            ON daily_prices(variety_id, date);
        CREATE INDEX IF NOT EXISTS idx_price_compare_variety_date
            ON price_compare(variety_id, snapshot_date);
        CREATE INDEX IF NOT EXISTS idx_varieties_name
            ON varieties(name, standard, origin);
        CREATE INDEX IF NOT EXISTS idx_daily_index_variety_date
            ON daily_index(index_variety_id, date);
        CREATE INDEX IF NOT EXISTS idx_estimated_daily_prices_name_date
            ON estimated_daily_prices(name, date);
        CREATE INDEX IF NOT EXISTS idx_index_varieties_name
            ON index_varieties(name);
    """)
    conn.commit()
    conn.close()


def upsert_variety(conn: sqlite3.Connection, name: str, standard: str = "",
                   origin: str = "", market: str = "亳州",
                   p1: str = "", p2: str = "", p3: str = "",
                   measure_unit: str = "元/千克",
                   current_price: float | None = None) -> int:
    """插入或更新品种，返回 variety_id"""
    cursor = conn.execute(
        """INSERT INTO varieties (name, standard, origin, market, p1, p2, p3,
                                  measure_unit, current_price, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
           ON CONFLICT(name, standard, origin, market) DO UPDATE SET
               p1 = CASE WHEN excluded.p1 != '' THEN excluded.p1 ELSE varieties.p1 END,
               p2 = CASE WHEN excluded.p2 != '' THEN excluded.p2 ELSE varieties.p2 END,
               p3 = CASE WHEN excluded.p3 != '' THEN excluded.p3 ELSE varieties.p3 END,
               measure_unit = excluded.measure_unit,
               current_price = COALESCE(excluded.current_price, varieties.current_price),
               updated_at = datetime('now')
        """,
        (name, standard, origin, market, p1, p2, p3, measure_unit, current_price)
    )
    if cursor.lastrowid:
        return cursor.lastrowid
    row = conn.execute(
        "SELECT id FROM varieties WHERE name=? AND standard=? AND origin=? AND market=?",
        (name, standard, origin, market)
    ).fetchone()
    return row["id"]


def bulk_insert_daily_prices(conn: sqlite3.Connection, variety_id: int,
                             records: list[tuple[str, float]]):
    """批量插入日价格，忽略重复"""
    conn.executemany(
        """INSERT OR IGNORE INTO daily_prices (variety_id, date, price)
           VALUES (?, ?, ?)""",
        [(variety_id, date, price) for date, price in records]
    )


def upsert_price_compare(conn: sqlite3.Connection, variety_id: int,
                          snapshot_date: str, new_price: float,
                          week_change: float, week_change_pct: float,
                          month_change: float, month_change_pct: float,
                          year_change: float, year_change_pct: float):
    """插入或更新价格对比快照"""
    conn.execute(
        """INSERT INTO price_compare
           (variety_id, snapshot_date, new_price,
            week_change, week_change_pct, month_change, month_change_pct,
            year_change, year_change_pct)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(variety_id, snapshot_date) DO UPDATE SET
               new_price=excluded.new_price,
               week_change=excluded.week_change,
               week_change_pct=excluded.week_change_pct,
               month_change=excluded.month_change,
               month_change_pct=excluded.month_change_pct,
               year_change=excluded.year_change,
               year_change_pct=excluded.year_change_pct
        """,
        (variety_id, snapshot_date, new_price,
         week_change, week_change_pct, month_change, month_change_pct,
         year_change, year_change_pct)
    )


def insert_crawl_log(conn: sqlite3.Connection, variety_id: int | None,
                     action: str, status: str, message: str = ""):
    conn.execute(
        "INSERT INTO crawl_log (variety_id, action, status, message) VALUES (?, ?, ?, ?)",
        (variety_id, action, status, message)
    )


def get_variety_count(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) as cnt FROM varieties").fetchone()
    return row["cnt"]


def get_daily_price_count(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) as cnt FROM daily_prices").fetchone()
    return row["cnt"]


def upsert_index_variety(conn: sqlite3.Connection, code: str, name: str,
                         exp_class: int = 2) -> int:
    """插入或更新指数品种，返回 id"""
    cursor = conn.execute(
        """INSERT INTO index_varieties (code, name, exp_class)
           VALUES (?, ?, ?)
           ON CONFLICT(code) DO UPDATE SET
               name = excluded.name,
               exp_class = excluded.exp_class
        """,
        (code, name, exp_class)
    )
    if cursor.lastrowid:
        return cursor.lastrowid
    row = conn.execute(
        "SELECT id FROM index_varieties WHERE code=?", (code,)
    ).fetchone()
    return row["id"]


def bulk_insert_daily_index(conn: sqlite3.Connection,
                            index_variety_id: int,
                            records: list[tuple[str, float]]):
    """批量插入指数日数据，忽略重复"""
    conn.executemany(
        """INSERT OR IGNORE INTO daily_index (index_variety_id, date, index_value)
           VALUES (?, ?, ?)""",
        [(index_variety_id, date, value) for date, value in records]
    )


def upsert_k_value(conn: sqlite3.Connection, name: str, k_value: float,
                   k_cv: float, base_price: float, sample_count: int,
                   index_variety_code: str):
    """插入或更新品种K值"""
    conn.execute(
        """INSERT INTO variety_k_values
           (name, k_value, k_cv, base_price, sample_count, index_variety_code, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
           ON CONFLICT(name) DO UPDATE SET
               k_value = excluded.k_value,
               k_cv = excluded.k_cv,
               base_price = excluded.base_price,
               sample_count = excluded.sample_count,
               index_variety_code = excluded.index_variety_code,
               updated_at = datetime('now')
        """,
        (name, k_value, k_cv, base_price, sample_count, index_variety_code)
    )


def bulk_upsert_estimated_prices(conn: sqlite3.Connection, name: str,
                                 records: list[tuple[str, float, str]]):
    """批量插入估算价格，source='estimated' 或 'actual'"""
    conn.executemany(
        """INSERT INTO estimated_daily_prices (name, date, price, source)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(name, date) DO UPDATE SET
               price = excluded.price,
               source = excluded.source
        """,
        [(name, date, price, source) for date, price, source in records]
    )


if __name__ == "__main__":
    init_db()
    print(f"数据库初始化完成: {DB_PATH}")

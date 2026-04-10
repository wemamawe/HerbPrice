# 中药材价格趋势分析系统

**[English](#traditional-chinese-medicine-price-trend-analysis-system) | 中文**

基于康美中药网数据，爬取 303 个品种 13 年价格指数，通过 K 值拟合反推历史日均价格，并提供交互式趋势图、价格预测和中医古籍处方成本分析。

## ✨ 功能特性

- 🕷️ **价格爬虫** — 自动爬取康美中药网 1000+ 品种的实时与历史价格
- 📈 **指数反推** — 利用 13 年价格指数 + K 值拟合，还原日均价格时间序列
- 🔮 **智能预测** — Prophet + Ridge + EMA 三模型自适应集成，预测未来 180 天价格走势
- 📚 **古籍分析** — 从 16 本方书中提取 5800+ 处方，计算病症治疗成本
- 📊 **交互式图表** — ECharts 渲染，支持缩放、平移、区间选择
- ⏰ **定时更新** — 支持 launchd / cron 自动增量更新

## 核心原理

通过指数站公开的品种价格指数（2013 年至今），结合主站近一年的实际成交价格，拟合转换系数 K：

```
Index(t) = K × AvgPrice(t)
→ 反推历史价格：AvgPrice(t) = Index(t) / K
```

K 值经变异系数（CV < 10%）校验，确保拟合可靠性。

## 项目结构

```
├── app.py               # Flask Web 应用（API + 页面路由）
├── crawler.py           # 主站价格爬虫（品种列表 + 日价格 + 涨跌对比）
├── index_crawler.py     # 指数站爬虫 + K 值计算 + 历史价格估算
├── forecast.py          # 价格预测（Prophet + Ridge + EMA 自适应集成）
├── tcm_analyzer.py      # 中医古籍处方提取 + 病症治疗成本分析
├── db.py                # SQLite 数据库管理
├── cron_update.sh       # 定时增量更新脚本
├── requirements.txt
├── data/
│   ├── price_index.db   # SQLite 数据库
│   └── tcm_books/       # 古籍文本文件
└── static/
    ├── index.html       # 价格趋势图页面（ECharts）
    └── tcm.html         # 中医病症治疗成本分析页面
```

## 快速开始

```bash
# 克隆项目
git clone <repo-url>
cd price_index

# 创建虚拟环境并安装依赖
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 初始化数据库
python db.py

# 爬取数据（首次需按顺序执行）
python crawler.py varieties    # 1. 爬取品种列表
python crawler.py crawl        # 2. 爬取主站日价格
python index_crawler.py all    # 3. 爬取指数 + 计算 K 值 + 生成估算价格

# 提取古籍处方（可选）
python tcm_analyzer.py extract

# 启动 Web 应用
python app.py
# 访问 http://localhost:5001
```

## 命令参考

### crawler.py — 主站爬虫

| 命令 | 说明 |
|------|------|
| `varieties` | 爬取品种列表（涨跌排行页） |
| `crawl` | 爬取全部品种日价格（跳过已有） |
| `update` | 增量更新所有品种最新价格 |
| `test [N]` | 测试爬取前 N 个品种（默认 5） |
| `stats` | 显示数据统计 |

### index_crawler.py — 指数爬虫

| 命令 | 说明 |
|------|------|
| `index` | 爬取全部品种指数数据（跳过已有） |
| `index_all` | 全量重新爬取指数数据 |
| `compute` | 计算 K 值并生成估算历史价格 |
| `all` | 爬取指数 + 计算价格（一步到位） |
| `stats` | 显示统计信息 |

### tcm_analyzer.py — 古籍处方分析

| 命令 | 说明 |
|------|------|
| `extract` | 从古籍中提取处方数据 |
| `cost <方名>` | 计算指定处方的治疗成本 |
| `symptoms` | 计算所有病症的治疗成本 |
| `stats` | 显示提取统计 |

## Web 页面

### 价格趋势图（`/`）

- **品种搜索**：模糊搜索 303 个品种，点击切换
- **时间区间**：1 年 / 3 年 / 5 年 / 10 年 / 全部，支持自定义日期
- **交互式图表**：ECharts 渲染，支持缩放、平移、悬浮查看
- **数据来源区分**：蓝色（指数反推估算）/ 橙色（主站实际价格）
- **统计卡片**：当前价、最高/最低、区间涨跌幅、K 值偏差
- **价格预测**：三模型集成预测未来 180 天，显示置信区间

### 中医病症分析（`/tcm`）

- **病症列表**：显示所有病症及对应处方数量
- **治疗成本**：按病症聚合，展示单剂/疗程费用的中位数、均值
- **处方详情**：每个处方的药材组成、用量、单味成本、价格匹配率
- **数据总览**：处方总数、药材数、高频病症/药材 Top 20

## API

| 接口 | 说明 |
|------|------|
| `GET /api/varieties` | 品种列表（含数据范围、价格区间） |
| `GET /api/prices?name=白术&start=&end=` | 指定品种的价格数据 |
| `GET /api/k_value?name=白术` | 品种 K 值信息 |
| `GET /api/forecast?name=白术` | 预测未来 180 天价格（6h 缓存） |
| `GET /api/tcm/symptoms` | 所有病症列表及处方数量 |
| `GET /api/tcm/symptom_cost?symptom=咳嗽` | 指定病症的治疗成本详情 |
| `GET /api/tcm/overview` | TCM 分析总览（处方/药材/病症统计） |

## 预测模型

自适应集成方案：短期以 EMA 动量为主（药材价格短期惯性强），中长期逐步引入 Prophet 季节性。

| 模型 | 短期 (1-30天) | 长期 (90+天) | 作用 |
|------|:---:|:---:|------|
| 多周期 EMA | 85%→65% | 45%→35% | 7/30/90 天三层动量叠加，含末端异常值保护 |
| Prophet | 5%→20% | 35%→45% | 年度季节性 + 趋势变化点检测 |
| Ridge 回归 | 10%→15% | 20% | 低阶多项式长期趋势（近 1 年，强正则化） |

**实测改进**（16 天短期预测）：

| 指标 | 旧版 | 新版 | 改进 |
|------|:---:|:---:|:---:|
| 平均 MAPE | 2.16% | 0.67% | -69% |
| ≤5% 误差占比 | 89.7% | 99.7% | +10% |
| 改善品种 | — | 18/20 | 90% |

输出 80% 置信区间（混合波动率 + Prophet 区间），预测结果带 6 小时服务端缓存。

## 数据库

SQLite，路径 `data/price_index.db`，WAL 模式。

| 表 | 说明 |
|------|------|
| `varieties` | 品种基础信息（名称、规格、产地、编码） |
| `daily_prices` | 主站日价格历史 |
| `price_compare` | 涨跌对比快照（周/月/年） |
| `index_varieties` | 指数品种编码映射 |
| `daily_index` | 品种指数日数据（13 年） |
| `variety_k_values` | K 值及拟合参数 |
| `estimated_daily_prices` | 估算历史价格（实际 + 反推） |
| `crawl_log` | 爬取日志 |
| `tcm_formulas` | 古籍处方 |
| `tcm_formula_herbs` | 处方药材明细（古名、用量） |
| `tcm_formula_symptoms` | 处方-病症关联 |
| `tcm_herb_mapping` | 古今药材名映射 |

## 定时更新

### 方式一：launchd（推荐，macOS）

launchd 在电脑睡眠/关机错过调度时间后，**开机会自动补执行**。

```bash
# 安装
cp com.priceindex.update.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.priceindex.update.plist

# 查看状态
launchctl list | grep priceindex

# 卸载
launchctl unload ~/Library/LaunchAgents/com.priceindex.update.plist
```

### 方式二：cron

> ⚠️ macOS 的 cron **不会补执行**错过的任务。

```bash
# crontab -e（每周一凌晨 3 点）
0 3 * * 1 /path/to/price_index/cron_update.sh >> /path/to/price_index/data/cron.log 2>&1
```

## 依赖

| 包 | 最低版本 | 用途 |
|---|:---:|---|
| Flask | 3.0 | Web 框架 |
| requests | 2.31 | HTTP 请求 |
| beautifulsoup4 | 4.12 | HTML 解析 |
| lxml | 5.0 | BS4 解析器后端 |
| Prophet | 1.1.5 | 时间序列预测 |
| scikit-learn | 1.4 | Ridge 回归 |

## License

MIT

---

# Traditional Chinese Medicine Price Trend Analysis System

**[中文](#中药材价格趋势分析系统) | English**

A comprehensive system for analyzing Traditional Chinese Medicine (TCM) herb price trends. Built on data from Kangmei Chinese Medicine Network, it crawls 303 herb varieties across 13 years of price indices, reconstructs historical daily prices via K-value fitting, and provides interactive trend charts, price forecasting, and classical formula treatment cost analysis.

## ✨ Features

- 🕷️ **Price Crawler** — Automatically scrapes real-time and historical prices for 1000+ varieties from Kangmei
- 📈 **Index Reconstruction** — Reconstructs daily price time series using 13 years of price indices + K-value fitting
- 🔮 **Smart Forecasting** — Adaptive ensemble of Prophet + Ridge + EMA models, forecasting 180 days ahead
- 📚 **Classical Text Analysis** — Extracts 5,800+ formulas from 16 ancient medical texts, calculates treatment costs by symptom
- 📊 **Interactive Charts** — ECharts rendering with zoom, pan, and range selection
- ⏰ **Scheduled Updates** — Supports launchd / cron for automatic incremental updates

## Core Methodology

Using publicly available price indices from the index site (2013–present), combined with recent actual transaction prices, the system fits a conversion coefficient K:

```
Index(t) = K × AvgPrice(t)
→ Reconstruct price: AvgPrice(t) = Index(t) / K
```

K values are validated via coefficient of variation (CV < 10%) to ensure reliability.

## Project Structure

```
├── app.py               # Flask web app (API + page routing)
├── crawler.py           # Main site crawler (variety list + daily prices + change comparison)
├── index_crawler.py     # Index site crawler + K-value calculation + historical price estimation
├── forecast.py          # Price forecasting (Prophet + Ridge + EMA adaptive ensemble)
├── tcm_analyzer.py      # Classical text formula extraction + symptom treatment cost analysis
├── db.py                # SQLite database management
├── cron_update.sh       # Scheduled incremental update script
├── requirements.txt
├── data/
│   ├── price_index.db   # SQLite database
│   └── tcm_books/       # Classical text files
└── static/
    ├── index.html       # Price trend chart page (ECharts)
    └── tcm.html         # TCM symptom treatment cost analysis page
```

## Quick Start

```bash
# Clone
git clone <repo-url>
cd price_index

# Set up virtual environment
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Initialize database
python db.py

# Crawl data (run in order for first time)
python crawler.py varieties    # 1. Crawl variety list
python crawler.py crawl        # 2. Crawl daily prices
python index_crawler.py all    # 3. Crawl indices + compute K values + estimate prices

# Extract classical formulas (optional)
python tcm_analyzer.py extract

# Start web app
python app.py
# Visit http://localhost:5001
```

## Command Reference

### crawler.py — Main Site Crawler

| Command | Description |
|---------|-------------|
| `varieties` | Crawl variety list (from price ranking page) |
| `crawl` | Crawl daily prices for all varieties (skip existing) |
| `update` | Incremental update of latest prices |
| `test [N]` | Test crawl for first N varieties (default: 5) |
| `stats` | Show database statistics |

### index_crawler.py — Index Crawler

| Command | Description |
|---------|-------------|
| `index` | Crawl all variety index data (skip existing) |
| `index_all` | Full re-crawl of all index data |
| `compute` | Compute K values and generate estimated prices |
| `all` | Crawl indices + compute prices (all-in-one) |
| `stats` | Show statistics |

### tcm_analyzer.py — Classical Formula Analysis

| Command | Description |
|---------|-------------|
| `extract` | Extract formulas from classical texts |
| `cost <name>` | Calculate cost for a specific formula |
| `symptoms` | Calculate treatment costs for all symptoms |
| `stats` | Show extraction statistics |

## Web Pages

### Price Trend Chart (`/`)

- **Variety Search**: Fuzzy search across 303 varieties
- **Time Range**: 1Y / 3Y / 5Y / 10Y / All, with custom date picker
- **Interactive Chart**: ECharts with zoom, pan, and hover details
- **Data Source Distinction**: Blue (index-estimated) / Orange (actual prices)
- **Stats Cards**: Current price, high/low, range change %, K-value deviation
- **Price Forecast**: Ensemble forecast for 180 days with confidence intervals

### TCM Symptom Analysis (`/tcm`)

- **Symptom List**: All symptoms with associated formula counts
- **Treatment Cost**: Aggregated by symptom — median, mean for single dose / course
- **Formula Details**: Herb composition, dosage, per-herb cost, price match rate
- **Overview**: Total formulas, herbs, top 20 frequent symptoms/herbs

## API

| Endpoint | Description |
|----------|-------------|
| `GET /api/varieties` | Variety list (with data range, price range) |
| `GET /api/prices?name=白术&start=&end=` | Price data for a variety |
| `GET /api/k_value?name=白术` | K-value information |
| `GET /api/forecast?name=白术` | 180-day price forecast (6h cache) |
| `GET /api/tcm/symptoms` | All symptoms with formula counts |
| `GET /api/tcm/symptom_cost?symptom=咳嗽` | Treatment cost details for a symptom |
| `GET /api/tcm/overview` | TCM overview (formula/herb/symptom stats) |

## Forecasting Model

Adaptive ensemble: short-term dominated by EMA momentum (herb prices exhibit strong short-term inertia), gradually shifting to Prophet seasonality for medium/long-term.

| Model | Short-term (1-30d) | Long-term (90+d) | Role |
|-------|:---:|:---:|------|
| Multi-period EMA | 85%→65% | 45%→35% | 7/30/90-day layered momentum with tail anomaly protection |
| Prophet | 5%→20% | 35%→45% | Annual seasonality + changepoint detection |
| Ridge Regression | 10%→15% | 20% | Low-order polynomial long-term trend (1-year window, strong regularization) |

**Benchmark** (16-day short-term forecast):

| Metric | Previous | Current | Improvement |
|--------|:---:|:---:|:---:|
| Average MAPE | 2.16% | 0.67% | -69% |
| ≤5% error rate | 89.7% | 99.7% | +10% |
| Improved varieties | — | 18/20 | 90% |

Outputs 80% confidence interval (hybrid volatility + Prophet interval), with 6-hour server-side caching.

## Database

SQLite at `data/price_index.db`, WAL mode.

| Table | Description |
|-------|-------------|
| `varieties` | Variety info (name, spec, origin, code) |
| `daily_prices` | Main site daily price history |
| `price_compare` | Price change snapshots (week/month/year) |
| `index_varieties` | Index variety code mapping |
| `daily_index` | Variety index daily data (13 years) |
| `variety_k_values` | K values and fitting parameters |
| `estimated_daily_prices` | Estimated historical prices (actual + reconstructed) |
| `crawl_log` | Crawl logs |
| `tcm_formulas` | Classical formulas |
| `tcm_formula_herbs` | Formula herb details (ancient name, dosage) |
| `tcm_formula_symptoms` | Formula-symptom associations |
| `tcm_herb_mapping` | Ancient-to-modern herb name mapping |

## Scheduled Updates

### Option 1: launchd (Recommended for macOS)

launchd **automatically catches up** on missed schedules after sleep/shutdown.

```bash
# Install
cp com.priceindex.update.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.priceindex.update.plist

# Check status
launchctl list | grep priceindex

# Unload
launchctl unload ~/Library/LaunchAgents/com.priceindex.update.plist
```

### Option 2: cron

> ⚠️ macOS cron **does not catch up** on missed jobs.

```bash
# crontab -e (every Monday at 3:00 AM)
0 3 * * 1 /path/to/price_index/cron_update.sh >> /path/to/price_index/data/cron.log 2>&1
```

## Dependencies

| Package | Min Version | Purpose |
|---------|:---:|---------|
| Flask | 3.0 | Web framework |
| requests | 2.31 | HTTP requests |
| beautifulsoup4 | 4.12 | HTML parsing |
| lxml | 5.0 | BS4 parser backend |
| Prophet | 1.1.5 | Time series forecasting |
| scikit-learn | 1.4 | Ridge regression |

## License

MIT

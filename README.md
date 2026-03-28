# 本草价鉴 · HerbPrice

基于康美中药网数据，爬取 303 个品种 13 年的价格指数，通过 K 值拟合反推历史日均价格，并提供交互式趋势图和价格预测。

## 核心原理

通过指数站公开的品种价格指数（2013 年至今），结合主站近一年的实际成交价格，拟合转换系数 K：

```
Index(t) = K × AvgPrice(t)
→ 反推历史价格：AvgPrice(t) = Index(t) / K
```

K 值经变异系数（CV < 10%）校验，确保拟合可靠性。

## 项目结构

```
├── app.py              # Flask Web 应用（API + 静态页面）
├── crawler.py          # 主站价格爬虫（品种列表 + 日价格 + 涨跌对比）
├── index_crawler.py    # 指数站爬虫 + K值计算 + 历史价格估算
├── forecast.py         # 价格预测模块（Prophet + Ridge + EMA 集成）
├── db.py               # SQLite 数据库管理
├── cron_update.sh      # 定时增量更新脚本
├── requirements.txt
├── data/
│   └── price_index.db  # SQLite 数据库
└── static/
    └── index.html      # 前端页面（ECharts 交互式图表）
```

## 快速开始

```bash
# 安装依赖
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 初始化数据库
python db.py

# 爬取数据（首次需按顺序执行）
python crawler.py varieties    # 1. 爬取品种列表
python crawler.py crawl        # 2. 爬取主站日价格
python index_crawler.py all    # 3. 爬取指数 + 计算K值 + 生成估算价格

# 启动 Web 应用
python app.py
# 访问 http://localhost:5001
```

## 命令说明

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

## Web 功能

- **品种搜索**：模糊搜索 303 个品种，点击切换
- **时间区间**：1 年 / 3 年 / 5 年 / 10 年 / 全部，支持自定义日期
- **交互式图表**：ECharts 渲染，支持缩放、平移、悬浮查看
- **数据来源区分**：蓝色（指数反推估算）/ 橙色（主站实际价格）
- **统计卡片**：当前价、最高/最低、区间涨跌幅、K 值偏差
- **价格预测**：三模型集成预测未来 180 天，显示置信区间

## 预测模型

自适应集成方案：短期以 EMA 动量为主（药材价格短期惯性强），中长期逐步引入 Prophet 季节性。

| 模型 | 短期(1-30天) | 长期(90+天) | 作用 |
|------|-------------|------------|------|
| 多周期 EMA | 85%→65% | 45%→35% | 7/30/90天三层动量叠加，含末端异常值保护 |
| Prophet | 5%→20% | 35%→45% | 年度季节性 + 趋势变化点检测（changepoint_range=0.9） |
| Ridge 回归 | 10%→15% | 20% | 低阶多项式长期趋势（近 1 年，强正则化） |

改进效果（16天短期预测实测）：

| 指标 | 旧版（固定权重） | 新版（自适应） | 改进 |
|------|----------------|--------------|------|
| 平均 MAPE | 2.16% | 0.67% | -69% |
| ≤5%误差占比 | 89.7% | 99.7% | +10% |
| 改善品种 | — | 18/20 | 90% |

输出 80% 置信区间（混合波动率+Prophet区间），预测结果带 6 小时服务端缓存。

## 定时更新

### 方式一：launchd（推荐，macOS）

launchd 在电脑睡眠/关机错过调度时间后，**开机会自动补执行**，比 cron 更可靠。

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

> ⚠️ macOS 的 cron **不会补执行**错过的任务。如果调度时间点电脑处于关机/睡眠状态，任务会被跳过。

```bash
# crontab -e 添加（每周一凌晨 3 点执行）
0 3 * * 1 /path/to/price_index/cron_update.sh >> /path/to/price_index/data/cron.log 2>&1
```

## 数据库表

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

## API

| 接口 | 说明 |
|------|------|
| `GET /api/varieties` | 品种列表（含数据范围、价格区间） |
| `GET /api/prices?name=白术&start=&end=` | 指定品种的价格数据 |
| `GET /api/k_value?name=白术` | 品种 K 值信息 |
| `GET /api/forecast?name=白术` | 预测未来 180 天价格 |

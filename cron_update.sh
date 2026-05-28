#!/bin/bash
# 定时增量更新中药价格数据 + 新闻事件采集
# 建议每周执行一次: crontab -e 添加
# 0 3 * * 1 /Users/warma/wema/code/HerbPrice/cron_update.sh >> /Users/warma/wema/code/HerbPrice/data/cron.log 2>&1

set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

echo "========================================"
echo "增量更新开始: $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"

# 激活虚拟环境
source .venv/bin/activate

# 1. 价格数据增量更新
echo "--- 步骤1: 价格数据爬取 ---"
python crawler.py update

# 2. 新闻事件多源采集（--no-llm 跑爬取；LLM 可选，需配置好 .env）
echo "--- 步骤2: 新闻事件采集 ---"
python news_crawler.py run --days 30 2>&1 || echo "新闻采集遇到错误，已跳过"

echo "增量更新结束: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

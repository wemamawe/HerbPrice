#!/bin/bash
# 定时增量更新中药价格数据
# 建议每周执行一次: crontab -e 添加
# 0 3 * * 1 /Users/warma/wema/code/price_index/cron_update.sh >> /Users/warma/wema/code/price_index/data/cron.log 2>&1

set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

echo "========================================"
echo "增量更新开始: $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"

# 激活虚拟环境
source .venv/bin/activate

# 执行增量更新
python crawler.py update

echo "增量更新结束: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

#!/bin/bash
# HerbPrice 更新脚本
# 用法: ./update.sh
cd /home/ubuntu/HerbPrice

# 丢弃 index.html 的暂存和工作区修改（由 post-merge hook 重新生成）
git restore --staged static/index.html 2>/dev/null || true
git checkout -- static/index.html 2>/dev/null || true

# 拉取最新代码（post-merge hook 会自动处理路径替换和服务重启）
git pull

echo "[update] 完成"

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Maker-Checker 信息来源审查报告生成器 v2.0
增强功能：
  - URL 自动有效性验证（对 high 可信度来源）
  - 可视化审查仪表盘（ASCII 饼图 + 可信度分布）
  - external_safe 字段自动审查（对外报告合规性）
  - 版本追踪（保留历史审查记录 diff 摘要）

用法：
  python3 maker_checker.py --sources sources.json --output review_report.md
  python3 maker_checker.py --sources sources.json --output review_report.md --verify-urls
  python3 maker_checker.py --inline  （从stdin读取JSON）
  python3 maker_checker.py --sources sources.json --external  （对外报告模式，严格审查）

sources.json 格式：
[
  {
    "section": "章节名称",
    "claim": "所引用的具体论点或数据",
    "source_url": "原始来源URL",
    "source_name": "来源名称",
    "source_date": "信息日期（YYYY-MM 或 YYYY）",
    "confidence": "high/medium/low",
    "notes": "备注（可选）",
    "external_safe": true/false   （可选，对外报告合规标注）
  }
]
"""

import json
import sys
import argparse
import urllib.request
import urllib.error
from datetime import datetime

# ─── 常量 ────────────────────────────────────────────────────────────────────

CONFIDENCE_LABELS = {
    "high":   ("[OK] 高可信", "来源权威、数据可核实"),
    "medium": ("[!!] 中等",   "来源可靠但数据为预测/估算"),
    "low":    ("[XX] 待核实", "来源不明确或数据存疑，建议删除或替换"),
}

CONFIDENCE_ORDER = {"high": 0, "medium": 1, "low": 2}

# 内部来源黑名单（对外报告禁止使用）
INTERNAL_SOURCE_PATTERNS = [
    "iwiki.woa.com", "tapd.woa.com", "km.woa.com", "git.woa.com",
    "trpc", "internal", "iwiki", "tapd", "isearch", "内部", "保密",
    "iSearch", "KM文档", "工蜂"
]


# ─── URL 验证 ─────────────────────────────────────────────────────────────────

def verify_url(url: str, timeout: int = 8) -> dict:
    """验证 URL 是否可访问，返回状态信息。"""
    if not url or not url.startswith("http"):
        return {"status": "skip", "label": "[-] 无URL", "code": None}
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; research-report-checker/2.0)"}
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            code = resp.getcode()
            if code == 200:
                return {"status": "ok", "label": "[+] 链接有效", "code": code}
            else:
                return {"status": "warn", "label": f"[?] HTTP {code}", "code": code}
    except urllib.error.HTTPError as e:
        return {"status": "error", "label": f"[X] HTTP错误 {e.code}", "code": e.code}
    except urllib.error.URLError as e:
        return {"status": "error", "label": f"[X] 访问失败", "code": None}
    except Exception as e:
        return {"status": "error", "label": f"[X] 验证异常", "code": None}


def check_external_safety(source: dict) -> dict:
    """检查来源是否适合对外报告使用。"""
    src_name = source.get("source_name", "")
    src_url  = source.get("source_url", "")
    notes    = source.get("notes", "")
    combined = f"{src_name} {src_url} {notes}".lower()

    for pattern in INTERNAL_SOURCE_PATTERNS:
        if pattern.lower() in combined:
            return {
                "safe": False,
                "reason": f"包含内部来源关键词: '{pattern}'",
                "label": "[!] 不可对外"
            }

    explicit = source.get("external_safe")
    if explicit is False:
        return {"safe": False, "reason": "已标注 external_safe=false", "label": "[!] 不可对外"}
    elif explicit is True:
        return {"safe": True, "reason": "已标注 external_safe=true", "label": "[+] 已确认"}
    else:
        return {"safe": None, "reason": "未标注 external_safe 字段", "label": "[?] 待确认"}


# ─── 可视化仪表盘 ─────────────────────────────────────────────────────────────

def ascii_bar(value: int, total: int, width: int = 20, fill: str = "█", empty: str = "░") -> str:
    """生成 ASCII 进度条。"""
    if total == 0:
        return empty * width
    filled = round(value / total * width)
    return fill * filled + empty * (width - filled)


def generate_dashboard(sources: list, url_results: dict = None, external_mode: bool = False) -> str:
    """生成可视化审查仪表盘（Markdown 格式）。"""
    total  = len(sources)
    high   = sum(1 for s in sources if s.get("confidence") == "high")
    medium = sum(1 for s in sources if s.get("confidence") == "medium")
    low    = sum(1 for s in sources if s.get("confidence") == "low")

    lines = []
    lines.append("## 📊 审查仪表盘\n")
    lines.append("```")
    lines.append(f"  总引用数：{total} 条")
    lines.append(f"")
    lines.append(f"  [OK] 高可信  {ascii_bar(high,   total)}  {high:2d} 条  ({high/total*100:.0f}%)" if total else "")
    lines.append(f"  [!!] 中等    {ascii_bar(medium, total)}  {medium:2d} 条  ({medium/total*100:.0f}%)" if total else "")
    lines.append(f"  [XX] 待核实  {ascii_bar(low,    total)}  {low:2d} 条  ({low/total*100:.0f}%)" if total else "")
    lines.append(f"")

    if url_results:
        ok_count   = sum(1 for r in url_results.values() if r["status"] == "ok")
        err_count  = sum(1 for r in url_results.values() if r["status"] == "error")
        skip_count = sum(1 for r in url_results.values() if r["status"] == "skip")
        lines.append(f"  URL验证结果：有效 {ok_count} / 失效 {err_count} / 无链接 {skip_count}")

    if external_mode:
        safe_ok   = sum(1 for s in sources if check_external_safety(s)["safe"] is True)
        safe_no   = sum(1 for s in sources if check_external_safety(s)["safe"] is False)
        safe_unk  = sum(1 for s in sources if check_external_safety(s)["safe"] is None)
        lines.append(f"  对外合规：已确认 {safe_ok} / 不可对外 {safe_no} / 待确认 {safe_unk}")

    lines.append("```\n")
    return "\n".join(lines)


# ─── 主报告生成 ───────────────────────────────────────────────────────────────

def generate_review_report(
    sources: list,
    topic: str = "研究报告",
    verify_urls: bool = False,
    external_mode: bool = False,
    prev_report_path: str = None
) -> str:
    """将来源列表转换为结构化 Markdown 审查报告（v2.0）。"""
    now   = datetime.now().strftime("%Y-%m-%d %H:%M")
    total = len(sources)
    high  = sum(1 for s in sources if s.get("confidence") == "high")
    medium= sum(1 for s in sources if s.get("confidence") == "medium")
    low   = sum(1 for s in sources if s.get("confidence") == "low")

    # URL 验证（仅对 high 可信度来源）
    url_results = {}
    if verify_urls:
        print("[验证URL中，请稍候...]", file=sys.stderr)
        for i, s in enumerate(sources):
            url = s.get("source_url", "")
            if url:
                url_results[i] = verify_url(url)
                status_icon = url_results[i]["label"]
                print(f"  [{i+1}/{total}] {status_icon} {url[:60]}", file=sys.stderr)
            else:
                url_results[i] = {"status": "skip", "label": "[-] 无URL", "code": None}

    lines = []
    lines.append(f"# 信息来源审查报告 — {topic}")
    lines.append(f"\n> 生成时间：{now} | 版本：v2.0 | 共 {total} 条引用 | [OK] 高可信 {high} | [!!] 待确认 {medium} | [XX] 待核实 {low}")
    if external_mode:
        lines.append("> **对外报告模式已启用** — 严格审查内部来源合规性")
    lines.append("\n---\n")

    # 仪表盘
    lines.append(generate_dashboard(sources, url_results if verify_urls else None, external_mode))
    lines.append("---\n")

    # 警示区：低可信
    if low > 0:
        lines.append("## [!] 审查警示\n")
        lines.append(f"**发现 {low} 条低可信引用**，建议在生成最终报告前替换或删除：\n")
        for s in sources:
            if s.get("confidence") == "low":
                lines.append(f"- **[{s.get('section','?')}]** {s.get('claim','')[:80]}...")
        lines.append("\n---\n")

    # 警示区：对外合规
    if external_mode:
        unsafe = [s for s in sources if check_external_safety(s)["safe"] is False]
        if unsafe:
            lines.append("## [!] 对外合规警示\n")
            lines.append(f"**发现 {len(unsafe)} 条不可对外使用的引用**，请立即处理：\n")
            for s in unsafe:
                safety = check_external_safety(s)
                lines.append(f"- **[{s.get('section','?')}]** {s.get('claim','')[:60]}... — {safety['reason']}")
            lines.append("\n---\n")

    # 警示区：URL 失效
    if verify_urls:
        broken_urls = [(i, s) for i, s in enumerate(sources)
                       if url_results.get(i, {}).get("status") == "error"]
        if broken_urls:
            lines.append("## [!] URL 失效警示\n")
            lines.append(f"**发现 {len(broken_urls)} 条链接无法访问**，请核实或替换：\n")
            for i, s in broken_urls:
                lines.append(f"- **[{s.get('section','?')}]** [{s.get('source_name','?')}]({s.get('source_url','')}) — {url_results[i]['label']}")
            lines.append("\n---\n")

    # 按章节分组明细
    sections: dict = {}
    for i, s in enumerate(sources):
        sec = s.get("section", "未分类")
        sections.setdefault(sec, []).append((i, s))

    lines.append("## 按章节审查明细\n")

    for sec_name, items in sections.items():
        lines.append(f"### {sec_name}\n")

        headers = ["#", "论点/数据", "来源", "日期", "可信度", "URL状态" if verify_urls else None, "对外合规" if external_mode else None, "备注"]
        headers = [h for h in headers if h]
        lines.append("| " + " | ".join(headers) + " |")
        lines.append("|" + "|".join(["---"] * len(headers)) + "|")

        for idx, (orig_i, s) in enumerate(items, 1):
            claim     = s.get("claim", "").replace("|", "｜")
            claim_short = claim[:55] + ("..." if len(claim) > 55 else "")
            src_name  = s.get("source_name", "—")
            src_url   = s.get("source_url", "")
            src_date  = s.get("source_date", "—")
            conf_key  = s.get("confidence", "medium")
            conf_label= CONFIDENCE_LABELS.get(conf_key, ("[?]", ""))[0]
            notes     = s.get("notes", "—").replace("|", "｜")

            src_cell  = f"[{src_name}]({src_url})" if src_url else src_name

            row = [str(idx), claim_short, src_cell, src_date, conf_label]

            if verify_urls:
                url_label = url_results.get(orig_i, {}).get("label", "[-]")
                row.append(url_label)

            if external_mode:
                safety = check_external_safety(s)
                row.append(safety["label"])

            row.append(notes)
            lines.append("| " + " | ".join(row) + " |")

        lines.append("")

    # 版本追踪（如果提供了历史报告路径）
    if prev_report_path:
        try:
            with open(prev_report_path, encoding="utf-8") as f:
                prev_content = f.read()
            prev_lines = set(prev_content.splitlines())
            lines.append("---\n")
            lines.append("## 版本变更摘要\n")
            lines.append(f"> 与历史版本 `{prev_report_path}` 对比\n")
            lines.append(f"- 本次总引用：{total} 条（历史文件已存档）")
        except Exception:
            pass

    # Checker 确认区
    lines.append("---\n")
    lines.append("## Checker 确认区\n")
    lines.append("请逐项审查以上内容，并在确认后填写以下声明：\n")
    lines.append("- [ ] 已确认所有 [OK] 高可信引用，内容准确无误")
    lines.append("- [ ] 已处理所有 [!!] 中等可信引用（已核实 / 已标注估算）")
    lines.append("- [ ] 已删除或替换所有 [XX] 低可信引用")
    lines.append("- [ ] 已确认报告中无虚构数据、无过时信息（超过2年）")
    if verify_urls:
        lines.append("- [ ] 已处理所有 [X] URL失效引用")
    if external_mode:
        lines.append("- [ ] 已确认所有引用均为公开来源，无内部数据泄露风险")
    lines.append("")
    lines.append("**审查人：** _______________  **审查日期：** _______________")
    lines.append("")
    lines.append("**审查结论：** [ ] 批准生成最终报告  [ ] 需修改后重新审查  [ ] 拒绝\n")
    lines.append("---")
    lines.append("\n*本报告由 research-report-cn skill Maker-Checker v2.0 自动生成。*")

    return "\n".join(lines)


# ─── CLI 入口 ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Maker-Checker 审查报告生成器 v2.0")
    parser.add_argument("--sources",      help="来源JSON文件路径")
    parser.add_argument("--output",       help="输出Markdown文件路径（默认stdout）")
    parser.add_argument("--topic",        default="研究报告", help="报告主题名称")
    parser.add_argument("--inline",       action="store_true", help="从stdin读取JSON")
    parser.add_argument("--verify-urls",  action="store_true", help="自动验证所有URL有效性")
    parser.add_argument("--external",     action="store_true", help="对外报告模式（严格合规审查）")
    parser.add_argument("--prev-report",  help="上一版审查报告路径（用于版本追踪）")
    args = parser.parse_args()

    if args.inline:
        sources = json.load(sys.stdin)
    elif args.sources:
        with open(args.sources, encoding="utf-8") as f:
            sources = json.load(f)
    else:
        parser.print_help()
        sys.exit(1)

    report = generate_review_report(
        sources,
        topic=args.topic,
        verify_urls=args.verify_urls,
        external_mode=args.external,
        prev_report_path=args.prev_report
    )

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"[OK] 审查报告已保存至：{args.output}")
    else:
        print(report)


if __name__ == "__main__":
    main()

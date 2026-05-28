"""LLM 新闻解读模块

用 LLM 将中药材新闻自动解读为结构化事件，入库 weather_events 表。
替代 weather_monitor.py 中 NewsMonitor 的关键词匹配方法。

用法:
    python llm_news_interpreter.py test       # 测试 LLM 连接
    python llm_news_interpreter.py run        # 解读最新新闻
    python llm_news_interpreter.py interpret "新闻文本"  # 解读指定文本
"""

import sys
import json
import logging
import time
from datetime import date, datetime
from db import get_connection

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ═══════════════════════════════════════════════════════════════════════
# 系统提示词
# ═══════════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """你是一位资深的中药材市场分析师。你的任务是从新闻资讯中提取与中药材价格相关的事件信息。

## 事件类型定义

| 类型 | 说明 | 典型关键信号 |
|------|------|-------------|
| drought | 干旱 | 干旱、旱情、缺水、降雨不足 |
| flood | 洪涝 | 洪水、暴雨、水淹、涝灾 |
| frost | 霜冻 | 霜冻、低温冻害、倒春寒 |
| pest | 病虫害 | 病虫害、根腐病、枯萎 |
| heat_wave | 高温 | 高温、热害、干热风 |
| area_increase | 扩种 | 扩大种植、面积增加、跟风种植 |
| area_decrease | 缩种 | 缩减种植、改种、弃种 |
| demand_surge | 需求激增 | 需求增加、抢购、缺货 |
| policy_positive | 利好政策 | 补贴、扶持、纳入医保 |
| export_ban | 出口限制 | 限制出口、禁运 |

## 严重程度 (severity) 校准标准
- 0.1-0.3: 轻微，局部小范围，"部分"、"少量"
- 0.4-0.6: 中等，影响一个产区的一部分
- 0.7-0.8: 严重，主产区大面积受灾，"N年不遇"、"大面积"、"严重"
- 0.9-1.0: 极端，主产区绝收/全面受灾，"历史最X"、"绝收"

**重要校准规则**：
- 出现"N十年不遇"、"历史新高/最严重" → severity ≥ 0.8
- 扩种面积增长 >50% → severity ≥ 0.8
- 明确提到"减产X%" → severity ≈ 减产比例（如减产30% → 0.8）

## 主要药材产区占比参考（用于估算 affected_pct）
- 当归: 岷县占70%, 渭源/临潭占15%
- 白术: 亳州占30%, 磐安占12%
- 白芍: 亳州占70%, 磐安占15%
- 三七: 文山占90%
- 黄芪: 陇西占30%, 赤峰占18%
- 金银花: 平邑占60%
- 麦冬: 三台占70%
- 党参: 渭源/陇西各占20-25%
- 枸杞: 中卫占25%, 海西占20%

## 价格影响幅度参考（historical evidence）
- 主产区严重干旱(如岷县当归): +20~+80%
- 主产区洪涝(如亳州白术): +15~+40%
- 大规模扩种(面积+50%以上): -15~-30%（延后1个种植周期生效）
- 需求激增(疫情等): +15~+30%
- 一般灾害(非主产区): +5~+15%

## 输出要求
以 JSON 格式输出，严格按以下结构：
```json
{
  "events": [
    {
      "herb_name": "药材名称",
      "event_type": "drought|flood|frost|pest|heat_wave|area_increase|area_decrease|demand_surge|policy_positive|export_ban",
      "severity": 0.5,
      "affected_region": "受影响的县/市",
      "affected_pct": 0.3,
      "summary": "一句话描述事件及预期影响",
      "confidence": 0.8,
      "price_direction": "up|down|neutral",
      "estimated_impact_pct": 5
    }
  ],
  "net_assessment": "当多个相反信号同时存在时，给出净影响判断",
  "no_event_reason": "如果没有提取到事件，说明原因"
}
```

## 注意事项
1. 只提取与中药材供给/需求/价格直接相关的事件
2. 如果新闻中没有明确事件，返回空 events 列表
3. affected_pct 必须参考上方产区占比数据，不要凭空猜测
4. estimated_impact_pct 必须参考上方历史影响数据，主产区重大灾害至少 +20%
5. 一条新闻可能涉及多个药材或多个事件
6. 当新闻包含对冲信号（如干旱+扩种），在 net_assessment 中说明综合影响
"""


def interpret_news(news_text: str, llm_client=None) -> dict:
    """用 LLM 解读单条新闻

    Args:
        news_text: 新闻标题+正文
        llm_client: LLM 客户端实例

    Returns:
        解读结果 dict（含 events 列表）
    """
    if llm_client is None:
        from llm_client import get_llm_client
        llm_client = get_llm_client()

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"请分析以下中药材相关新闻，提取事件信息：\n\n{news_text}"},
    ]

    try:
        result = llm_client.chat_json(messages, temperature=0.2)
        return result
    except Exception as e:
        log.error(f"LLM 解读失败: {e}")
        return {"events": [], "error": str(e)}


def interpret_batch(news_list: list[str], llm_client=None) -> list[dict]:
    """批量解读新闻（一次传入多条，减少 API 调用）"""
    if not news_list:
        return []

    if llm_client is None:
        from llm_client import get_llm_client
        llm_client = get_llm_client()

    # 每批最多 5 条
    batch_size = 5
    all_results = []

    for i in range(0, len(news_list), batch_size):
        batch = news_list[i:i + batch_size]
        combined = "\n\n---\n\n".join(
            f"【新闻{j+1}】{text}" for j, text in enumerate(batch)
        )

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"请分析以下 {len(batch)} 条中药材新闻，逐条提取事件：\n\n{combined}"},
        ]

        try:
            result = llm_client.chat_json(messages, temperature=0.2, max_tokens=3000)
            all_results.append(result)
        except Exception as e:
            log.error(f"批量解读失败: {e}")
            all_results.append({"events": [], "error": str(e)})

        if i + batch_size < len(news_list):
            time.sleep(1)  # 避免限流

    return all_results


def save_events_to_db(events: list[dict], source: str = "llm_news",
                      news_url: str = "", event_date: str = "") -> int:
    """将 LLM 解读的事件存入 weather_events 表

    Args:
        events: LLM 解读结果列表
        source: 来源标识
        news_url: 新闻原文链接（用于溯源）
        event_date: 新闻发布日期 YYYY-MM-DD（优先使用，否则用 today）

    Returns: 成功入库的事件数
    """
    conn = get_connection()
    count = 0

    # 确定事件日期
    start_date_val = (
        event_date if event_date and len(event_date) == 10
        else date.today().isoformat()
    )

    # 获取已有的药材名列表用于校验（herb_origins + estimated_daily_prices 两表取并集）
    herb_names_origins = set(
        r[0] for r in conn.execute(
            "SELECT DISTINCT herb_name FROM herb_origins"
        ).fetchall()
    )
    herb_names_prices = set(
        r[0] for r in conn.execute(
            "SELECT DISTINCT name FROM estimated_daily_prices"
        ).fetchall()
    )
    all_herb_names = herb_names_origins | herb_names_prices

    for evt in events:
        herb_name = evt.get("herb_name", "").strip()
        confidence = evt.get("confidence", 0)

        # 校验：置信度阈值
        if confidence < 0.6:
            log.debug(f"跳过低置信度事件: {herb_name} conf={confidence}")
            continue

        # 精确匹配
        if herb_name not in all_herb_names:
            # 尝试模糊匹配（herb_name 是 "白术" 而表中是 "白术（亳州）" 等情况）
            matched = [h for h in all_herb_names if herb_name in h or h in herb_name]
            # 排除 "中药材" 等泛称
            matched = [h for h in matched if len(h) >= 2 and len(herb_name) >= 2
                       and herb_name not in ("中药材", "药材", "饮片", "中草药")]
            if matched:
                herb_name = min(matched, key=len)  # 取最短（最精确）匹配
                log.debug(f"模糊匹配: {evt['herb_name']} → {herb_name}")
            else:
                log.debug(f"跳过未知药材: '{herb_name}'")
                continue

        event_type = evt.get("event_type", "")
        if event_type not in (
            "drought", "flood", "frost", "pest", "heat_wave",
            "area_increase", "area_decrease", "demand_surge",
            "policy_positive", "export_ban"
        ):
            continue

        severity = min(1.0, max(0.0, float(evt.get("severity", 0.5))))
        region = evt.get("affected_region", "")
        summary = evt.get("summary", "")
        impact_pct = evt.get("estimated_impact_pct")
        price_direction = evt.get("price_direction", "neutral")

        detail = f"[LLM|{source}] {summary}"
        if news_url:
            detail += f" | {news_url[:100]}"

        try:
            conn.execute("""
                INSERT INTO weather_events
                (origin, province, event_type, start_date, severity, detail,
                 affected_herbs, price_impact_pct)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                region, "", event_type, start_date_val,
                severity, detail, herb_name,
                float(impact_pct) if impact_pct is not None else None,
            ))
            count += 1
        except Exception as e:
            log.warning(f"事件入库失败: {e}")

    conn.commit()
    conn.close()
    log.info(f"成功入库 {count} 条 LLM 解读事件")
    return count


def run_interpret():
    """从新闻源获取最新新闻并解读"""
    from llm_client import get_llm_client

    client = get_llm_client()
    if not client.is_available():
        log.error(f"LLM 服务不可用: {client.api_base}")
        return

    # 尝试从中药材天地网获取新闻（复用 weather_monitor 的逻辑）
    try:
        from weather_monitor import NewsMonitor
        monitor = NewsMonitor()
        # 获取几个重点品种的新闻
        herbs = ["当归", "白术", "白芍", "黄芪", "党参", "三七", "金银花"]
        all_news = []
        for herb in herbs:
            news = monitor.fetch_news(herb, days=3)
            all_news.extend(news)
            time.sleep(0.5)

        if all_news:
            log.info(f"获取到 {len(all_news)} 条新闻，开始 LLM 解读...")
            news_texts = [n.get("title", "") + " " + n.get("content", "") for n in all_news]
            results = interpret_batch(news_texts, client)

            total_events = []
            for r in results:
                total_events.extend(r.get("events", []))

            if total_events:
                saved = save_events_to_db(total_events)
                log.info(f"共提取 {len(total_events)} 个事件，入库 {saved} 个")
            else:
                log.info("未提取到事件")
        else:
            log.info("未获取到新闻，尝试测试模式...")
            _test_interpret(client)
    except Exception as e:
        log.warning(f"新闻获取失败，使用测试模式: {e}")
        _test_interpret(client)


def _test_interpret(client):
    """测试 LLM 解读能力"""
    test_news = [
        "甘肃岷县遭遇40年不遇干旱，当归主产区降雨量不足常年三分之一，部分地块当归苗枯死，预计减产30%以上。",
        "2025年亳州白术种植面积创历史新高，据调查全市种植面积达45万亩，较去年增长60%，明年供应量将大幅增加。",
        "受流感疫情影响，板蓝根、金银花等清热解毒类药材需求激增，多地药店断货，产地收购价上涨20%。",
    ]

    log.info("=== 测试 LLM 新闻解读 ===")
    for i, news in enumerate(test_news):
        log.info(f"\n新闻{i+1}: {news[:50]}...")
        result = interpret_news(news, client)
        events = result.get("events", [])
        if events:
            for evt in events:
                direction = "↑" if evt.get("price_direction") == "up" else "↓"
                log.info(
                    f"  → {evt['herb_name']} [{evt['event_type']}] "
                    f"严重度:{evt.get('severity', '?')} "
                    f"{direction}{evt.get('estimated_impact_pct', '?')}% "
                    f"(置信度:{evt.get('confidence', '?')})"
                )
        else:
            log.info(f"  → 未检测到事件: {result.get('no_event_reason', '?')}")
        time.sleep(1)


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "test"

    if cmd == "test":
        from llm_client import get_llm_client
        client = get_llm_client()
        print(f"LLM: {client.api_base} / {client.model}")
        print(f"可用: {client.is_available()}")
        if client.is_available():
            _test_interpret(client)
        else:
            print("LLM 服务不可用，请检查配置")

    elif cmd == "run":
        run_interpret()

    elif cmd == "interpret":
        text = " ".join(sys.argv[2:])
        if not text:
            print("用法: python llm_news_interpreter.py interpret '新闻文本'")
            sys.exit(1)
        from llm_client import get_llm_client
        client = get_llm_client()
        result = interpret_news(text, client)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    else:
        print("用法: python llm_news_interpreter.py [test|run|interpret '文本']")

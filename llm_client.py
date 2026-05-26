"""LLM 客户端封装

兼容 OpenAI Chat Completions 接口，支持：
- 本地模型 (Ollama, vLLM)
- 云端 API (OpenAI, DeepSeek, 通义千问)

配置通过环境变量或 .env 文件：
    LLM_API_BASE=http://localhost:11434/v1   (Ollama)
    LLM_API_KEY=sk-xxx
    LLM_MODEL=qwen2.5:7b
"""

import os
import json
import logging
import time
from typing import Any
from pathlib import Path

import requests

log = logging.getLogger(__name__)

# 自动加载 .env 文件
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().strip().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip())

# 默认配置
DEFAULT_API_BASE = os.environ.get("LLM_API_BASE", "http://localhost:11434/v1")
DEFAULT_API_KEY = os.environ.get("LLM_API_KEY", "ollama")
DEFAULT_MODEL = os.environ.get("LLM_MODEL", "qwen2.5:7b")


class LLMClient:
    """OpenAI 兼容的 LLM 客户端"""

    def __init__(self, api_base: str = "", api_key: str = "", model: str = ""):
        self.api_base = (api_base or DEFAULT_API_BASE).rstrip("/")
        self.api_key = api_key or DEFAULT_API_KEY
        self.model = model or DEFAULT_MODEL
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        })

    def chat(self, messages: list[dict], temperature: float = 0.3,
             max_tokens: int = 2000, json_mode: bool = False,
             retries: int = 2) -> dict[str, Any]:
        """调用 Chat Completions API

        Returns:
            {"content": str, "usage": {"prompt_tokens": int, "completion_tokens": int}}
        """
        url = f"{self.api_base}/chat/completions"
        payload: dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        if json_mode:
            payload["response_format"] = {"type": "json_object"}

        for attempt in range(retries + 1):
            try:
                resp = self.session.post(url, json=payload, timeout=60)
                resp.raise_for_status()
                data = resp.json()

                content = data["choices"][0]["message"]["content"]
                usage = data.get("usage", {})
                return {
                    "content": content,
                    "usage": {
                        "prompt_tokens": usage.get("prompt_tokens", 0),
                        "completion_tokens": usage.get("completion_tokens", 0),
                    },
                }
            except requests.exceptions.Timeout:
                log.warning(f"LLM 调用超时 (attempt {attempt + 1})")
                if attempt < retries:
                    time.sleep(2 ** attempt)
            except requests.exceptions.HTTPError as e:
                log.error(f"LLM API 错误: {e.response.status_code} {e.response.text[:200]}")
                if e.response.status_code == 429:
                    time.sleep(5)
                elif attempt >= retries:
                    raise
            except Exception as e:
                log.error(f"LLM 调用失败: {e}")
                if attempt >= retries:
                    raise

        raise RuntimeError("LLM 调用失败，已耗尽重试次数")

    def chat_json(self, messages: list[dict], **kwargs) -> dict:
        """调用 LLM 并解析 JSON 响应"""
        result = self.chat(messages, json_mode=True, **kwargs)
        content = result["content"]

        # 尝试直接解析
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            pass

        # 尝试提取 ```json ... ``` 块
        import re
        match = re.search(r"```json\s*(.*?)\s*```", content, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass

        # 尝试找第一个 { 到最后一个 }
        start = content.find("{")
        end = content.rfind("}")
        if start >= 0 and end > start:
            try:
                return json.loads(content[start:end + 1])
            except json.JSONDecodeError:
                pass

        raise ValueError(f"无法解析 LLM JSON 响应: {content[:200]}")

    def is_available(self) -> bool:
        """检查 LLM 服务是否可用"""
        try:
            # 先尝试 /models 端点
            url = f"{self.api_base}/models"
            resp = self.session.get(url, timeout=5)
            if resp.status_code == 200:
                return True
            # 部分 API（如腾讯混元）不支持 /models，用轻量 chat 验证
            result = self.chat(
                [{"role": "user", "content": "hi"}],
                max_tokens=5, retries=0
            )
            return bool(result.get("content"))
        except Exception:
            return False


# 全局单例
_client: LLMClient | None = None


def get_llm_client() -> LLMClient:
    """获取全局 LLM 客户端"""
    global _client
    if _client is None:
        _client = LLMClient()
    return _client


if __name__ == "__main__":
    client = get_llm_client()
    print(f"API Base: {client.api_base}")
    print(f"Model: {client.model}")
    print(f"Available: {client.is_available()}")

    if client.is_available():
        result = client.chat([
            {"role": "system", "content": "你是中药材价格分析师。"},
            {"role": "user", "content": "用一句话分析当归价格近期走势的可能原因。"},
        ])
        print(f"\nResponse: {result['content']}")
        print(f"Tokens: {result['usage']}")

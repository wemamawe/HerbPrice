"""LLM 客户端封装

兼容 OpenAI Chat Completions 接口，支持：
- 本地模型 (Ollama, vLLM)
- 云端 API (OpenAI, DeepSeek, 通义千问, 混元)

配置通过 .env 文件：

  方式一：直接指定（旧方式，兼容）
    LLM_API_BASE=https://api.deepseek.com/v1
    LLM_API_KEY=sk-xxx
    LLM_MODEL=deepseek-chat

  方式二：多 Provider 切换（新方式）
    LLM_PROVIDER=deepseek          # 切换为 deepseek 或 hunyuan

    HUNYUAN_API_BASE=https://tokenhub.tencentmaas.com/v1
    HUNYUAN_API_KEY=sk-xxx
    HUNYUAN_MODEL=hy3-preview

    DEEPSEEK_API_BASE=https://api.deepseek.com/v1
    DEEPSEEK_API_KEY=sk-xxx
    DEEPSEEK_MODEL=deepseek-chat
"""

import os
import json
import logging
import time
from typing import Any
from pathlib import Path

import requests

log = logging.getLogger(__name__)

# 自动加载 .env 文件（每次都重新读取，支持运行时修改）
def _load_env():
    _env_path = Path(__file__).parent / ".env"
    if _env_path.exists():
        for line in _env_path.read_text().strip().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ[key.strip()] = val.strip()

_load_env()

# Provider 路由表
_PROVIDERS = {
    "hunyuan": {
        "base": "HUNYUAN_API_BASE",
        "key":  "HUNYUAN_API_KEY",
        "model":"HUNYUAN_MODEL",
    },
    "deepseek": {
        "base": "DEEPSEEK_API_BASE",
        "key":  "DEEPSEEK_API_KEY",
        "model":"DEEPSEEK_MODEL",
    },
}

def _resolve_config() -> tuple[str, str, str]:
    """根据 LLM_PROVIDER 解析当前使用的 api_base/api_key/model"""
    provider = os.environ.get("LLM_PROVIDER", "").lower()
    if provider in _PROVIDERS:
        p = _PROVIDERS[provider]
        api_base = os.environ.get(p["base"], "")
        api_key  = os.environ.get(p["key"],  "ollama")
        model    = os.environ.get(p["model"], "")
        if api_base:
            return api_base, api_key, model

    # 兼容旧方式：直接读 LLM_API_BASE / LLM_API_KEY / LLM_MODEL
    return (
        os.environ.get("LLM_API_BASE", "http://localhost:11434/v1"),
        os.environ.get("LLM_API_KEY",  "ollama"),
        os.environ.get("LLM_MODEL",    "qwen2.5:7b"),
    )


class LLMClient:
    """OpenAI 兼容的 LLM 客户端"""

    def __init__(self, api_base: str = "", api_key: str = "", model: str = ""):
        if api_base or api_key or model:
            # 显式传参时直接使用
            self.api_base = (api_base or "http://localhost:11434/v1").rstrip("/")
            self.api_key  = api_key or "ollama"
            self.model    = model or "qwen2.5:7b"
        else:
            # 从 .env / 环境变量自动解析
            base, key, mdl = _resolve_config()
            self.api_base = base.rstrip("/")
            self.api_key  = key
            self.model    = mdl
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


# 全局单例（按 provider 分别缓存）
_clients: dict[str, "LLMClient"] = {}


def get_llm_client(provider: str = "") -> "LLMClient":
    """获取 LLM 客户端

    Args:
        provider: 指定 provider（"hunyuan" 或 "deepseek"）。
                  不传则读取 .env 中的 LLM_PROVIDER。
    """
    global _clients

    # 确定 key
    if provider:
        os.environ["LLM_PROVIDER"] = provider
    active_provider = os.environ.get("LLM_PROVIDER", "default")

    if active_provider not in _clients:
        _clients[active_provider] = LLMClient()

    return _clients[active_provider]


def switch_provider(provider: str) -> "LLMClient":
    """切换 LLM Provider，返回新的客户端实例

    用法：
        client = switch_provider("deepseek")
        client = switch_provider("hunyuan")
    """
    # 清除旧缓存，强制重新创建
    _clients.pop(provider, None)
    os.environ["LLM_PROVIDER"] = provider
    client = LLMClient()
    _clients[provider] = client
    log.info(f"已切换到 {provider}: {client.api_base} / {client.model}")
    return client


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

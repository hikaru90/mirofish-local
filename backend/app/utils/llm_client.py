"""
LLM客户端封装
统一使用OpenAI格式调用
"""

import json
import re
import time
import threading
from typing import Optional, Dict, Any, List
from openai import OpenAI

from ..config import Config
from ..utils.logger import get_logger


logger = get_logger('mirofish.llm_client')


class LLMClient:
    """LLM客户端"""
    _rate_lock = threading.Lock()
    _last_request_by_key: Dict[str, float] = {}
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        model: Optional[str] = None
    ):
        self.api_key = api_key or Config.LLM_API_KEY
        self.base_url = base_url or Config.LLM_BASE_URL
        self.model = model or Config.LLM_MODEL_NAME
        
        if not self.api_key:
            raise ValueError("LLM_API_KEY 未配置")
        
        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url,
            timeout=30.0,
            max_retries=0
        )

    def _rate_limit_key(self) -> str:
        """Build per-provider throttle key."""
        api_suffix = (self.api_key or "")[-8:]
        return f"{self.base_url}|{api_suffix}"

    def _wait_for_rate_limit_slot(self):
        """Enforce minimum interval between LLM requests."""
        rps = max(float(Config.LLM_RATE_LIMIT_RPS), 0.0001)
        min_interval = 1.0 / rps
        key = self._rate_limit_key()

        with self._rate_lock:
            now = time.monotonic()
            last_ts = self._last_request_by_key.get(key, 0.0)
            elapsed = now - last_ts
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
                now = time.monotonic()
            self._last_request_by_key[key] = now

    @staticmethod
    def _is_rate_limit_error(exc: Exception) -> bool:
        status_code = getattr(exc, "status_code", None)
        if status_code == 429:
            return True
        text = str(exc).lower()
        return "429" in text or "rate limit" in text

    @staticmethod
    def _extract_retry_after_seconds(exc: Exception) -> float:
        """
        Extract retry delay from provider error payload if available.
        Falls back to 1.0s if no explicit hint is present.
        """
        # Common message shape: "Try again in 9 seconds."
        msg = str(exc)
        match = re.search(r"try again in\s+(\d+)\s+seconds?", msg, flags=re.IGNORECASE)
        if match:
            return float(match.group(1))

        # Common payload fragment: "retryAfter': 9"
        match = re.search(r"retryafter['\"]?\s*[:=]\s*(\d+)", msg, flags=re.IGNORECASE)
        if match:
            return float(match.group(1))

        # OpenAI responses sometimes carry retry-after header.
        response = getattr(exc, "response", None)
        if response is not None:
            headers = getattr(response, "headers", {}) or {}
            retry_after = headers.get("retry-after") or headers.get("Retry-After")
            if retry_after is not None:
                try:
                    return float(retry_after)
                except (TypeError, ValueError):
                    pass

        return 1.0
    
    def chat(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.7,
        max_tokens: int = 4096,
        response_format: Optional[Dict] = None,
        timeout: Optional[float] = None
    ) -> str:
        """
        发送聊天请求
        
        Args:
            messages: 消息列表
            temperature: 温度参数
            max_tokens: 最大token数
            response_format: 响应格式（如JSON模式）
            
        Returns:
            模型响应文本
        """
        kwargs = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        
        if response_format:
            kwargs["response_format"] = response_format
        if timeout is not None:
            kwargs["timeout"] = timeout
        
        max_retries = max(int(Config.LLM_RATE_LIMIT_MAX_RETRIES), 0)
        buffer_seconds = max(float(Config.LLM_RATE_LIMIT_RETRY_BUFFER_SECONDS), 0.0)

        for attempt in range(max_retries + 1):
            try:
                self._wait_for_rate_limit_slot()
                response = self.client.chat.completions.create(**kwargs)
                content = response.choices[0].message.content
                # 部分模型（如MiniMax M2.5）会在content中包含<think>思考内容，需要移除
                content = re.sub(r'<think>[\s\S]*?</think>', '', content).strip()
                return content
            except Exception as e:
                if not self._is_rate_limit_error(e) or attempt >= max_retries:
                    raise

                retry_after = self._extract_retry_after_seconds(e) + buffer_seconds
                logger.warning(
                    "LLM rate limited; retrying in %.2fs (attempt %s/%s)",
                    retry_after,
                    attempt + 1,
                    max_retries + 1,
                )
                time.sleep(retry_after)

        # Defensive fallback (loop either returns or raises)
        raise RuntimeError("Unexpected LLM retry loop termination")
    
    def chat_json(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.3,
        max_tokens: int = 4096,
        timeout: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        发送聊天请求并返回JSON
        
        Args:
            messages: 消息列表
            temperature: 温度参数
            max_tokens: 最大token数
            
        Returns:
            解析后的JSON对象
        """
        response = self.chat(
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            response_format={"type": "json_object"},
            timeout=timeout
        )
        if not response:
            raise ValueError("LLM返回为空，无法解析JSON")

        # 清理markdown代码块标记
        cleaned_response = response.strip()
        cleaned_response = re.sub(r'^```(?:json)?\s*\n?', '', cleaned_response, flags=re.IGNORECASE)
        cleaned_response = re.sub(r'\n?```\s*$', '', cleaned_response)
        cleaned_response = cleaned_response.strip()

        try:
            return json.loads(cleaned_response)
        except json.JSONDecodeError:
            # 兜底：有些模型会在JSON前后附带说明文本，尝试提取首个JSON对象
            start = cleaned_response.find("{")
            end = cleaned_response.rfind("}")
            if start != -1 and end != -1 and end > start:
                candidate = cleaned_response[start:end + 1]
                try:
                    return json.loads(candidate)
                except json.JSONDecodeError:
                    pass
            raise ValueError(f"LLM返回的JSON格式无效: {cleaned_response}")


from __future__ import annotations

import json
import os
import urllib.request
from dataclasses import dataclass
from typing import Any, Optional


@dataclass(frozen=True)
class LLMConfig:
    api_key: str
    model: str = "gpt-4o-mini"
    temperature: float = 0.2
    timeout_s: int = 60


def get_env_config() -> LLMConfig:
    api_key = os.getenv("NPS_LENS_OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY") or ""
    model = os.getenv("NPS_LENS_OPENAI_MODEL") or os.getenv("OPENAI_MODEL") or "gpt-4o-mini"
    temperature = float(os.getenv("NPS_LENS_OPENAI_TEMPERATURE", "0.2"))
    timeout_s = int(os.getenv("NPS_LENS_OPENAI_TIMEOUT_S", "60"))
    return LLMConfig(api_key=api_key, model=model, temperature=temperature, timeout_s=timeout_s)


def chat_completion(*, config: LLMConfig, system: str, user: str) -> str:
    if not config.api_key:
        raise ValueError("Missing OpenAI API key. Set OPENAI_API_KEY or NPS_LENS_OPENAI_API_KEY.")
    url = "https://api.openai.com/v1/chat/completions"
    payload: dict[str, Any] = {
        "model": config.model,
        "temperature": float(config.temperature),
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {config.api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=config.timeout_s) as resp:
            body = resp.read().decode("utf-8")
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(f"LLM request failed: {exc}") from exc

    try:
        obj = json.loads(body)
        return str(obj["choices"][0]["message"]["content"])
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(f"Invalid LLM response: {exc}") from exc

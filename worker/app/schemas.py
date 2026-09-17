from __future__ import annotations

from pydantic import BaseModel, Field


class ChatMessage(BaseModel):
    role: str = "user"
    content: str


class GenerateRequest(BaseModel):
    request_id: str = Field(default="")
    messages: list[ChatMessage] = Field(default_factory=list)
    model: str = "sim"
    max_tokens: int = 64
    temperature: float = 0.0

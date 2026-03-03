from __future__ import annotations

from datetime import date
from typing import Optional

from pydantic import Field

from nps_lens.models.common import CanonicalBase


class Review(CanonicalBase):
    store: str = Field(..., description="app_store|google_play|otro")
    review_date: date
    rating: int = Field(..., ge=1, le=5)
    text: str
    app_version: Optional[str] = None
    device: Optional[str] = None

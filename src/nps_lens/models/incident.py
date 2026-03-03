from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import Field

from nps_lens.models.common import CanonicalBase


class Incident(CanonicalBase):
    incident_id: str
    opened_at: datetime
    closed_at: Optional[datetime] = None
    category: str
    severity: str
    system: Optional[str] = None
    description: str
    segment: Optional[str] = None

from __future__ import annotations

# Fixed internal policy for business-defensible linking.
# The threshold is intentionally conservative, but must preserve enough
# coverage to reconstruct causal journeys with real Helix/VoC evidence.
LINK_MIN_SIMILARITY = 0.15
LINK_TOP_K_PER_INCIDENT = 5
HOTSPOT_MIN_TERM_OCCURRENCES = 3
LINK_MAX_DAYS_APART = 90
# Keep scenario tables complete for normal datasets while bounding API payloads in pathological
# cases. Compact chips and PowerPoint apply their own editorial limits at render time.
LINK_MAX_VISIBLE_INCIDENTS = 50
LINK_MAX_VISIBLE_COMMENTS = 50

from __future__ import annotations

# Fixed internal policy for business-defensible linking.
# The threshold is intentionally conservative, but must preserve enough
# coverage to reconstruct causal journeys with real Helix/VoC evidence.
LINK_MIN_SIMILARITY = 0.15
LINK_TOP_K_PER_INCIDENT = 5
HOTSPOT_MIN_TERM_OCCURRENCES = 3
LINK_MAX_DAYS_APART = 21

from __future__ import annotations

import importlib.util
from functools import lru_cache
from pathlib import Path

import pandas as pd


@lru_cache(maxsize=1)
def _load_streamlit_app_module():
    path = Path(__file__).resolve().parents[1] / "app" / "streamlit_app.py"
    spec = importlib.util.spec_from_file_location("test_streamlit_app", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_annotate_chain_candidates_generates_unique_keys_for_duplicate_rows() -> None:
    streamlit_app = _load_streamlit_app_module()
    chain_df = pd.DataFrame(
        [
            {
                "nps_topic": "Acceso bloqueado",
                "touchpoint": "Login / autenticacion",
                "palanca": "Acceso",
                "subpalanca": "Bloqueo / OTP",
                "linked_incidents": 2,
                "linked_comments": 1,
                "linked_pairs": 2,
                "incident_records": [{"incident_id": "INC001"}],
                "comment_records": [{"comment_id": "NPS001"}],
            },
            {
                "nps_topic": "Acceso bloqueado",
                "touchpoint": "Login / autenticacion",
                "palanca": "Acceso",
                "subpalanca": "Bloqueo / OTP",
                "linked_incidents": 2,
                "linked_comments": 1,
                "linked_pairs": 2,
                "incident_records": [{"incident_id": "INC001"}],
                "comment_records": [{"comment_id": "NPS001"}],
            },
        ]
    )

    out = streamlit_app._annotate_chain_candidates(chain_df)

    assert out["chain_key"].nunique() == len(out)


def test_select_chain_rows_ignores_duplicate_selected_keys() -> None:
    streamlit_app = _load_streamlit_app_module()
    chain_df = pd.DataFrame(
        [
            {
                "nps_topic": "Acceso bloqueado",
                "touchpoint": "Login / autenticacion",
                "palanca": "Acceso",
                "subpalanca": "Bloqueo / OTP",
                "linked_incidents": 2,
                "linked_comments": 1,
                "linked_pairs": 2,
                "incident_records": [{"incident_id": "INC001"}],
                "comment_records": [{"comment_id": "NPS001"}],
            },
            {
                "nps_topic": "Operativa critica fallida",
                "touchpoint": "Pagos / firma",
                "palanca": "Operativa",
                "subpalanca": "Error funcional / timeout",
                "linked_incidents": 3,
                "linked_comments": 2,
                "linked_pairs": 3,
                "incident_records": [{"incident_id": "INC002"}],
                "comment_records": [{"comment_id": "NPS002"}],
            },
        ]
    )

    annotated = streamlit_app._annotate_chain_candidates(chain_df)
    selected_keys = [
        annotated.iloc[0]["chain_key"],
        annotated.iloc[0]["chain_key"],
        annotated.iloc[1]["chain_key"],
    ]

    out = streamlit_app._select_chain_rows(annotated, selected_keys)

    assert out["chain_key"].tolist() == annotated["chain_key"].tolist()


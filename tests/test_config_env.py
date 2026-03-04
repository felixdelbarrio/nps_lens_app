from nps_lens.config import Settings


def test_settings_reads_context_values_from_env(monkeypatch):
    # service_origin_buug values
    monkeypatch.setenv(
        "NPS_LENS_SERVICE_ORIGIN_BUUG",
        "BBVA México, BBVA España",
    )
    # mapping origin -> n1
    monkeypatch.setenv(
        "NPS_LENS_SERVICE_ORIGIN_N1",
        '{"BBVA México": ["Senda", "Helix"], "BBVA España": ["Senda"]}',
    )
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_N2", "SN2A, SN2B")
    monkeypatch.setenv("NPS_LENS_DEFAULT_SERVICE_ORIGIN", "BBVA México")
    monkeypatch.setenv("NPS_LENS_DEFAULT_SERVICE_ORIGIN_N1", "Senda")

    s = Settings.from_env()

    assert s.service_origin_values == ["BBVA México", "BBVA España"]
    assert s.service_origin_n1_map["BBVA México"] == ["Senda", "Helix"]
    assert s.service_origin_n2_values == ["SN2A", "SN2B"]


def test_settings_accepts_compact_n1_format(monkeypatch):
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_BUUG", "BBVA México")
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_N1", "BBVA México:Senda|Helix")
    monkeypatch.delenv("NPS_LENS_SERVICE_ORIGIN_N2", raising=False)

    s = Settings.from_env()
    assert s.service_origin_n1_map["BBVA México"] == ["Senda", "Helix"]


def test_settings_fails_fast_when_required_env_missing(monkeypatch):
    # Missing NPS_LENS_SERVICE_ORIGIN_N1
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_BUUG", "BBVA México")
    monkeypatch.delenv("NPS_LENS_SERVICE_ORIGIN_N1", raising=False)
    monkeypatch.delenv("SERVICE_ORIGIN_N1", raising=False)

    try:
        Settings.from_env()
        assert False, "Expected Settings.from_env() to raise"
    except RuntimeError as e:
        assert "NPS_LENS_SERVICE_ORIGIN_N1" in str(e)


def test_settings_fails_fast_when_n1_map_incomplete(monkeypatch):
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_BUUG", "BBVA México, BBVA España")
    # Map only defines México
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_N1", '{"BBVA México": ["Senda"]}')

    try:
        Settings.from_env()
        assert False, "Expected Settings.from_env() to raise"
    except RuntimeError as e:
        assert "missing entries" in str(e)
        assert "BBVA España" in str(e)

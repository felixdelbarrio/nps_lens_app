from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pytest

from nps_lens.services.chatgpt_browser import ChatGPTBrowserClient, ChatGPTBrowserRun
from nps_lens.services.taxonomy_discovery import (
    ChatGPTDiscoveryConfig,
    ChatGPTTaxonomyDiscoveryProvider,
    DiscoveryErrorCode,
    TaxonomyDiscoveryError,
)


class FakeRun:
    def __init__(self, *, designer: str | None = None, invalid_ids: bool = False) -> None:
        self.designer = designer
        self.invalid_ids = invalid_ids
        self.designer_calls = 0
        self.classifier_calls = 0

    def create_taxonomy(self, prompt: str, designer_url: str) -> str:
        self.designer_calls += 1
        assert designer_url == "https://chatgpt.com/g/designer"
        assert '"Comment"' in prompt
        assert "NPS" not in prompt and "Fecha" not in prompt
        return self.designer or json.dumps(
            {"taxonomy": [{"lever": "Pagos", "sublevers": ["Transferencias"]}]}
        )

    def classify_comments(self, prompt: str, classifier_url: str) -> str:
        self.classifier_calls += 1
        assert classifier_url == "https://chatgpt.com/g/classifier"
        rows = json.loads(prompt.rsplit(". Comentarios: ", 1)[1])
        ids = [row["id"] for row in rows]
        if self.invalid_ids:
            ids[-1] = "unknown"
        return json.dumps(
            {
                "classifications": [
                    {
                        "id": key,
                        "primary_classification": {
                            "lever": "Pagos",
                            "sublever": "Transferencias",
                        },
                    }
                    for key in ids
                ]
            }
        )


class FakeBrowser:
    def __init__(self, run: FakeRun) -> None:
        self.run = run
        self.headless_runs = 0
        self.disconnected = False

    def session_status(self) -> str:
        return "connected"

    def connect(self) -> str:
        return "connected"

    def verify_connection(self) -> str:
        return "connected"

    def disconnect(self) -> None:
        self.disconnected = True

    @contextmanager
    def automation(self) -> Iterator[FakeRun]:
        self.headless_runs += 1
        yield self.run


def provider(run: FakeRun, batch_size: int = 50) -> ChatGPTTaxonomyDiscoveryProvider:
    return ChatGPTTaxonomyDiscoveryProvider(
        FakeBrowser(run),  # type: ignore[arg-type]
        ChatGPTDiscoveryConfig(
            "https://chatgpt.com/g/designer",
            "https://chatgpt.com/g/classifier",
            batch_size,
        ),
    )


def test_designer_then_batched_classifier_preserves_order() -> None:
    run = FakeRun()
    comments = [(f"opaque-{index}", f"comentario {index}") for index in range(120)]
    result = provider(run).discover(comments)
    assert run.designer_calls == 1
    assert run.classifier_calls == 3
    assert result["lever"] == ["Pagos"] * 120
    assert result["sublever"] == ["Transferencias"] * 120


def test_invalid_designer_json_stops_before_classifier() -> None:
    run = FakeRun(designer="```json\n{}\n```")
    with pytest.raises(TaxonomyDiscoveryError) as caught:
        provider(run).discover([("opaque-1", "comentario")])
    assert caught.value.code is DiscoveryErrorCode.INVALID_JSON
    assert run.classifier_calls == 0


def test_unknown_or_missing_classification_ids_are_rejected() -> None:
    run = FakeRun(invalid_ids=True)
    with pytest.raises(TaxonomyDiscoveryError) as caught:
        provider(run).discover([("opaque-1", "uno"), ("opaque-2", "dos")])
    assert caught.value.code is DiscoveryErrorCode.INVALID_CLASSIFICATION


class FakePage:
    url = "https://chatgpt.com/g/designer"
    frames: list[object] = []

    def __init__(self) -> None:
        self.visited: list[str] = []

    def goto(self, url: str, **_kwargs: object) -> None:
        self.visited.append(url)
        return None

    def close(self) -> None:
        return None

    def locator(self, _selector: str) -> "ChallengeBody":
        return ChallengeBody("")


class FakeContext:
    def __init__(self) -> None:
        self.pages = [FakePage()]

    def new_page(self) -> FakePage:
        page = FakePage()
        self.pages.append(page)
        return page


def test_browser_visibility_is_limited_to_explicit_connect(tmp_path: Path, monkeypatch) -> None:
    client = ChatGPTBrowserClient(
        tmp_path / "profile",
        "https://chatgpt.com/g/designer",
        "https://chatgpt.com/g/classifier",
    )
    seen: list[bool] = []
    contexts: list[FakeContext] = []

    @contextmanager
    def fake_context(*, headless: bool) -> Iterator[FakeContext]:
        seen.append(headless)
        client.profile_dir.mkdir(parents=True, exist_ok=True)
        context = FakeContext()
        contexts.append(context)
        yield context

    monkeypatch.setattr(client, "_context", fake_context)
    monkeypatch.setattr(client, "_composer_visible", lambda _page: True)

    assert client.connect() == "connected"
    assert contexts[0].pages[-1].visited == [
        "https://chatgpt.com/",
        "https://chatgpt.com/g/designer",
        "https://chatgpt.com/g/classifier",
    ]
    assert client.verify_connection() == "connected"
    assert client.session_status() == "connected"
    with client.automation():
        pass
    assert seen == [False, False, True, True]


def test_disconnect_removes_complete_dedicated_profile(tmp_path: Path) -> None:
    profile = tmp_path / "profile"
    profile.mkdir()
    (profile / "technical-state").write_text("session", encoding="utf-8")
    client = ChatGPTBrowserClient(
        profile,
        "https://chatgpt.com/g/designer",
        "https://chatgpt.com/g/classifier",
    )
    client.disconnect()
    assert not profile.exists()


class ChallengeFrame:
    url = "https://challenges.cloudflare.com/turnstile/v0/"


class ChallengeBody:
    def __init__(self, text: str = "Verifique que es un ser humano") -> None:
        self.text = text

    def inner_text(self, **_kwargs: object) -> str:
        return self.text


class ChallengePage(FakePage):
    frames = [ChallengeFrame()]

    def __init__(self) -> None:
        self.goto_calls = 0

    def goto(self, *_args: object, **_kwargs: object) -> None:
        self.goto_calls += 1

    def locator(self, _selector: str) -> ChallengeBody:
        return ChallengeBody()


class ChallengeContext:
    def __init__(self) -> None:
        self.page = ChallengePage()
        self.new_page_calls = 0

    def new_page(self) -> ChallengePage:
        self.new_page_calls += 1
        return self.page


def test_cloudflare_is_terminal_without_reload_or_retry() -> None:
    context = ChallengeContext()
    run = ChatGPTBrowserRun(context, 1000)
    with pytest.raises(TaxonomyDiscoveryError) as caught:
        run.create_taxonomy("payload", "https://chatgpt.com/g/designer")
    assert caught.value.code is DiscoveryErrorCode.INTERACTION_REQUIRED
    assert context.new_page_calls == 1
    assert context.page.goto_calls == 1


def test_login_or_mfa_is_interaction_required() -> None:
    class LoginPage(ChallengePage):
        frames: list[object] = []
        url = "https://chatgpt.com/"

        def locator(self, _selector: str) -> ChallengeBody:
            return ChallengeBody("Enter verification code to log in")

    page = LoginPage()
    assert ChatGPTBrowserRun._interaction_required(page)

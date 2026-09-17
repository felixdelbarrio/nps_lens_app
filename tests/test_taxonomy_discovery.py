from __future__ import annotations

import json
from contextlib import contextmanager
from typing import Iterator

import pytest

from nps_lens.services.chatgpt_browser import ChatGPTBrowserClient
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
        self.automation_runs = 0
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
        self.automation_runs += 1
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
    def __init__(self):
        self.visited = []
        self.state = "ready"
        self.responses = []
        self.prompt = ""
        self.sent = []
        self.last = self
        self.response_error = None

    async def goto(self, url, **kwargs):
        self.visited.append(url)
        self.responses = []

    async def evaluate(self, script):
        return self.state

    async def wait_for_function(self, script, **kwargs):
        if "before =>" in script:
            if self.response_error:
                self.state = self.response_error
                self.response_error = None
            else:
                self.responses.append('{"result": "complete"}')

    def locator(self, selector):
        return self

    async def count(self):
        return len(self.responses)

    async def fill(self, prompt):
        self.prompt = prompt

    async def press(self, key):
        self.sent.append(self.prompt)

    async def inner_text(self):
        return self.responses[-1]


class FakeContext:
    def __init__(self):
        self.pages = [FakePage()]
        self.closed = False
        self.windows = []

    async def close(self):
        self.closed = True

    async def new_cdp_session(self, page):
        return self

    async def send(self, command, args=None):
        if args:
            self.windows.append(args["bounds"]["windowState"])
        return {"windowId": 1}

    async def detach(self):
        pass


@pytest.fixture
def browser(monkeypatch):
    from playwright import async_api

    context = FakeContext()
    launches = []

    class Driver:
        chromium = None
        stopped = False

        async def start(self):
            self.chromium = self
            return self

        async def launch_persistent_context(self, profile, **options):
            launches.append((profile, options))
            return context

        async def stop(self):
            self.stopped = True

    driver = Driver()
    monkeypatch.setattr(async_api, "async_playwright", lambda: driver)
    client = ChatGPTBrowserClient(
        "https://chatgpt.com/g/designer", "https://chatgpt.com/g/classifier"
    )
    monkeypatch.setattr(client, "_capture_processes", lambda: None)
    yield client, context, launches, driver
    client.disconnect()


def test_one_sandboxed_installed_chrome_for_whole_session(browser):
    client, context, launches, driver = browser
    assert client.session_status() == "not_connected"
    assert not launches
    assert client.connect() == "connected"
    profile = client.profile_dir
    assert profile.is_dir()
    assert client.verify_connection() == "connected"
    with client.automation() as run:
        run.create_taxonomy("designer bulk", client.designer_url)
        run.classify_comments("classifier bulk", client.classifier_url)
    assert len(launches) == 1
    assert launches[0][1] == dict(channel="chrome", headless=False, chromium_sandbox=True)
    assert context.pages[0].visited == [
        "https://chatgpt.com/",
        client.designer_url,
        client.classifier_url,
        client.designer_url,
        client.classifier_url,
    ]
    assert context.windows == ["minimized"]
    assert not context.closed
    client.disconnect()
    assert context.closed and driver.stopped
    assert not profile.exists()
    assert client.session_status() == "not_connected"
    assert client._loop is None


def test_challenge_keeps_same_window_without_reload(browser):
    client, context, launches, _ = browser
    page = context.pages[0]
    page.state = "interaction"
    for _ in range(2):
        with pytest.raises(TaxonomyDiscoveryError) as error:
            client.connect()
        assert error.value.code == DiscoveryErrorCode.INTERACTION_REQUIRED
    assert len(launches) == 1
    assert page.visited == ["https://chatgpt.com/"]
    assert not context.closed
    page.state = "ready"
    assert client.verify_connection() == "connected"
    assert page.visited.count("https://chatgpt.com/") == 1


def test_mid_response_challenge_resumes_without_resending(browser):
    client, context, launches, _ = browser
    client.connect()
    page = context.pages[0]
    with pytest.raises(TaxonomyDiscoveryError), client.automation() as run:
        run.create_taxonomy("designer", client.designer_url)
        page.response_error = "interaction"
        run.classify_comments("classifier", client.classifier_url)
    visited = list(page.visited)
    page.state = "ready"
    client.verify_connection()
    with client.automation() as run:
        run.create_taxonomy("designer", client.designer_url)
        run.classify_comments("classifier", client.classifier_url)
    assert page.sent == ["designer", "classifier"]
    assert page.visited == visited
    assert len(launches) == 1


@pytest.mark.parametrize(
    "state,code",
    [
        ("policy", DiscoveryErrorCode.CORPORATE_POLICY_BLOCKED),
        ("project", DiscoveryErrorCode.PROJECT_NOT_ACCESSIBLE),
    ],
)
def test_terminal_access_errors_close_resources(browser, state, code):
    client, context, _, driver = browser
    context.pages[0].state = state
    with pytest.raises(TaxonomyDiscoveryError) as error:
        client.connect()
    assert error.value.code == code
    assert context.closed and driver.stopped
    assert client.profile_dir is None


def test_invalid_json_closes_session(browser):
    client, context, _, driver = browser
    client.connect()
    with pytest.raises(TaxonomyDiscoveryError), client.automation():
        raise TaxonomyDiscoveryError(DiscoveryErrorCode.INVALID_JSON, "invalid")
    assert context.closed and driver.stopped
    assert client.profile_dir is None


def test_missing_chrome_does_not_install_or_fallback(browser, monkeypatch):
    client, _, launches, driver = browser

    async def missing(*args, **kwargs):
        raise RuntimeError("Chrome distribution not found")

    monkeypatch.setattr(driver, "launch_persistent_context", missing)
    with pytest.raises(TaxonomyDiscoveryError) as error:
        client.connect()
    assert error.value.code == DiscoveryErrorCode.BROWSER_UNAVAILABLE
    assert not launches and driver.stopped
    assert client.profile_dir is None


def test_automation_does_not_open_browser_without_connect(browser):
    client, _, launches, _ = browser
    with pytest.raises(TaxonomyDiscoveryError) as error, client.automation():
        pass
    assert error.value.code == DiscoveryErrorCode.AUTH_REQUIRED
    assert not launches


def test_disconnect_interrupts_a_pending_wait(browser, monkeypatch):
    import asyncio
    from concurrent.futures import CancelledError, ThreadPoolExecutor
    from threading import Event

    client, context, _, driver = browser
    client.connect()
    entered = Event()

    async def waiting(*args, **kwargs):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(context.pages[0], "wait_for_function", waiting)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(client.create_taxonomy, "pending", client.designer_url)
        assert entered.wait(timeout=5)
        profile = client.profile_dir
        client.disconnect()
        with pytest.raises(CancelledError):
            future.result(timeout=5)
    assert context.closed and driver.stopped
    assert not profile.exists()


def test_project_verification_challenge_does_not_renavigate(browser, monkeypatch):
    client, context, launches, _ = browser
    page = context.pages[0]
    goto = page.goto

    async def challenge(url, **kwargs):
        await goto(url, **kwargs)
        if url == client.designer_url:
            page.state = "interaction"

    monkeypatch.setattr(page, "goto", challenge)
    with pytest.raises(TaxonomyDiscoveryError):
        client.connect()
    page.state = "ready"
    client.verify_connection()
    assert page.visited == ["https://chatgpt.com/", client.designer_url, client.classifier_url]
    assert len(launches) == 1


def test_network_error_does_not_retry_and_closes(browser, monkeypatch):
    client, context, _, driver = browser
    calls = []

    async def failing(*args, **kwargs):
        calls.append(args)
        raise RuntimeError("connection failed")

    monkeypatch.setattr(context.pages[0], "goto", failing)
    with pytest.raises(TaxonomyDiscoveryError) as error:
        client.connect()
    assert error.value.code == DiscoveryErrorCode.CHATGPT_UNAVAILABLE
    assert len(calls) == 1
    assert context.closed and driver.stopped
    assert client.profile_dir is None


def test_missing_completion_is_never_accepted(browser, monkeypatch):
    client, context, _, driver = browser
    client.connect()
    page = context.pages[0]
    wait = page.wait_for_function

    async def incomplete(script, **kwargs):
        if "before =>" in script:
            page.responses.append("partial JSON")
            raise RuntimeError("timeout waiting for completion")
        await wait(script, **kwargs)

    monkeypatch.setattr(page, "wait_for_function", incomplete)
    with pytest.raises(TaxonomyDiscoveryError) as error, client.automation() as run:
        run.create_taxonomy("bulk", client.designer_url)
    assert error.value.code == DiscoveryErrorCode.TIMEOUT
    assert page.sent == ["bulk"]
    assert context.closed and driver.stopped


def test_process_cleanup_targets_only_owned_processes(browser, monkeypatch):
    from unittest.mock import Mock

    import psutil

    client, _, _, _ = browser
    owned = Mock()
    unrelated = Mock()
    client._processes = [owned]
    calls = []

    def wait(processes, timeout):
        calls.append(list(processes))
        return ([], list(processes)) if len(calls) < 3 else (list(processes), [])

    monkeypatch.setattr(psutil, "wait_procs", wait)
    client._reap_processes()
    owned.terminate.assert_called_once()
    owned.kill.assert_called_once()
    unrelated.terminate.assert_not_called()
    assert all(processes == [owned] for processes in calls)


def test_disconnect_cleans_driver_profile_and_thread_even_when_context_close_fails(
    browser, monkeypatch
):
    client, context, _, driver = browser
    client.connect()
    profile = client.profile_dir

    async def failed_close():
        raise RuntimeError("context already lost")

    monkeypatch.setattr(context, "close", failed_close)
    with pytest.raises(RuntimeError, match="already lost"):
        client.disconnect()
    assert driver.stopped
    assert not profile.exists()
    assert client._loop is None


def test_shutdown_keeps_driver_transport_alive_until_context_closes(browser, monkeypatch):
    import asyncio

    client, context, _, _ = browser
    client.connect()

    async def transport():
        await asyncio.Event().wait()

    async def setup():
        return asyncio.create_task(transport())

    task = client._call(setup())
    original = context.close

    async def close():
        assert not task.cancelled()
        await original()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    monkeypatch.setattr(context, "close", close)
    client.disconnect()

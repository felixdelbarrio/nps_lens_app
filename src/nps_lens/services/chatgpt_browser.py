from __future__ import annotations

import asyncio
import shutil
import tempfile
import threading
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any, Coroutine, Iterator, Optional

import psutil

from nps_lens.services.taxonomy_discovery import DiscoveryErrorCode, TaxonomyDiscoveryError

# Inspect only visible controls, not assistant text (which may discuss login).
_ACCESS = """() => {
    const visible = s => [...document.querySelectorAll(s)].some(e => e.getClientRects().length);
    if (location.href.startsWith('chrome-error:') ||
        /ERR_BLOCKED_BY_ADMINISTRATOR|blocked by your administrator/i.test(document.body.innerText))
        return 'policy';
    if (!visible('#prompt-textarea') &&
        /you do not have access|gpt inaccessible or not found|no tienes acceso/i.test(document.body.innerText))
        return 'project';
    if (location.hostname !== 'chatgpt.com' || location.pathname.includes('/auth/') ||
        visible('iframe[src*="challenges.cloudflare.com"], iframe[src*="turnstile"], #challenge-running') ||
        /^(Just a moment|Un momento)/i.test(document.title))
        return 'interaction';
    if (visible('[data-testid="login-button"], [data-testid="signup-button"]'))
        return 'interaction';
    return visible('#prompt-textarea') && visible('[data-testid="accounts-profile-button"]')
        ? 'ready' : 'loading';
}"""

_COMPLETE = """before => {
    const access = (ACCESS)();
    if (['interaction', 'policy', 'project'].includes(access)) return access;
    const messages = document.querySelectorAll('[data-message-author-role="assistant"]');
    if (messages.length <= before) return false;
    const turn = messages[messages.length - 1].closest('article');
    return !!turn?.querySelector('[data-testid="copy-turn-action-button"]') &&
        !document.querySelector('[data-testid="stop-button"]');
}""".replace(
    "ACCESS", _ACCESS
)


class ChatGPTBrowserClient:
    """One temporary Chrome session, owned by one asynchronous event-loop thread.

    HTTP worker threads never touch Playwright objects. Disconnect can close the
    context while another request is waiting for human input or a response.
    """

    def __init__(
        self, designer_url: str, classifier_url: str, *, timeout_ms: int = 120_000
    ) -> None:
        self.designer_url = designer_url
        self.classifier_url = classifier_url
        self.timeout_ms = timeout_ms
        self.profile_dir: Optional[Path] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()
        self._operations = threading.Lock()
        self._context: Any = None
        self._driver: Any = None
        self._page: Any = None
        self._status = "not_connected"
        self._verified: set[str] = set()
        self._verification_url: Optional[str] = None
        self._completed: dict[tuple[str, str], str] = {}
        self._pending: Optional[tuple[str, str]] = None
        self._sent = False
        self._before = 0
        self._processes: list[psutil.Process] = []

    def _call(self, operation: Coroutine[Any, Any, Any]) -> Any:
        with self._lock:
            if self._loop is None:
                self._loop = asyncio.new_event_loop()
                self._thread = threading.Thread(
                    target=self._loop.run_forever, name="nps-chatgpt", daemon=True
                )
                self._thread.start()
            future = asyncio.run_coroutine_threadsafe(operation, self._loop)
        return future.result()

    def session_status(self) -> str:
        return self._status

    async def _window(self, state: str) -> None:
        session = None
        try:
            session = await self._context.new_cdp_session(self._page)
            window = await session.send("Browser.getWindowForTarget")
            await session.send(
                "Browser.setWindowBounds",
                {"windowId": window["windowId"], "bounds": {"windowState": state}},
            )
        except Exception:
            pass  # Optional UX; corporate policy may disable CDP window management.
        finally:
            if session is not None:
                with suppress(Exception):
                    await session.detach()

    async def _access(self) -> None:
        state = await self._page.evaluate(_ACCESS)
        if state == "policy":
            raise TaxonomyDiscoveryError(
                DiscoveryErrorCode.CORPORATE_POLICY_BLOCKED,
                "Una política corporativa bloquea Chrome o ChatGPT. Consulta con TI.",
            )
        if state == "project":
            raise TaxonomyDiscoveryError(
                DiscoveryErrorCode.PROJECT_NOT_ACCESSIBLE,
                "Esta cuenta no tiene acceso al proyecto de ChatGPT.",
            )
        if state == "loading":
            raise TaxonomyDiscoveryError(
                DiscoveryErrorCode.UI_CHANGED,
                "No se pudo verificar una página autenticada de ChatGPT. Revisa su interfaz.",
            )
        if state == "interaction":
            self._status = "interaction_required"
            await self._window("normal")
            raise TaxonomyDiscoveryError(
                DiscoveryErrorCode.INTERACTION_REQUIRED,
                "Completa login, MFA o verificación humana en la misma ventana Chrome "
                "y pulsa Verificar conexión. No cierres ni recargues la ventana.",
            )

    async def _ready(self) -> None:
        try:
            await self._page.wait_for_function(
                "() => (" + _ACCESS + ")() !== 'loading'",
                polling=1000,
                timeout=self.timeout_ms,
            )
        except Exception:
            await self._access()
            raise
        await self._access()

    async def _start(self) -> None:
        from playwright.async_api import async_playwright

        self.profile_dir = Path(tempfile.mkdtemp(prefix="nps-lens-chatgpt-"))
        self.profile_dir.chmod(0o700)
        try:
            self._driver = await async_playwright().start()
            self._context = await self._driver.chromium.launch_persistent_context(
                str(self.profile_dir),
                channel="chrome",
                headless=False,
                chromium_sandbox=True,
            )
            self._capture_processes()
            self._page = (
                self._context.pages[0] if self._context.pages else await self._context.new_page()
            )
            for page in self._context.pages:
                if page is not self._page:
                    await page.close()
        except Exception as exc:
            message = str(exc).casefold()
            await self._close()
            code = DiscoveryErrorCode.CHATGPT_UNAVAILABLE
            if "not found" in message or "doesn't exist" in message:
                code = DiscoveryErrorCode.BROWSER_UNAVAILABLE
            elif "policy" in message or "administrator" in message:
                code = DiscoveryErrorCode.CORPORATE_POLICY_BLOCKED
            raise TaxonomyDiscoveryError(
                code,
                "No se pudo iniciar Google Chrome instalado. "
                "No se instalará otro navegador ni se modificarán controles de seguridad.",
            ) from exc
        self._verification_url = "https://chatgpt.com/"
        await self._page.goto(
            self._verification_url, wait_until="domcontentloaded", timeout=self.timeout_ms
        )

    async def _verify(self) -> str:
        if self._context is None:
            await self._start()
        if self._status == "connected":
            await self._access()
            return self._status
        # Resume the current page before navigating anywhere: never reload a challenge.
        await self._ready()
        if self._verification_url:
            self._verified.add(self._verification_url)
        for url in (self.designer_url, self.classifier_url):
            if url not in self._verified:
                self._verification_url = url
                await self._page.goto(url, wait_until="domcontentloaded", timeout=self.timeout_ms)
                await self._ready()
                self._verified.add(url)
        self._verification_url = None
        self._status = "connected"
        await self._window("minimized")
        return self._status

    async def _guard(self, operation: Coroutine[Any, Any, Any]) -> Any:
        try:
            return await operation
        except TaxonomyDiscoveryError as exc:
            if exc.code != DiscoveryErrorCode.INTERACTION_REQUIRED:
                await self._close()
            raise
        except Exception as exc:
            if "ERR_BLOCKED_BY_ADMINISTRATOR" in str(exc):
                await self._close()
                raise TaxonomyDiscoveryError(
                    DiscoveryErrorCode.CORPORATE_POLICY_BLOCKED,
                    "Una política corporativa bloquea ChatGPT. Consulta con TI.",
                ) from exc
            # Navigation timeouts can be challenges, never blindly retry.
            if self._page is not None:
                try:
                    await self._access()
                except TaxonomyDiscoveryError as access_error:
                    if access_error.code == DiscoveryErrorCode.INTERACTION_REQUIRED:
                        raise access_error from exc
                    await self._close()
                    raise access_error from exc
                except Exception:
                    pass
            await self._close()
            code = (
                DiscoveryErrorCode.TIMEOUT
                if "timeout" in str(exc).lower()
                else DiscoveryErrorCode.CHATGPT_UNAVAILABLE
            )
            raise TaxonomyDiscoveryError(
                code, "ChatGPT no completó la operación. La sesión se ha cerrado."
            ) from exc

    def connect(self) -> str:
        with self._operations:
            return str(self._call(self._guard(self._verify())))

    def verify_connection(self) -> str:
        return self.connect()

    async def _conversation(self, prompt: str, url: str) -> str:
        key = (url, prompt)
        if key in self._completed:
            return self._completed[key]
        if self._pending is not None and self._pending != key:
            raise TaxonomyDiscoveryError(
                DiscoveryErrorCode.INTERACTION_REQUIRED,
                "Reanuda la operación pendiente o desconecta antes de iniciar otra.",
            )
        if self._pending is None:
            self._pending = key
            self._sent = False
            await self._page.goto(url, wait_until="domcontentloaded", timeout=self.timeout_ms)
        await self._ready()
        messages = self._page.locator('[data-message-author-role="assistant"]')
        if not self._sent:
            self._before = await messages.count()
            if self._before:
                raise TaxonomyDiscoveryError(
                    DiscoveryErrorCode.UI_CHANGED,
                    "El proyecto no abrió una conversación limpia. Revisa la URL configurada.",
                )
            composer = self._page.locator("#prompt-textarea")
            await composer.fill(prompt)
            # Mark before submit: an ambiguous timeout must never duplicate a prompt.
            self._sent = True
            await composer.press("Enter")
        await self._page.wait_for_function(
            _COMPLETE, arg=self._before, polling=1000, timeout=self.timeout_ms
        )
        await self._access()
        response = (await messages.last.inner_text()).strip()
        if not response:
            raise TaxonomyDiscoveryError(
                DiscoveryErrorCode.UI_CHANGED, "Respuesta vacía de ChatGPT."
            )
        self._completed[key] = response
        self._pending = None
        return str(response)

    def create_taxonomy(self, prompt: str, designer_url: str) -> str:
        return str(self._call(self._guard(self._conversation(prompt, designer_url))))

    def classify_comments(self, prompt: str, classifier_url: str) -> str:
        return str(self._call(self._guard(self._conversation(prompt, classifier_url))))

    async def _clear_run(self) -> None:
        self._completed.clear()
        self._pending = None
        self._sent = False

    def _capture_processes(self) -> None:
        if self.profile_dir is None:
            return
        marker = "--user-data-dir=" + str(self.profile_dir)
        for process in psutil.process_iter(["cmdline"]):
            try:
                if marker in (process.info["cmdline"] or []):
                    self._processes = [process, *process.children(recursive=True)]
                    return
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

    def _reap_processes(self) -> None:
        _, alive = psutil.wait_procs(self._processes, timeout=2)
        for process in alive:
            with suppress(psutil.NoSuchProcess):
                process.terminate()
        _, alive = psutil.wait_procs(alive, timeout=2)
        for process in alive:
            with suppress(psutil.NoSuchProcess):
                process.kill()
        _, alive = psutil.wait_procs(alive, timeout=2)
        if alive:
            raise RuntimeError("No se pudo cerrar un proceso Chrome de NPS Lens.")
        self._processes.clear()

    async def _close(self) -> None:
        self._capture_processes()
        context, driver = self._context, self._driver
        self._context = self._driver = self._page = None
        self._status = "not_connected"
        self._verified.clear()
        self._verification_url = None
        await self._clear_run()
        try:
            if context is not None:
                await context.close()
        finally:
            try:
                if driver is not None:
                    await driver.stop()
            finally:
                await asyncio.to_thread(self._reap_processes)
                if self.profile_dir is not None:
                    shutil.rmtree(self.profile_dir)
                    self.profile_dir = None

    async def _shutdown(self) -> None:
        pending = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        await self._close()

    def disconnect(self) -> None:
        with self._lock:
            if self._loop is None:
                return
            loop, thread = self._loop, self._thread
            asyncio.run_coroutine_threadsafe(self._shutdown(), loop).result()
            loop.call_soon_threadsafe(loop.stop)
            if thread is not None:
                thread.join()
            loop.close()
            self._loop = self._thread = None

    @contextmanager
    def automation(self) -> Iterator[ChatGPTBrowserClient]:
        with self._operations:
            if self._status != "connected":
                code = (
                    DiscoveryErrorCode.INTERACTION_REQUIRED
                    if self._status == "interaction_required"
                    else DiscoveryErrorCode.AUTH_REQUIRED
                )
                raise TaxonomyDiscoveryError(code, "Conecta o verifica ChatGPT antes de generar.")
            try:
                yield self
            except TaxonomyDiscoveryError as exc:
                if exc.code != DiscoveryErrorCode.INTERACTION_REQUIRED:
                    self.disconnect()
                raise
            except Exception:
                self.disconnect()
                raise
            else:
                self._call(self._clear_run())

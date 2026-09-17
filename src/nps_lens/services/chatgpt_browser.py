from __future__ import annotations

import os
import shutil
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Optional
from urllib.parse import urlparse

from nps_lens.services.taxonomy_discovery import DiscoveryErrorCode, TaxonomyDiscoveryError


class ChatGPTBrowserRun:
    def __init__(self, context: Any, timeout_ms: int) -> None:
        self.context = context
        self.timeout_ms = timeout_ms

    @staticmethod
    def _auth_url(url: str) -> bool:
        lowered = url.casefold()
        parsed = urlparse(lowered)
        host = (parsed.hostname or "").casefold()
        path = parsed.path.casefold()
        return "/auth/" in path or host == "auth.openai.com"

    @classmethod
    def _interaction_required(cls, page: Any) -> bool:
        if cls._auth_url(page.url):
            return True
        try:
            frame_urls = [str(frame.url).casefold() for frame in page.frames]
            if any(
                token in url
                for url in frame_urls
                for token in ("challenges.cloudflare.com", "turnstile", "/cdn-cgi/challenge")
            ):
                return True
            text = page.locator("body").inner_text(timeout=3000).casefold()
        except Exception:
            return False
        return any(
            token in text
            for token in (
                "captcha",
                "turnstile",
                "verify you are human",
                "verifique que es un ser humano",
                "checking your browser",
                "comprobando su navegador",
                "multi-factor authentication",
                "verification code",
                "código de verificación",
                "iniciar sesión",
                "log in",
            )
        )

    def _assert_access(self, page: Any) -> None:
        if self._interaction_required(page):
            raise TaxonomyDiscoveryError(
                DiscoveryErrorCode.INTERACTION_REQUIRED,
                "ChatGPT requiere login, MFA o verificación humana. Usa Verificar conexión.",
            )
        body = page.locator("body")
        text = body.inner_text(timeout=5000).casefold()
        if any(
            token in text
            for token in (
                "you do not have access",
                "no tienes acceso",
                "gpt inaccessible or not found",
            )
        ):
            raise TaxonomyDiscoveryError(
                DiscoveryErrorCode.PROJECT_NOT_ACCESSIBLE,
                "El proyecto de ChatGPT no está accesible para esta sesión.",
            )

    def _composer(self, page: Any) -> Any:
        candidates = [
            page.get_by_role("textbox", name="Message ChatGPT"),
            page.get_by_role("textbox", name="Enviar mensaje"),
            page.locator("textarea[placeholder*='Message']"),
            page.locator("textarea[placeholder*='mensaje']"),
            page.locator("#prompt-textarea"),
        ]
        for candidate in candidates:
            try:
                if candidate.first.is_visible(timeout=1500):
                    return candidate.first
            except Exception:
                continue
        self._assert_access(page)
        raise TaxonomyDiscoveryError(
            DiscoveryErrorCode.UI_CHANGED,
            "La interfaz de ChatGPT ha cambiado y no se encontró el campo de mensaje.",
        )

    @staticmethod
    def _assistant_messages(page: Any) -> Any:
        return page.locator('[data-message-author-role="assistant"]')

    def _send_and_read(self, page: Any, prompt: str) -> str:
        messages = self._assistant_messages(page)
        before = messages.count()
        composer = self._composer(page)
        composer.fill(prompt)
        composer.press("Enter")
        try:
            page.wait_for_function(
                "count => document.querySelectorAll('[data-message-author-role=\"assistant\"]').length > count",
                arg=before,
                timeout=self.timeout_ms,
            )
            stop = page.get_by_role("button", name="Stop generating")
            stop_es = page.get_by_role("button", name="Detener generación")
            try:
                if stop.is_visible(timeout=1000):
                    stop.wait_for(state="hidden", timeout=self.timeout_ms)
                elif stop_es.is_visible(timeout=1000):
                    stop_es.wait_for(state="hidden", timeout=self.timeout_ms)
            except Exception:
                # Some ChatGPT builds never render a stop control for short responses.
                pass
            page.wait_for_timeout(500)
            response = messages.last.inner_text(timeout=5000).strip()
        except TaxonomyDiscoveryError:
            raise
        except Exception as exc:
            self._assert_access(page)
            raise TaxonomyDiscoveryError(
                DiscoveryErrorCode.TIMEOUT, "ChatGPT no completó la respuesta a tiempo."
            ) from exc
        if not response:
            raise TaxonomyDiscoveryError(
                DiscoveryErrorCode.UI_CHANGED, "No se pudo leer la respuesta de ChatGPT."
            )
        return str(response)

    def _conversation(self, prompt: str, url: str) -> str:
        for attempt in range(2):
            page = self.context.new_page()
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=self.timeout_ms)
                self._assert_access(page)
                return self._send_and_read(page, prompt)
            except TaxonomyDiscoveryError as exc:
                if attempt == 0 and exc.code in {
                    DiscoveryErrorCode.TIMEOUT,
                    DiscoveryErrorCode.CHATGPT_UNAVAILABLE,
                }:
                    continue
                raise
            except Exception as exc:
                if attempt == 0:
                    continue
                raise TaxonomyDiscoveryError(
                    DiscoveryErrorCode.CHATGPT_UNAVAILABLE,
                    "No se pudo comunicar con ChatGPT mediante HTTPS.",
                ) from exc
            finally:
                page.close()
        raise AssertionError("unreachable")

    def create_taxonomy(self, prompt: str, designer_url: str) -> str:
        return self._conversation(prompt, designer_url)

    def classify_comments(self, prompt: str, classifier_url: str) -> str:
        return self._conversation(prompt, classifier_url)


class ChatGPTBrowserClient:
    """Owns all Playwright/DOM details and a minimal dedicated browser profile."""

    def __init__(
        self,
        profile_dir: Path,
        designer_url: str,
        classifier_url: str,
        *,
        timeout_ms: int = 120_000,
        login_timeout_s: int = 300,
    ) -> None:
        self.profile_dir = profile_dir
        self.designer_url = designer_url
        self.classifier_url = classifier_url
        self.timeout_ms = timeout_ms
        self.login_timeout_s = login_timeout_s
        self._lock = threading.RLock()
        self._active_context: Optional[Any] = None

    def _prepare_profile(self) -> None:
        self.profile_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.profile_dir.chmod(0o700)

    def _minimize_profile(self) -> None:
        """Discard browsing residue while retaining ChatGPT's technical login state."""
        disposable = (
            "Default/Cache",
            "Default/Code Cache",
            "Default/GPUCache",
            "Default/History",
            "Default/History-journal",
            "Default/Download Metadata",
            "Default/IndexedDB",
            "Default/Local Storage",
            "Default/Session Storage",
            "Default/Sessions",
            "Default/Web Data",
            "Default/Web Data-journal",
            "BrowserMetrics",
            "Crashpad",
        )
        for relative in disposable:
            target = self.profile_dir / relative
            try:
                if target.is_dir():
                    shutil.rmtree(target)
                elif target.exists():
                    target.unlink()
            except OSError:
                # Cleanup must never hide the operation's real result.
                continue

    @contextmanager
    def _context(self, *, headless: bool) -> Iterator[Any]:
        os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "0")
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise TaxonomyDiscoveryError(
                DiscoveryErrorCode.CHATGPT_UNAVAILABLE,
                "Playwright no está instalado en la aplicación local.",
            ) from exc
        self._prepare_profile()
        playwright = sync_playwright().start()
        context: Optional[Any] = None
        try:
            # TLS, proxy, user-agent and browser defaults are deliberately untouched.
            context = playwright.chromium.launch_persistent_context(
                str(self.profile_dir), headless=headless
            )
            self._active_context = context
            yield context
        except TaxonomyDiscoveryError:
            raise
        except Exception as exc:
            raise TaxonomyDiscoveryError(
                DiscoveryErrorCode.CHATGPT_UNAVAILABLE,
                "Chromium no pudo iniciarse o fue bloqueado por el entorno corporativo.",
            ) from exc
        finally:
            self._active_context = None
            try:
                if context is not None:
                    context.close()
            finally:
                playwright.stop()
                self._minimize_profile()

    @staticmethod
    def _composer_visible(page: Any) -> bool:
        selectors = (
            "#prompt-textarea",
            "textarea[placeholder*='Message']",
            "textarea[placeholder*='mensaje']",
        )
        return any(page.locator(selector).first.is_visible(timeout=500) for selector in selectors)

    def _status_in_context(self, context: Any) -> str:
        page = context.new_page()
        try:
            page.goto("https://chatgpt.com/", wait_until="domcontentloaded", timeout=self.timeout_ms)
            if ChatGPTBrowserRun._interaction_required(page):
                return "interaction_required"
            try:
                return "connected" if self._composer_visible(page) else "expired"
            except Exception:
                return "expired"
        finally:
            page.close()

    def session_status(self) -> str:
        if not self.profile_dir.exists():
            return "not_connected"
        with self._lock, self._context(headless=True) as context:
            return self._status_in_context(context)

    def _visible_verification(self) -> str:
        with self._lock, self._context(headless=False) as context:
            page = context.new_page()
            try:
                page.goto("https://chatgpt.com/", wait_until="domcontentloaded", timeout=self.timeout_ms)
                self._wait_for_manual_access(context)
                for project_url in (self.designer_url, self.classifier_url):
                    active = context.pages[-1]
                    active.goto(project_url, wait_until="domcontentloaded", timeout=self.timeout_ms)
                    self._wait_for_manual_access(context, check_project=True)
                return "connected"
            finally:
                page.close()

    def _wait_for_manual_access(self, context: Any, *, check_project: bool = False) -> None:
        deadline = time.monotonic() + self.login_timeout_s
        while time.monotonic() < deadline:
            active = context.pages[-1]
            if check_project and not ChatGPTBrowserRun._interaction_required(active):
                ChatGPTBrowserRun(context, self.timeout_ms)._assert_access(active)
            try:
                if self._composer_visible(active):
                    return
            except Exception:
                pass
            active.wait_for_timeout(500)
        active = context.pages[-1]
        if ChatGPTBrowserRun._interaction_required(active):
            raise TaxonomyDiscoveryError(
                DiscoveryErrorCode.INTERACTION_REQUIRED,
                "Completa manualmente el login, MFA o challenge y vuelve a verificar.",
            )
        raise TaxonomyDiscoveryError(
            DiscoveryErrorCode.TIMEOUT,
            "No se completó el acceso a ChatGPT dentro del tiempo disponible.",
        )

    def connect(self) -> str:
        return self._visible_verification()

    def verify_connection(self) -> str:
        return self._visible_verification()

    def disconnect(self) -> None:
        with self._lock:
            if self._active_context is not None:
                self._active_context.close()
                self._active_context = None
            if self.profile_dir.exists():
                shutil.rmtree(self.profile_dir)

    @contextmanager
    def automation(self) -> Iterator[ChatGPTBrowserRun]:
        if not self.profile_dir.exists():
            raise TaxonomyDiscoveryError(
                DiscoveryErrorCode.AUTH_REQUIRED, "Conecta ChatGPT antes de descubrir la taxonomía."
            )
        with self._lock, self._context(headless=True) as context:
            status = self._status_in_context(context)
            if status == "interaction_required":
                raise TaxonomyDiscoveryError(
                    DiscoveryErrorCode.INTERACTION_REQUIRED,
                    "ChatGPT requiere interacción humana. Usa Verificar conexión.",
                )
            if status != "connected":
                raise TaxonomyDiscoveryError(
                    DiscoveryErrorCode.AUTH_REQUIRED,
                    "La sesión de ChatGPT ha caducado. Vuelve a conectar.",
                )
            yield ChatGPTBrowserRun(context, self.timeout_ms)

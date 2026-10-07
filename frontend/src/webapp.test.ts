import { readFileSync } from 'node:fs';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';

const source = readFileSync('../webapp/apps-script/App.html', 'utf8')
  .replace(/^<script>\s*|\s*<\/script>\s*$/g, '');
const index = readFileSync('../webapp/apps-script/Index.html', 'utf8');

async function open(viewer: Record<string, unknown> = {}, publication = '{}') {
  document.documentElement.innerHTML = index
    .replace(/<\?!= include\('[^']+'\) \?>/g, '')
    .replace('<?!= publicationJson ?>', publication)
    .replace('<?!= viewerJson ?>', JSON.stringify({local: true, isAdmin: true, ...viewer}));
  await window.eval(source);
}

beforeEach(() => {
  vi.useFakeTimers();
  history.replaceState(null, '', '/');
});
afterEach(() => {
  vi.clearAllTimers();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

it('starts with NPS visible or hidden and keeps navigation usable', async () => {
  for (const visible of [true, false]) {
    await open({evolutionNpsVisible: visible});
    expect(document.getElementById('screen-title')?.textContent)
      .toBe(visible ? 'Evolución NPS' : 'Comentarios');
    expect(document.getElementById('content')?.innerHTML).not.toBe('');
  }
});

it('saves both toggle states without reloading and updates public navigation', async () => {
  await open();
  document.querySelector<HTMLButtonElement>('[data-admin-route="settings"]')!.click();
  await vi.runOnlyPendingTimersAsync();
  for (const visible of [false, true]) {
    document.querySelector<HTMLInputElement>('#evolution-visible')!.checked = visible;
    document.querySelector<HTMLButtonElement>('#evolution-save')!.click();
    await vi.runOnlyPendingTimersAsync();
    expect(document.querySelector<HTMLButtonElement>('#evolution-save')!.disabled).toBe(false);
    document.querySelector<HTMLButtonElement>('[data-area="insights"]')!.click();
    expect(Boolean(document.querySelector('[data-section="summary"]'))).toBe(visible);
    document.querySelector<HTMLButtonElement>('[data-admin-route="settings"]')!.click();
    await vi.runOnlyPendingTimersAsync();
    expect(document.querySelector<HTMLInputElement>('#evolution-visible')!.checked).toBe(visible);
  }
});

it.each([
  [false, 'https://docs.google.com/presentation/d/report_1/edit?usp=sharing', true],
  [false, 'https://docs.google.com.evil.test/presentation/d/report/edit', false],
  [false, 'javascript:alert(1)', false],
  [false, 'informe.pptx', false],
  [true, 'informe.pptx', true],
  [true, '../informe.pptx', false],
])('validates report links (local=%s, url=%s)', async (local, reportUrl, accepted) => {
  await open({local, reportUrl});
  const link = document.querySelector<HTMLAnchorElement>('#report-link')!;
  expect(link.hidden).toBe(!accepted);
  expect(link.hasAttribute('href')).toBe(accepted);
});

it('shows bootstrap errors as text instead of leaving an empty screen', async () => {
  await open({}, '{');
  expect(document.querySelector('#content .error')?.textContent)
    .toContain('No se ha podido abrir la edición:');
});

it.each([false, true])('loads a deferred snapshot and reports failures (failure=%s)', async fail => {
  vi.stubGlobal('DecompressionStream', undefined);
  let success: (value: unknown) => void;
  let failure: (error: Error) => void;
  const run = {
    withSuccessHandler(callback: typeof success) { success = callback; return this; },
    withFailureHandler(callback: typeof failure) { failure = callback; return this; },
    getPublishedShell: vi.fn(() => fail ? failure(new Error('Snapshot no disponible')) : success({payload: {screens: {}}})),
  };
  vi.stubGlobal('google', {script: {run}});
  await open({local: false, shellDeferred: true, scopeKey: 'edition'});
  expect(run.getPublishedShell).toHaveBeenCalledWith('edition', false);
  const content = document.getElementById('content')!;
  if (fail) expect(content.textContent).toContain('Snapshot no disponible');
  else expect(content.querySelector('.context-card')).not.toBeNull();
});

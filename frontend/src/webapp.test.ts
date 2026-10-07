import { readFileSync } from 'node:fs';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';

const index = readFileSync('../webapp/apps-script/Index.html', 'utf8');

async function open(viewer: Record<string, unknown> = {}, publication = '{}') {
  document.documentElement.innerHTML = index
    .replace(/<\?!= include\('([^']+)'\) \?>/g,
      (_, name: string) => readFileSync('../webapp/apps-script/' + name + '.html', 'utf8'))
    .replace('<?!= publicationJson ?>', publication)
    .replace('<?!= viewerJson ?>', JSON.stringify({local: true, isAdmin: true, ...viewer}));
  const scripts = document.querySelectorAll<HTMLScriptElement>('script:not([src]):not([type="application/json"])');
  for (const script of scripts) await window.eval(script.textContent || '');
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

it('waits for persisted visibility before allowing a change', async () => {
  let success: (value: unknown) => void;
  let resolveSettings: typeof success;
  const run = {
    withSuccessHandler(callback: typeof success) { success = callback; return this; },
    withFailureHandler() { return this; },
    getEvolutionNpsSettings() { resolveSettings = success; },
    saveEvolutionNpsSettings: vi.fn((visible: boolean) => success({visible, reportUrl: ''})),
  };
  vi.stubGlobal('google', {script: {run}});
  await open({local: false});
  document.querySelector<HTMLButtonElement>('[data-admin-route="settings"]')!.click();
  await vi.advanceTimersByTimeAsync(0);
  const input = document.querySelector<HTMLInputElement>('#evolution-visible')!;
  const button = document.querySelector<HTMLButtonElement>('#evolution-save')!;
  expect(input.disabled).toBe(true);
  expect(button.disabled).toBe(true);
  resolveSettings!({visible: false});
  await vi.advanceTimersByTimeAsync(0);
  expect(input.disabled).toBe(false);
  expect(input.checked).toBe(false);
  expect(document.getElementById('evolution-status')?.textContent).toContain('oculta');
  input.click();
  expect(document.getElementById('evolution-status')?.textContent).toContain('Sin guardar:');
  button.click();
  await vi.advanceTimersByTimeAsync(0);
  expect(run.saveEvolutionNpsSettings).toHaveBeenCalledWith(true, '');
  expect(document.getElementById('evolution-status')?.textContent).toContain('visible');
});

it.each([
  [false, 'https://docs.google.com/presentation/d/report_1/edit?usp=sharing', true],
  [false, 'https://docs.google.com.evil.test/presentation/d/report/edit', false],
  [false, 'https://docs.google.com@evil.test/presentation/d/report/edit', false],
  [false, 'https://docs.google.com/presentation/d/report%22/edit', false],
  [false, 'https://docs.google.com/presentation/d//edit', false],
  [false, 'https://docs.google.com/presentation/d/report_1', true],
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

it('starts and records activity without crypto.randomUUID', async () => {
  const getRandomValues = vi.fn((bytes: Uint8Array) => bytes.fill(7));
  vi.stubGlobal('crypto', {getRandomValues});
  let success: (value: unknown) => void;
  const run = {
    withSuccessHandler(callback: typeof success) { success = callback; return this; },
    withFailureHandler() { return this; },
    recordActivityEvents: vi.fn((events: {sessionId: string}[]) => success({accepted: events.length})),
  };
  vi.stubGlobal('google', {script: {run}});
  await open({local: false});
  await vi.runOnlyPendingTimersAsync();
  expect(document.getElementById('screen-title')?.textContent).toBe('Evolución NPS');
  expect(run.recordActivityEvents).toHaveBeenCalledOnce();
  const events = run.recordActivityEvents.mock.calls[0][0];
  expect(events).toHaveLength(2);
  expect(getRandomValues).toHaveBeenCalledOnce();
  for (const event of events) expect(event.sessionId).toBe('07'.repeat(16));
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

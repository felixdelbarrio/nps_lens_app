export type ThemeMode = "light" | "dark";

const THEME_STORAGE_KEY = "nps-lens-theme-mode";

function getStorage(): Storage | null {
  if (typeof window === "undefined") {
    return null;
  }
  const candidate = window.localStorage;
  if (!candidate || typeof candidate.getItem !== "function" || typeof candidate.setItem !== "function") {
    return null;
  }
  return candidate;
}

export function normalizeThemeMode(value: string | null | undefined): ThemeMode {
  return value === "dark" ? "dark" : "light";
}

export function applyDocumentTheme(mode: ThemeMode) {
  document.documentElement.dataset.theme = mode;
  document.documentElement.style.colorScheme = mode;
}

export function readStoredThemeMode(): ThemeMode {
  const storage = getStorage();
  if (!storage) {
    return "light";
  }
  try {
    return normalizeThemeMode(storage.getItem(THEME_STORAGE_KEY));
  } catch {
    return "light";
  }
}

export function persistThemeMode(mode: ThemeMode) {
  const storage = getStorage();
  if (!storage) {
    return;
  }
  try {
    storage.setItem(THEME_STORAGE_KEY, mode);
  } catch {
    // Ignore storage write failures and keep the UI responsive.
  }
}

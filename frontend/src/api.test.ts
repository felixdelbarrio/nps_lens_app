import { afterEach, describe, expect, it, vi } from "vitest";

import { completeArtifactDownload } from "./api";

describe("completeArtifactDownload", () => {
  afterEach(() => {
    delete window.pywebview;
    vi.restoreAllMocks();
  });

  it("uses the file persisted by the local API without navigating to the JSON response", async () => {
    const revealFile = vi.fn(async () => true);
    window.pywebview = { api: { reveal_file: revealFile } };
    const click = vi.spyOn(HTMLAnchorElement.prototype, "click");

    const savedPath = await completeArtifactDownload({
      blob: null,
      fileName: "diagnostico.json",
      savedPath: "/tmp/diagnostico.json"
    });

    expect(savedPath).toBe("/tmp/diagnostico.json");
    expect(revealFile).toHaveBeenCalledWith("/tmp/diagnostico.json");
    expect(click).not.toHaveBeenCalled();
  });
});

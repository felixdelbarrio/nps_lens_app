import { afterEach, describe, expect, it, vi } from "vitest";

import {
  completeArtifactDownload,
  downloadExecutiveReport,
  downloadWebPublication
} from "./api";

describe("completeArtifactDownload", () => {
  afterEach(() => {
    delete window.pywebview;
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it.each([
    ["presentation", downloadExecutiveReport, "/api/dashboard/report/pptx", "informe.pptx"],
    ["WebApp publication", downloadWebPublication, "/api/dashboard/publication.zip", "edicion.zip"]
  ])(
    "downloads the %s response in a local browser even when the API persisted a copy",
    async (_, requestArtifact, endpoint, fileName) => {
      const fetchMock = vi.fn(async (_input: RequestInfo | URL) =>
        new Response("artifact-content", {
          headers: {
            "Content-Disposition": `attachment; filename="${fileName}"`,
            "X-NPS-LENS-SAVED-PATH": `/tmp/${fileName}`
          }
        })
      );
      const createObjectUrl = vi.fn(() => "blob:artifact");
      const revokeObjectUrl = vi.fn();
      const click = vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => {});
      vi.stubGlobal("fetch", fetchMock);
      vi.stubGlobal(
        "URL",
        Object.assign(URL, {
          createObjectURL: createObjectUrl,
          revokeObjectURL: revokeObjectUrl
        })
      );

      const artifact = await requestArtifact({
        service_origin: "BBVA México",
        service_origin_n1: "Senda",
        service_origin_n2: "",
        pop_year: "2026",
        pop_month: "03",
        nps_group: "Detractores",
        score_channel: "Web",
        min_n: 200,
        min_similarity: 0.15,
        max_days_apart: 90,
        touchpoint_source: "executive_journeys",
        report_dimension_analysis: "palanca"
      });
      const savedPath = await completeArtifactDownload(artifact);

      expect(String(fetchMock.mock.calls[0]?.[0])).toContain(endpoint);
      expect(artifact.blob).not.toBeNull();
      expect(artifact.blob?.size).toBeGreaterThan(0);
      expect(artifact.savedPath).toBe("");
      expect(savedPath).toBe("");
      expect(createObjectUrl).toHaveBeenCalledWith(artifact.blob);
      expect(click).toHaveBeenCalledOnce();
    }
  );

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

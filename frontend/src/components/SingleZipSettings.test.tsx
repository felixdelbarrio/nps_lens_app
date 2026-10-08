import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { SWRConfig } from "swr";
import { afterEach, expect, it, vi } from "vitest";
import { SingleZipSettings } from "./SingleZipSettings";

const defaults = {
  single_zip_enabled: false, classifier_single_zip_url: "", helix_single_zip_url: "",
  classifier_url: "https://chatgpt.com/g/original-comments", helix_classifier_url: "https://chatgpt.com/g/original-helix"
};
afterEach(() => vi.unstubAllGlobals());

it("persists the mode and separate empty routes, and copies the new instructions", async () => {
  let settings = { ...defaults };
  const fetcher = vi.fn(async (url: string, init?: RequestInit) => {
    if (url.includes("/instructions")) return Response.json({
      versions: { classifier_single_zip: "comments-v1", helix_single_zip: "helix-v1" },
      classifier_single_zip: "Comentarios por entregas: espera Sí.", helix_single_zip: "Incidencias por entregas: espera Sí."
    });
    if (init?.method === "PUT") settings = { ...settings, ...JSON.parse(init.body as string) };
    return Response.json(settings);
  });
  vi.stubGlobal("fetch", fetcher);
  const user = userEvent.setup();
  const clipboard = vi.spyOn(navigator.clipboard, "writeText").mockResolvedValue();
  render(<SWRConfig value={{ provider: () => new Map() }}><SingleZipSettings context={{service_origin:"Bank"}} disabled={false} /></SWRConfig>);
  const toggle = await screen.findByRole("checkbox", { name: "Activar ZIP único" });
  await waitFor(() => expect(toggle).toBeEnabled());
  expect(toggle).not.toBeChecked();
  expect(screen.getByLabelText("URL · Comentarios · ZIP único")).toHaveValue("");
  expect(screen.getByLabelText("URL · Incidencias · ZIP único")).toHaveValue("");
  expect(screen.queryByRole("link")).not.toBeInTheDocument();
  await user.click(toggle);
  await waitFor(() => expect(toggle).toBeChecked());
  const comments = screen.getByLabelText("URL · Comentarios · ZIP único");
  await user.type(comments, "https://chatgpt.com/g/new-comments");
  await user.tab();
  await waitFor(() => expect(settings.classifier_single_zip_url).toBe("https://chatgpt.com/g/new-comments"));
  expect(settings.classifier_url).toBe(defaults.classifier_url);
  expect(settings.helix_single_zip_url).toBe("");
  await user.click(screen.getByRole("button", {name:"Copiar instrucciones de Clasifica comentarios"}));
  expect(clipboard).toHaveBeenCalledWith("Comentarios por entregas: espera Sí.");
  await user.clear(comments);
  await user.tab();
  await waitFor(() => expect(settings.classifier_single_zip_url).toBe(""));
  await user.click(toggle);
  await waitFor(() => expect(toggle).not.toBeChecked());
});

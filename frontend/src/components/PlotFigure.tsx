import { useEffect, useRef } from "react";

import type { PlotlyFigureSpec } from "../api";

type PlotlyModule = {
  newPlot: (
    root: HTMLDivElement,
    data: unknown[],
    layout?: Record<string, unknown>,
    config?: Record<string, unknown>
  ) => Promise<void>;
  react: (
    root: HTMLDivElement,
    data: unknown[],
    layout?: Record<string, unknown>,
    config?: Record<string, unknown>
  ) => Promise<void>;
  purge: (root: HTMLDivElement) => void;
};

let plotlyModulePromise: Promise<PlotlyModule> | null = null;

function loadPlotlyModule() {
  if (!plotlyModulePromise) {
    plotlyModulePromise = import("plotly.js-dist-min").then(
      (module) => (module.default ?? module) as PlotlyModule
    );
  }
  return plotlyModulePromise;
}

type PlotFigureProps = {
  figure?: PlotlyFigureSpec | null;
  emptyMessage: string;
  testId?: string;
};

export function PlotFigure({ figure, emptyMessage, testId }: PlotFigureProps) {
  const rootRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!rootRef.current || !figure?.data) {
      return undefined;
    }
    const root = rootRef.current!;
    const currentFigure = figure!;

    let cancelled = false;

    async function renderFigure() {
      const plotly = await loadPlotlyModule();
      if (cancelled) {
        return;
      }
      const layout = currentFigure.layout || {};
      const config = {
        displayModeBar: false,
        responsive: true,
        ...currentFigure.config
      };
      if (root.dataset.rendered === "true") {
        await plotly.react(root, currentFigure.data || [], layout, config);
      } else {
        await plotly.newPlot(root, currentFigure.data || [], layout, config);
        root.dataset.rendered = "true";
      }
    }

    void renderFigure();
    return () => {
      cancelled = true;
      if (root.dataset.rendered === "true") {
        loadPlotlyModule()
          .then((plotly) => {
            plotly.purge(root);
            delete root.dataset.rendered;
          })
          .catch(() => undefined);
      }
    };
  }, [figure]);

  if (!figure?.data || figure.data.length === 0) {
    return <p className="empty-state">{emptyMessage}</p>;
  }

  return <div className="plot-figure" data-testid={testId} ref={rootRef} />;
}

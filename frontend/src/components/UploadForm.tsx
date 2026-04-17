import { FormEvent, useEffect, useState } from "react";

import type { Config } from "../api";

type UploadFormProps = {
  config: Config | null;
  uploading: boolean;
  onSubmit: (payload: {
    file: File;
    serviceOrigin: string;
    serviceOriginN1: string;
    serviceOriginN2: string;
  }) => Promise<void>;
};

export function UploadForm({ config, uploading, onSubmit }: UploadFormProps) {
  const [file, setFile] = useState<File | null>(null);
  const [serviceOrigin, setServiceOrigin] = useState("");
  const [serviceOriginN1, setServiceOriginN1] = useState("");
  const [serviceOriginN2, setServiceOriginN2] = useState("");

  useEffect(() => {
    if (!config) {
      return;
    }
    setServiceOrigin((current) => current || config.default_service_origin);
    setServiceOriginN1((current) => current || config.default_service_origin_n1);
  }, [config]);

  const serviceOriginN1Options =
    (config && config.service_origin_n1_map[serviceOrigin]) || [];

  useEffect(() => {
    if (!serviceOriginN1Options.length) {
      return;
    }
    if (!serviceOriginN1Options.includes(serviceOriginN1)) {
      setServiceOriginN1(serviceOriginN1Options[0]);
    }
  }, [serviceOriginN1, serviceOriginN1Options]);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!file) {
      return;
    }
    await onSubmit({
      file,
      serviceOrigin,
      serviceOriginN1,
      serviceOriginN2
    });
    setFile(null);
    setServiceOriginN2("");
    const input = event.currentTarget.querySelector<HTMLInputElement>('input[type="file"]');
    if (input) {
      input.value = "";
    }
  }

  return (
    <form className="panel upload-panel" onSubmit={handleSubmit}>
      <div className="panel-heading">
        <div>
          <p className="eyebrow">Carga acumulativa</p>
          <h2>Subir lote NPS</h2>
        </div>
        <p className="panel-copy">
          Cada carga queda registrada, se normaliza contra esquema canónico y solo inserta
          deltas netos en el histórico.
        </p>
      </div>

      <div className="form-grid">
        <label>
          <span>Fichero</span>
          <input
            data-testid="upload-input"
            type="file"
            accept=".xlsx,.xlsm,.xls"
            onChange={(event) => setFile(event.target.files?.[0] ?? null)}
          />
        </label>

        <label>
          <span>Service origin</span>
          <select
            data-testid="service-origin-select"
            value={serviceOrigin}
            onChange={(event) => setServiceOrigin(event.target.value)}
          >
            {(config?.service_origins || []).map((origin) => (
              <option key={origin} value={origin}>
                {origin}
              </option>
            ))}
          </select>
        </label>

        <label>
          <span>Service origin N1</span>
          <select
            data-testid="service-origin-n1-select"
            value={serviceOriginN1}
            onChange={(event) => setServiceOriginN1(event.target.value)}
          >
            {serviceOriginN1Options.map((value) => (
              <option key={value} value={value}>
                {value}
              </option>
            ))}
          </select>
        </label>

        <label>
          <span>Service origin N2</span>
          <input
            data-testid="service-origin-n2-input"
            placeholder="Opcional"
            value={serviceOriginN2}
            onChange={(event) => setServiceOriginN2(event.target.value)}
          />
        </label>
      </div>

      <button
        className="primary-button"
        disabled={uploading || !file || !serviceOrigin || !serviceOriginN1}
        type="submit"
      >
        {uploading ? "Procesando..." : "Subir y consolidar"}
      </button>
    </form>
  );
}

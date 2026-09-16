import { useState } from "react";
import useSWR from "swr";

import {
  fetchNpsColumnAliases,
  updateNpsColumnAliases,
  type ColumnAliasRegistryPayload,
  type TaxonomyContext
} from "../api";

export function ColumnAliasMaintenance({
  context = {},
  disabled = false
}: {
  context?: TaxonomyContext;
  disabled?: boolean;
}) {
  const aliasContext = {
    service_origin: context.service_origin || "",
    service_origin_n1: context.service_origin_n1 || ""
  };
  const contextKey = new URLSearchParams(aliasContext).toString();
  const { data, error, mutate } = useSWR(
    ["nps-column-aliases", contextKey],
    () => fetchNpsColumnAliases(aliasContext)
  );
  const [draft, setDraft] = useState<ColumnAliasRegistryPayload | null>(null);
  const [message, setMessage] = useState("");
  const [saving, setSaving] = useState(false);
  const registry = draft || data;

  if (error) return <p role="alert">{error.message}</p>;
  if (!registry) return <p>Preparando alias de columnas…</p>;

  function editAliases(index: number, aliases: string[]) {
    if (!registry) return;
    const fields = [...registry.fields];
    fields[index] = { ...fields[index], aliases };
    setDraft({ ...registry, fields });
    setMessage("");
  }

  async function save() {
    if (!registry) return;
    setSaving(true);
    setMessage("");
    try {
      const next = await updateNpsColumnAliases(registry, aliasContext);
      setDraft(null);
      await mutate(next, false);
      setMessage("Alias de columnas NPS guardados.");
    } catch (saveError) {
      setMessage(saveError instanceof Error ? saveError.message : "No se pudieron guardar los alias.");
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="settings-section-stack">
      <p className="secondary-copy">
        Se busca primero el nombre estándar. Los alias solo identifican cabeceras y no cambian valores ni reglas NPS.
      </p>
      <p className="field-hint">
        Configuración para: {registry.service_origin} → {registry.service_origin_n1}
      </p>
      {registry.fields.map((field, index) => (
        <article className="settings-subsection" key={field.canonical}>
          <div className="settings-subsection-copy">
            <h4>{field.canonical}</h4>
            <p className="field-hint">{field.required ? "Obligatorio" : "Opcional"}</p>
          </div>
          <div className="alias-chips">
            {field.aliases.map((alias, aliasIndex) => (
              <span className="alias-chip" key={`${alias}-${aliasIndex}`}>
                {alias}
                <button
                  aria-label={`Eliminar ${alias} de ${field.canonical}`}
                  disabled={disabled || saving}
                  onClick={() => editAliases(index, field.aliases.filter((_, itemIndex) => itemIndex !== aliasIndex))}
                  type="button"
                >
                  ×
                </button>
              </span>
            ))}
          </div>
          <input
            aria-label={`Añadir alias para ${field.canonical}`}
            disabled={disabled || saving}
            placeholder="Escribe un alias y pulsa Intro"
            onKeyDown={(event) => {
              if (event.key !== "Enter") return;
              event.preventDefault();
              const value = event.currentTarget.value.trim();
              if (value) editAliases(index, [...field.aliases, value]);
              event.currentTarget.value = "";
            }}
          />
        </article>
      ))}
      <button className="primary-button" disabled={disabled || saving || !draft} onClick={() => void save()} type="button">
        {saving ? "Guardando…" : "Guardar alias de columnas"}
      </button>
      {message ? <p role="status">{message}</p> : null}
    </div>
  );
}

import { Icon } from "./Icon";

type PrimaryNavProps = {
  items: Array<{
    id: string;
    label: string;
    description: string;
    icon: "home" | "upload" | "database";
  }>;
  value: string;
  onChange: (value: string) => void;
};

export function PrimaryNav({ items, value, onChange }: PrimaryNavProps) {
  return (
    <nav aria-label="Áreas principales" className="primary-nav">
      {items.map((item) => (
        <button
          aria-current={item.id === value ? "page" : undefined}
          className={`primary-nav-item${item.id === value ? " is-active" : ""}`}
          key={item.id}
          onClick={() => onChange(item.id)}
          type="button"
        >
          <span className="primary-nav-icon">
            <Icon className="bbva-icon-solid" label={item.label} name={item.icon} />
          </span>
          <span className="primary-nav-copy">
            <strong>{item.label}</strong>
            <span>{item.description}</span>
          </span>
        </button>
      ))}
    </nav>
  );
}

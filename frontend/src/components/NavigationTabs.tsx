type NavigationTabsProps = {
  items: Array<{ id: string; label: string }>;
  value: string;
  onChange: (value: string) => void;
  compact?: boolean;
  disabled?: boolean;
};

export function NavigationTabs({
  items,
  value,
  onChange,
  compact = false,
  disabled = false
}: NavigationTabsProps) {
  return (
    <div className={`nav-tabs${compact ? " nav-tabs-compact" : ""}`} role="tablist">
      {items.map((item) => (
        <button
          key={item.id}
          aria-selected={item.id === value}
          className={item.id === value ? "is-active" : ""}
          disabled={disabled}
          onClick={() => onChange(item.id)}
          role="tab"
          type="button"
        >
          {item.label}
        </button>
      ))}
    </div>
  );
}

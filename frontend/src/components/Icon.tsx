type IconName =
  | "database"
  | "document"
  | "filter"
  | "history"
  | "home"
  | "presentation"
  | "search"
  | "settings"
  | "upload"
  | "warning";

type IconProps = {
  name: IconName;
  label?: string;
  className?: string;
};

export function Icon({ name, label = "", className = "" }: IconProps) {
  return (
    <img
      alt={label}
      aria-hidden={label ? undefined : true}
      className={`bbva-icon ${className}`.trim()}
      src={`/assets/icons/bbva/${name}.svg`}
    />
  );
}

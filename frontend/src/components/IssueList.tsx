import type { Issue } from "../api";

type IssueListProps = {
  issues: Issue[];
  emptyMessage: string;
  testId?: string;
};

export function IssueList({ issues, emptyMessage, testId = "issues-list" }: IssueListProps) {
  if (!issues.length) {
    return (
      <ul className="issue-list" data-testid={testId}>
        <li className="issue-card issue-info">
          <span>INFO</span>
          <strong>sin_issues</strong>
          <p>{emptyMessage}</p>
        </li>
      </ul>
    );
  }

  return (
    <ul className="issue-list" data-testid={testId}>
      {issues.map((issue) => (
        <li className={`issue-card issue-${issue.level.toLowerCase()}`} key={`${issue.code}-${issue.message}`}>
          <span>{issue.level}</span>
          <strong>{issue.code || "issue"}</strong>
          <p>{issue.message}</p>
        </li>
      ))}
    </ul>
  );
}

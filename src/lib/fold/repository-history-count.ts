import { GitHubGraphqlClient } from "@/lib/github/graphql";
import { gitHubGraphqlBudget } from "@/lib/github/rate-limit-budget";
import { gitlabApiFetch } from "@/lib/security/gitlab-api-fetch";
import { GitLabApiError } from "@/lib/gitlab/api-error";

export type HistoryCountRepository = {
  id: string; sponsor_id: string; owner_name: string; github_repository_id: number | string;
  provider: string | null; instance_url: string | null;
};
export type HistoryCountCredentials = {
  github(sponsorId: string): Promise<string | null>;
  gitlab(sponsorId: string, instanceUrl: string): Promise<{ token: string } | null>;
};

export async function readRepositoryHistorySize(
  repository: HistoryCountRepository, credentials: HistoryCountCredentials, fetchFn?: typeof fetch,
): Promise<number> {
  let counts: { issues?: { totalCount?: unknown; count?: unknown }; pullRequests?: { totalCount?: unknown; count?: unknown } };
  if (repository.provider === "gitlab") {
    if (repository.instance_url === null) throw new Error("GitLab history count requires an instance.");
    const credential = await credentials.gitlab(repository.sponsor_id, repository.instance_url);
    if (credential === null) throw new Error("Sponsor GitLab credential was not available for history count.");
    // GitLab IssueConnection/MergeRequestConnection count is exact without a limit.
    const response = await (fetchFn ?? gitlabApiFetch)(`${repository.instance_url}/api/graphql`, {
      method: "POST", headers: { "content-type": "application/json", authorization: `Bearer ${credential.token}` },
      signal: AbortSignal.timeout(10_000),
      body: JSON.stringify({ query: "query($path: ID!) { repository: project(fullPath: $path) { id issues { count } pullRequests: mergeRequests { count } } }",
        variables: { path: repository.owner_name } }),
    });
    if (!response.ok) throw new GitLabApiError(response.status, "GitLab history count failed.");
    const result = await response.json();
    if (result.errors?.length || result.data?.repository?.id !== `gid://gitlab/Project/${repository.github_repository_id}`) {
      throw new Error("GitLab history count did not resolve the registered project identity.");
    }
    counts = result.data.repository;
  } else {
    const token = await credentials.github(repository.sponsor_id);
    if (token === null) throw new Error("Sponsor GitHub access token was not available for history count.");
    const [owner, name] = repository.owner_name.split("/");
    const client = new GitHubGraphqlClient({ accessToken: token, owner: repository.sponsor_id, budget: gitHubGraphqlBudget(), fetch: fetchFn });
    const result = await client.query<{ repository: typeof counts & { databaseId: number } | null }>(
      "query($owner: String!, $name: String!) { repository(owner: $owner, name: $name) { databaseId issues { totalCount } pullRequests { totalCount } } rateLimit { remaining limit cost resetAt } }",
      { owner, name },
    );
    if (String(result.repository?.databaseId) !== String(repository.github_repository_id)) {
      throw new Error("GitHub history count did not resolve the registered repository identity.");
    }
    counts = result.repository!;
  }
  const issues = counts.issues?.totalCount ?? counts.issues?.count;
  const pulls = counts.pullRequests?.totalCount ?? counts.pullRequests?.count;
  if (typeof issues !== "number" || typeof pulls !== "number" || !Number.isSafeInteger(issues) || issues < 0
    || !Number.isSafeInteger(pulls) || pulls < 0 || !Number.isSafeInteger(issues + pulls)) {
    throw new Error("Repository history count was not a nonnegative safe integer.");
  }
  return issues + pulls;
}

import { describe, expect, it, vi } from "vitest";
import { readDiffOnlyBacklog } from "@/lib/fold/sponsor-sweep-policy";
import { readRepositoryHistorySize, type HistoryCountRepository } from "@/lib/fold/repository-history-count";

const repository: HistoryCountRepository = { id: "repository", sponsor_id: "count-sponsor", owner_name: "owner/project",
  github_repository_id: 123, provider: "github", instance_url: null };
const credentials = () => ({ github: vi.fn(async () => "sponsor-oauth"), gitlab: vi.fn(async () => ({ token: "sponsor-pat" })) });

describe("diff-only backlog configuration", () => {
  it.each([undefined, "", " ", "x", "0", "-1", "1.5", "9007199254740992"])("defaults invalid threshold %s", (raw) => {
    expect(readDiffOnlyBacklog({ RECONCILIATION_DIFF_ONLY_BACKLOG: raw })).toBe(1000);
  });
  it("accepts a positive safe integer", () => expect(readDiffOnlyBacklog({ RECONCILIATION_DIFF_ONLY_BACKLOG: "2000" })).toBe(2000));
});

describe("repository history counts", () => {
  it.each(["github", "gitlab"])("counts all-state %s history with one sponsor-authenticated query", async (provider) => {
    const auth = credentials();
    const fetchFn = vi.fn<typeof fetch>(async () => Response.json({ data: { repository: {
      databaseId: 123, id: "gid://gitlab/Project/123", issues: { totalCount: 901, count: 901 },
      pullRequests: { totalCount: 701, count: 701 },
    } } }));
    await expect(readRepositoryHistorySize({ ...repository, provider, instance_url: "https://gitlab.example" }, auth, fetchFn)).resolves.toBe(1602);
    expect(fetchFn).toHaveBeenCalledOnce();
    const [url, init] = fetchFn.mock.calls[0]!;
    const query = JSON.parse(String(init?.body)).query;
    expect(query).not.toMatch(/nodes|edges|after:|first:|states?:/);
    if (provider === "github") {
      expect(auth.github).toHaveBeenCalledExactlyOnceWith("count-sponsor");
      expect(auth.gitlab).not.toHaveBeenCalled();
      expect(new Headers(init?.headers).get("authorization")).toBe("Bearer sponsor-oauth");
      expect(String(url)).toBe("https://api.github.com/graphql");
    } else {
      expect(auth.gitlab).toHaveBeenCalledExactlyOnceWith("count-sponsor", "https://gitlab.example");
      expect(auth.github).not.toHaveBeenCalled();
      expect(new Headers(init?.headers).get("authorization")).toBe("Bearer sponsor-pat");
      expect(String(url)).toBe("https://gitlab.example/api/graphql");
      expect(query).toContain("count");
      expect(query).not.toContain("limit:");
    }
  });
  it("refuses a recycled path resolving a different repository identity", async () => {
    await expect(readRepositoryHistorySize(repository, credentials(), async () => Response.json({ data: {
      repository: { databaseId: 124, issues: { totalCount: 1 }, pullRequests: { totalCount: 1 } },
    } }))).rejects.toThrow(/registered repository identity/);
  });
  it.each([-1, 1.5, null, "100", Number.MAX_SAFE_INTEGER])("rejects invalid count %s", async (count) => {
    await expect(readRepositoryHistorySize(repository, credentials(), async () => Response.json({ data: {
      repository: { databaseId: 123, issues: { totalCount: count }, pullRequests: { totalCount: 1 } },
    } }))).rejects.toThrow(/nonnegative safe integer/);
  });
});

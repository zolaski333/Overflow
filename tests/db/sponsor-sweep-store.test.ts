import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import type { Sql } from "postgres";
import type { StartedTestContainer } from "testcontainers";
import { runMigrations } from "../../scripts/migrate";
import { getSql, closeSql } from "@/lib/db/client";
import { PostgresSponsorSweepStore } from "@/lib/fold/sponsor-sweep-store";
import { PostgresSweepPassStore } from "@/lib/fold/sweep-pass-store";
import { PostgresReconciliationJobStore } from "@/lib/fold/reconciliation-job-store";
import { synchronizeReconciliationEvidence, DEFAULT_RECONCILIATION_FACT_BYTE_LIMIT } from "@/lib/fold/evidence-facts";
import { FOLD_REVISION } from "@/lib/fold/fold-revision";
import { RECONCILIATION_EVIDENCE_FORMAT } from "@/lib/fold/reconciliation-evidence";
import { materializeRepositoryFixture } from "../support/materialized-repository";
import { startPostgresContainer } from "../support/postgres-container";

let sql: Sql;
let container: StartedTestContainer;
const original = process.env.OVERFLOW_DATABASE_URL;
const at = new Date("2030-01-01T00:00:00Z");
const later = new Date(at.getTime() + 6 * 60 * 60_000);
beforeAll(async () => {
  const started = await startPostgresContainer({ database: "sponsor_sweeps", user: "sponsor_sweeps" });
  container = started.container;
  process.env.OVERFLOW_DATABASE_URL = started.databaseUrl;
  sql = getSql(); await runMigrations();
});
beforeEach(async () => {
  await sql`update registered_repositories set active = false`;
  await sql`delete from repository_reconciliation_jobs`;
  await sql`delete from reconciliation_sponsor_sweep_rounds`;
  await sql`delete from repository_reconciliation_sweep_policy`;
});
afterAll(async () => {
  await closeSql(); await container?.stop();
  if (original === undefined) delete process.env.OVERFLOW_DATABASE_URL; else process.env.OVERFLOW_DATABASE_URL = original;
});

async function fixture() {
  const f = await materializeRepositoryFixture(sql);
  const r = (await f.store.getRepository(f.repositoryId))!;
  return { ...f, sponsorId: r.sponsor.id };
}
async function group(sizes: number[]) {
  const repos: (Awaited<ReturnType<typeof fixture>> & { size: number })[] = [];
  for (const size of sizes) repos.push({ ...await fixture(), size });
  const sponsorId = repos[0]!.sponsorId;
  for (const r of repos) await sql`update registered_repositories set sponsor_id = ${sponsorId} where id = ${r.repositoryId}`;
  const store = new PostgresSponsorSweepStore(sql);
  const count = vi.fn(async (r: { id: string }) => repos.find((f) => f.repositoryId === r.id)!.size);
  await store.measure(at, count);
  return { store, repos, sponsorId, count };
}
async function complete(repositoryId: string) {
  await sql.begin((tx) => synchronizeReconciliationEvidence(tx, repositoryId, {
    expectedVersion: null, scanStartedAt: at, full: true, issues: [], pullRequests: [], dirtySubjects: [],
  }, DEFAULT_RECONCILIATION_FACT_BYTE_LIMIT));
  await sql`delete from repository_reconciliation_jobs where repository_id = ${repositoryId}`;
}

describe("sponsor-scoped sweep persistence", () => {
  it("isolates exhausted sponsors and their independent rotation", async () => {
    const a = await fixture(), b = await fixture();
    const store = new PostgresSponsorSweepStore(sql);
    expect(await store.spend(a.sponsorId, at, 1)).toBeNull();
    expect(await store.spend(a.sponsorId, at, 1)).toEqual(later);
    expect(await store.schedule(at, 1)).toMatchObject({ enqueued: 1, failed: 0 });
    expect(await sql`select repository_id from repository_reconciliation_jobs`).toEqual([{ repository_id: b.repositoryId }]);
    expect(await store.spend(b.sponsorId, at, 1)).toBeNull();
    const restarted = new PostgresSponsorSweepStore(sql);
    expect(await restarted.spend(a.sponsorId, at, 1)).toEqual(later);
    expect(await restarted.spend(a.sponsorId, later, 1)).toBeNull();
    expect(await sql`select spent from reconciliation_sponsor_sweep_rounds where sponsor_id = ${b.sponsorId}`)
      .toEqual([{ spent: 1 }]);
  });

  it("measures once per window, sums active repositories and uses a strict threshold", async () => {
    const { store, repos, count } = await group([500, 500]);
    expect(count).toHaveBeenCalledTimes(2);
    expect((await repos[0]!.store.getRepository(repos[0]!.repositoryId))?.diffOnly).toBe(false);
    await store.measure(new Date(at.getTime() + 3_600_000), count);
    expect(count).toHaveBeenCalledTimes(2);
    count.mockImplementation(async () => 501);
    await store.measure(later, count);
    expect(count).toHaveBeenCalledTimes(4);
    expect((await repos[0]!.store.getRepository(repos[0]!.repositoryId))?.diffOnly).toBe(true);
    await sql`update registered_repositories set active = false where id = ${repos[1]!.repositoryId}`;
    expect((await repos[0]!.store.getRepository(repos[0]!.repositoryId))?.diffOnly).toBe(false);
  });

  it("orders one catch-up at a time and keeps completion across windows and measurements", async () => {
    const { store, repos, count } = await group([200, 100, 1000]);
    expect(await store.schedule(at, 3000)).toMatchObject({ enqueued: 1 });
    expect(await sql`select repository_id from repository_reconciliation_jobs`).toEqual([{ repository_id: repos[1]!.repositoryId }]);
    expect(await new PostgresSponsorSweepStore(sql).schedule(later, 3000)).toMatchObject({ enqueued: 0 });
    await complete(repos[1]!.repositoryId);
    await store.measure(later, count);
    expect(await store.schedule(later, 3000)).toMatchObject({ enqueued: 1 });
    expect(await sql`select repository_id from repository_reconciliation_jobs`).toEqual([{ repository_id: repos[0]!.repositoryId }]);
    expect(await sql`select catch_up_state from repository_reconciliation_sweep_policy where repository_id = ${repos[1]!.repositoryId}`)
      .toEqual([{ catch_up_state: "COMPLETED" }]);
  });

  it("retains the last measurement on failure and retries after five minutes", async () => {
    const { store, repos } = await group([1200]);
    const count = vi.fn(async () => { throw new Error("count unavailable\n\u001b[31m"); });
    const log = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      await store.measure(later, count);
      await store.measure(new Date(later.getTime() + 299_999), count);
      expect(count).toHaveBeenCalledOnce();
      expect(log.mock.calls[0]?.[1]).toBe('"Error": "count unavailable\\u000a\\u001b[31m"');
      expect(await sql`select history_size::text, measured_at from repository_reconciliation_sweep_policy
        where repository_id = ${repos[0]!.repositoryId}`).toEqual([{ history_size: "1200", measured_at: at }]);
      await store.measure(new Date(later.getTime() + 300_000), count);
      expect(count).toHaveBeenCalledTimes(2);
    } finally { log.mockRestore(); }
  });

  it("resumes the same held pass after restart and enqueues no other catch-up", async () => {
    const { store, repos, sponsorId } = await group([600, 700]);
    await store.schedule(at, 1);
    const input = { repositoryId: repos[0]!.repositoryId, evidenceVersion: "held-v1", scanStartedAt: at, dirtySubjects: [] };
    const pass = await new PostgresSweepPassStore(sql).open(input);
    await pass.write("page-one", { cursor: "next" });
    await store.spend(sponsorId, at, 1);
    expect(await new PostgresSponsorSweepStore(sql).schedule(later, 1)).toMatchObject({ enqueued: 0 });
    const resumed = await new PostgresSweepPassStore(sql).open({ ...input, scanStartedAt: later });
    expect(resumed.id).toBe(pass.id);
    expect(resumed.scanStartedAt).toEqual(at);
    expect(await resumed.read("page-one")).toEqual({ cursor: "next" });
    expect(await sql`select repository_id from repository_reconciliation_jobs`).toEqual([{ repository_id: repos[0]!.repositoryId }]);
  });

  it("restores a missing job for the same durable catch-up before scheduling another repository", async () => {
    const { store, repos } = await group([600, 700]);
    await store.schedule(at, 3000);
    const input = { repositoryId: repos[0]!.repositoryId, evidenceVersion: "resume-v1", scanStartedAt: at, dirtySubjects: [] };
    const pass = await new PostgresSweepPassStore(sql).open(input);
    await pass.write("saved-page", { cursor: "next" });
    await sql`delete from repository_reconciliation_jobs where repository_id = ${repos[0]!.repositoryId}`;
    expect(await new PostgresSponsorSweepStore(sql).schedule(later, 3000)).toMatchObject({ enqueued: 1 });
    expect(await sql`select repository_id, reason from repository_reconciliation_jobs`)
      .toEqual([{ repository_id: repos[0]!.repositoryId, reason: "SWEEP" }]);
    const resumed = await new PostgresSweepPassStore(sql).open(input);
    expect(resumed.id).toBe(pass.id);
    expect(await resumed.read("saved-page")).toEqual({ cursor: "next" });
  });

  it("leaves webhook jobs incremental after catch-ups and restores sweeps below the threshold", async () => {
    const { store, repos } = await group([600, 700]);
    await store.schedule(at, 3000); await complete(repos[0]!.repositoryId);
    await store.schedule(at, 3000); await complete(repos[1]!.repositoryId);
    await repos[0]!.store.enqueueReconciliationJob(repos[0]!.repositoryId, "WEBHOOK");
    expect(await store.schedule(later, 3000)).toMatchObject({ enqueued: 0 });
    expect(await sql`select reason from repository_reconciliation_jobs`).toEqual([{ reason: "WEBHOOK" }]);
    await sql`update repository_reconciliation_sweep_policy set history_size = 0`;
    expect(await store.schedule(later, 3000)).toMatchObject({ enqueued: 1 });
    expect(await sql`select count(*)::int as count from repository_reconciliation_jobs where reason = 'SWEEP'`)
      .toEqual([{ count: 1 }]);
  });

  it("records terminal catch-up failure atomically with its leased job", async () => {
    const { store, repos } = await group([1200]);
    await store.schedule(at, 3000);
    const jobs = new PostgresReconciliationJobStore(sql);
    const job = (await jobs.claimNextReconciliationJob())!;
    expect(await jobs.failReconciliationJob(job.id, job.leaseToken)).toBe(true);
    expect(await sql`select catch_up_state from repository_reconciliation_sweep_policy where repository_id = ${repos[0]!.repositoryId}`)
      .toEqual([{ catch_up_state: "FAILED" }]);
    expect(await store.schedule(later, 3000)).toMatchObject({ enqueued: 0 });
  });

  it("rolls catch-up completion back when evidence publication rolls back", async () => {
    const { store, repos } = await group([1200]);
    await store.schedule(at, 3000);
    await expect(sql.begin(async (tx) => {
      await synchronizeReconciliationEvidence(tx, repos[0]!.repositoryId, { expectedVersion: null,
        scanStartedAt: at, full: true, issues: [], pullRequests: [], dirtySubjects: [] }, DEFAULT_RECONCILIATION_FACT_BYTE_LIMIT);
      throw new Error("rollback publication");
    })).rejects.toThrow("rollback publication");
    expect(await sql`select catch_up_state from repository_reconciliation_sweep_policy where repository_id = ${repos[0]!.repositoryId}`)
      .toEqual([{ catch_up_state: "IN_FLIGHT" }]);
    expect(await repos[0]!.store.getReconciliationEvidence(repos[0]!.repositoryId)).toBeNull();
  });

  it("keeps failed catch-ups terminal until a format or fold revision changes", async () => {
    const { store, repos } = await group([600, 700]);
    await store.schedule(at, 3000);
    await sql`update repository_reconciliation_jobs set state = 'FAILED' where repository_id = ${repos[0]!.repositoryId}`;
    await store.schedule(at, 3000);
    await sql`update repository_reconciliation_jobs set state = 'FAILED' where repository_id = ${repos[1]!.repositoryId}`;
    await store.schedule(later, 3000);
    expect(await sql`select catch_up_state from repository_reconciliation_sweep_policy order by repository_id`)
      .toEqual([{ catch_up_state: "FAILED" }, { catch_up_state: "FAILED" }]);
    for (const column of ["catch_up_format", "catch_up_revision"]) {
      await sql`update repository_reconciliation_sweep_policy set ${sql(column)} = 0 where repository_id = ${repos[0]!.repositoryId}`;
      expect(await store.schedule(later, 3000)).toMatchObject({ enqueued: 1 });
      await sql`update repository_reconciliation_jobs set state = 'FAILED' where repository_id = ${repos[0]!.repositoryId}`;
      await store.schedule(later, 3000);
    }
    expect(await sql`select catch_up_revision, catch_up_format from repository_reconciliation_sweep_policy where repository_id = ${repos[0]!.repositoryId}`)
      .toEqual([{ catch_up_revision: FOLD_REVISION, catch_up_format: RECONCILIATION_EVIDENCE_FORMAT }]);
  });

  it("does not copy a spent singleton when applying the migration", async () => {
    const f = await fixture();
    await sql`drop table repository_reconciliation_sweep_policy, reconciliation_sponsor_sweep_rounds`;
    await sql`delete from schema_migrations where name = '076_sponsor_reconciliation_sweeps.sql'`;
    await sql`insert into reconciliation_sweep_round (window_start, spent, next_enqueue_at)
      values (${at}, 3000, ${later}) on conflict (singleton) do update set spent = 3000`;
    await runMigrations();
    expect(await sql`select * from reconciliation_sponsor_sweep_rounds`).toHaveLength(0);
    expect(await new PostgresSponsorSweepStore(sql).spend(f.sponsorId, at, 3000)).toBeNull();
    expect(await sql`select spent from reconciliation_sponsor_sweep_rounds`).toEqual([{ spent: 1 }]);
  });
});

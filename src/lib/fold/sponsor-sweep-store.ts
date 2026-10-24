import { getSql } from "@/lib/db/client";
import type { SqlClient, TransactionClient } from "@/lib/db/types";
import { PostgresReconciliationJobStore } from "@/lib/fold/reconciliation-job-store";
import { SWEEP_WINDOW_MS } from "@/lib/fold/sweep-request-budget";
import { configuredDiffOnlyBacklog } from "@/lib/fold/sponsor-sweep-policy";
import { FOLD_REVISION } from "@/lib/fold/fold-revision";
import { RECONCILIATION_EVIDENCE_FORMAT } from "@/lib/fold/reconciliation-evidence";
import type { HistoryCountRepository } from "@/lib/fold/repository-history-count";
import { errorLogToken } from "@/lib/webhooks/log-field";

type Round = { window_start: Date; spent: number; rotation_cursor: string | null; next_enqueue_at: Date };
type Policy = HistoryCountRepository & { history_size: string; diff_only: boolean };

/** Sponsor rows serialize spend and rotation without locking another sponsor. */
export class PostgresSponsorSweepStore {
  public constructor(private readonly connection?: SqlClient, private readonly threshold = configuredDiffOnlyBacklog()) {}
  private get sql(): SqlClient { return this.connection ?? getSql(); }

  private async round(tx: TransactionClient, sponsorId: string, now: Date): Promise<Round> {
    await tx`insert into reconciliation_sponsor_sweep_rounds (sponsor_id, window_start, next_enqueue_at)
      values (${sponsorId}, ${now}, ${now}) on conflict do nothing`;
    const [row] = await tx<Round[]>`select * from reconciliation_sponsor_sweep_rounds
      where sponsor_id = ${sponsorId} for update`;
    if (now.getTime() < row!.window_start.getTime() + SWEEP_WINDOW_MS) return row!;
    const start = new Date(row!.window_start.getTime()
      + Math.floor((now.getTime() - row!.window_start.getTime()) / SWEEP_WINDOW_MS) * SWEEP_WINDOW_MS);
    const [fresh] = await tx<Round[]>`update reconciliation_sponsor_sweep_rounds
      set window_start = ${start}, spent = 0, next_enqueue_at = ${start}
      where sponsor_id = ${sponsorId} returning *`;
    return fresh!;
  }

  public async spend(sponsorId: string, now: Date, limit: number): Promise<Date | null> {
    return this.sql.begin(async (tx) => {
      const round = await this.round(tx, sponsorId, now);
      const rows = await tx`update reconciliation_sponsor_sweep_rounds set spent = spent + 1
        where sponsor_id = ${sponsorId} and spent < ${limit} returning spent`;
      return rows.length === 1 ? null : new Date(round.window_start.getTime() + SWEEP_WINDOW_MS);
    }) as Promise<Date | null>;
  }

  /** Lease count reads before network I/O; a failed count retries in five minutes. */
  public async measure(now: Date, count: (repository: HistoryCountRepository) => Promise<number>): Promise<void> {
    await this.sql`insert into repository_reconciliation_sweep_policy (repository_id)
      select id from registered_repositories where active on conflict do nothing`;
    const repositories = await this.sql<HistoryCountRepository[]>`with due as (
      update repository_reconciliation_sweep_policy p set next_measure_at = ${new Date(now.getTime() + SWEEP_WINDOW_MS)}
      from registered_repositories r where p.repository_id = r.id and r.active and p.next_measure_at <= ${now}
      returning r.id, r.sponsor_id, r.owner_name, r.github_repository_id, r.provider, r.instance_url
    ) select * from due order by sponsor_id, id`;
    for (const repository of repositories) {
      try {
        const size = await count(repository);
        if (!Number.isSafeInteger(size) || size < 0) throw new Error("Invalid repository history size.");
        await this.sql`update repository_reconciliation_sweep_policy set history_size = ${size}, measured_at = ${now}
          where repository_id = ${repository.id}`;
      } catch (error) {
        await this.sql`update repository_reconciliation_sweep_policy set next_measure_at = ${new Date(now.getTime() + 300_000)}
          where repository_id = ${repository.id}`;
        console.error(`Repository history count failed for ${repository.id}`, errorLogToken(error));
      }
    }
  }

  private async policies(): Promise<Policy[]> {
    return this.sql<Policy[]>`select r.id, r.sponsor_id, r.owner_name, r.github_repository_id, r.provider, r.instance_url,
      coalesce(p.history_size, 0)::text as history_size,
      sum(coalesce(p.history_size, 0)) over (partition by r.sponsor_id) > ${this.threshold} as diff_only
      from registered_repositories r left join repository_reconciliation_sweep_policy p on p.repository_id = r.id
      where r.active order by r.sponsor_id, coalesce(p.history_size, 0), r.id`;
  }

  public async schedule(now: Date, limit: number): Promise<{ attempted: number; enqueued: number; failed: number }> {
    const groups = new Map<string, Policy[]>();
    for (const row of await this.policies()) {
      const group = groups.get(row.sponsor_id) ?? [];
      group.push(row); groups.set(row.sponsor_id, group);
    }
    const result = { attempted: 0, enqueued: 0, failed: 0 };
    for (const [sponsorId, repositories] of groups) {
      try {
        const enqueued = await this.sql.begin(async (tx) => {
          const round = await this.round(tx, sponsorId, now);
          if (repositories[0]!.diff_only) return this.catchUp(tx, sponsorId, now, round, limit);
          if (round.spent >= limit || now < round.next_enqueue_at) return false;
          const ids = repositories.map((row) => row.id).sort();
          const next = ids.find((id) => round.rotation_cursor === null || id > round.rotation_cursor) ?? ids[0]!;
          await new PostgresReconciliationJobStore(tx as unknown as SqlClient).enqueueReconciliationJob(next, "SWEEP");
          const interval = Math.max(15_000, Math.floor(SWEEP_WINDOW_MS / ids.length));
          const due = round.next_enqueue_at.getTime();
          const anchor = now.getTime() - due > interval ? now.getTime() : due;
          await tx`update reconciliation_sponsor_sweep_rounds set rotation_cursor = ${next},
            next_enqueue_at = ${new Date(anchor + interval)} where sponsor_id = ${sponsorId}`;
          return true;
        });
        if (enqueued) { result.attempted++; result.enqueued++; }
      } catch (error) {
        result.attempted++; result.failed++;
        console.error(`Sponsor sweep enqueue failed for ${sponsorId}`, errorLogToken(error));
      }
    }
    return result;
  }

  /** The sponsor round lock and the job/state write share this transaction. */
  private async catchUp(tx: TransactionClient, sponsorId: string, now: Date, round: Round, limit: number): Promise<boolean> {
    await tx`update repository_reconciliation_sweep_policy p
      set catch_up_state = case when p.catch_up_state = 'IN_FLIGHT' then 'IN_FLIGHT' else 'PENDING' end,
        catch_up_revision = ${FOLD_REVISION}, catch_up_format = ${RECONCILIATION_EVIDENCE_FORMAT}
      from registered_repositories r where r.id = p.repository_id and r.active and r.sponsor_id = ${sponsorId}
        and (p.catch_up_state is null or p.catch_up_revision is distinct from ${FOLD_REVISION}
          or p.catch_up_format is distinct from ${RECONCILIATION_EVIDENCE_FORMAT})`;
    // Terminal failure does not retry on a later sweep, even after restart.
    await tx`update repository_reconciliation_sweep_policy p set catch_up_state = 'FAILED'
      from repository_reconciliation_jobs j, registered_repositories r
      where p.repository_id = j.repository_id and r.id = p.repository_id and r.sponsor_id = ${sponsorId}
        and p.catch_up_state = 'IN_FLIGHT' and j.state = 'FAILED'`;
    const active = await tx<{ repository_id: string; has_job: boolean }[]>`select p.repository_id,
      exists (select 1 from repository_reconciliation_jobs j where j.repository_id = p.repository_id) as has_job
      from repository_reconciliation_sweep_policy p
      join registered_repositories r on r.id = p.repository_id
      where r.sponsor_id = ${sponsorId} and r.active and p.catch_up_state = 'IN_FLIGHT' limit 1`;
    if (round.spent >= limit) return false;
    if (active.length !== 0) {
      if (active[0]!.has_job) return false;
      // Reactivation can leave a durable pass without its deleted job. Resume
      // that repository before allowing another catch-up for this sponsor.
      await new PostgresReconciliationJobStore(tx as unknown as SqlClient).enqueueReconciliationJob(active[0]!.repository_id, "SWEEP");
      return true;
    }
    const [next] = await tx<{ repository_id: string }[]>`select p.repository_id
      from repository_reconciliation_sweep_policy p join registered_repositories r on r.id = p.repository_id
      where r.sponsor_id = ${sponsorId} and r.active and p.catch_up_state = 'PENDING'
      order by p.history_size, p.repository_id limit 1 for update of p`;
    if (next === undefined) return false;
    await new PostgresReconciliationJobStore(tx as unknown as SqlClient).enqueueReconciliationJob(next.repository_id, "SWEEP");
    await tx`update repository_reconciliation_sweep_policy set catch_up_state = 'IN_FLIGHT'
      where repository_id = ${next.repository_id}`;
    return true;
  }
}

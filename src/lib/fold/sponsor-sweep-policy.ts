import { readEnv } from "@/lib/env";

export const DEFAULT_DIFF_ONLY_BACKLOG = 1000;

export function readDiffOnlyBacklog(environment: Partial<Record<string, string>>): number {
  const raw = environment.RECONCILIATION_DIFF_ONLY_BACKLOG ?? "";
  const value = /^\d+$/.test(raw) ? Number(raw) : NaN;
  return Number.isSafeInteger(value) && value > 0 ? value : DEFAULT_DIFF_ONLY_BACKLOG;
}

export function configuredDiffOnlyBacklog(): number {
  return readDiffOnlyBacklog({ RECONCILIATION_DIFF_ONLY_BACKLOG: readEnv("RECONCILIATION_DIFF_ONLY_BACKLOG") });
}

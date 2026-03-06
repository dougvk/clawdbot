import { runSubagentAnnounceFlow } from "../../agents/subagent-announce.js";
import { countActiveDescendantRuns } from "../../agents/subagent-registry.js";
import { SILENT_REPLY_TOKEN } from "../../auto-reply/tokens.js";
import type { ReplyPayload } from "../../auto-reply/types.js";
import { createOutboundSendDeps, type CliDeps } from "../../cli/outbound-send-deps.js";
import type { OpenClawConfig } from "../../config/config.js";
import { resolveAgentMainSessionKey } from "../../config/sessions.js";
import { callGateway } from "../../gateway/call.js";
import { sleepWithAbort } from "../../infra/backoff.js";
import {
  deliverOutboundPayloads,
  type OutboundDeliveryResult,
} from "../../infra/outbound/deliver.js";
import { resolveAgentOutboundIdentity } from "../../infra/outbound/identity.js";
import { resolveOutboundSessionRoute } from "../../infra/outbound/outbound-session.js";
import { buildOutboundSessionContext } from "../../infra/outbound/session-context.js";
import { logWarn } from "../../logger.js";
import type { CronDeliveryFormat, CronJob, CronRunTelemetry } from "../types.js";
import type { DeliveryTargetResolution } from "./delivery-target.js";
import { pickSummaryFromOutput } from "./helpers.js";
import type { RunCronAgentTurnResult } from "./run.js";
import {
  expectsSubagentFollowup,
  isLikelyInterimCronMessage,
  readDescendantSubagentFallbackReply,
  waitForDescendantSubagentSummary,
} from "./subagent-followup.js";

function normalizeDeliveryTarget(channel: string, to: string): string {
  const channelLower = channel.trim().toLowerCase();
  const toTrimmed = to.trim();
  if (channelLower === "feishu" || channelLower === "lark") {
    const lowered = toTrimmed.toLowerCase();
    if (lowered.startsWith("user:")) {
      return toTrimmed.slice("user:".length).trim();
    }
    if (lowered.startsWith("chat:")) {
      return toTrimmed.slice("chat:".length).trim();
    }
  }
  return toTrimmed;
}

export function matchesMessagingToolDeliveryTarget(
  target: { provider?: string; to?: string; accountId?: string },
  delivery: { channel?: string; to?: string; accountId?: string },
): boolean {
  if (!delivery.channel || !delivery.to || !target.to) {
    return false;
  }
  const channel = delivery.channel.trim().toLowerCase();
  const provider = target.provider?.trim().toLowerCase();
  if (provider && provider !== "message" && provider !== channel) {
    return false;
  }
  if (target.accountId && delivery.accountId && target.accountId !== delivery.accountId) {
    return false;
  }
  // Strip :topic:NNN from message targets and normalize Feishu/Lark prefixes on
  // both sides so cron duplicate suppression compares canonical IDs.
  const normalizedTargetTo = normalizeDeliveryTarget(channel, target.to.replace(/:topic:\d+$/, ""));
  const normalizedDeliveryTo = normalizeDeliveryTarget(channel, delivery.to);
  return normalizedTargetTo === normalizedDeliveryTo;
}

export function resolveCronDeliveryBestEffort(job: CronJob): boolean {
  if (typeof job.delivery?.bestEffort === "boolean") {
    return job.delivery.bestEffort;
  }
  if (job.payload.kind === "agentTurn" && typeof job.payload.bestEffortDeliver === "boolean") {
    return job.payload.bestEffortDeliver;
  }
  return false;
}

async function resolveCronAnnounceSessionKey(params: {
  cfg: OpenClawConfig;
  agentId: string;
  fallbackSessionKey: string;
  delivery: {
    channel: NonNullable<DeliveryTargetResolution["channel"]>;
    to?: string;
    accountId?: string;
    threadId?: string | number;
  };
}): Promise<string> {
  const to = params.delivery.to?.trim();
  if (!to) {
    return params.fallbackSessionKey;
  }
  try {
    const route = await resolveOutboundSessionRoute({
      cfg: params.cfg,
      channel: params.delivery.channel,
      agentId: params.agentId,
      accountId: params.delivery.accountId,
      target: to,
      threadId: params.delivery.threadId,
    });
    const resolved = route?.sessionKey?.trim();
    if (resolved) {
      return resolved;
    }
  } catch {
    // Fall back to main session routing if announce session resolution fails.
  }
  return params.fallbackSessionKey;
}

export type SuccessfulDeliveryTarget = Extract<DeliveryTargetResolution, { ok: true }>;

type DispatchCronDeliveryParams = {
  cfg: OpenClawConfig;
  cfgWithAgentDefaults: OpenClawConfig;
  deps: CliDeps;
  job: CronJob;
  agentId: string;
  agentSessionKey: string;
  runSessionId: string;
  runStartedAt: number;
  runEndedAt: number;
  timeoutMs: number;
  resolvedDelivery: DeliveryTargetResolution;
  deliveryRequested: boolean;
  deliveryFormat: CronDeliveryFormat;
  skipHeartbeatDelivery: boolean;
  skipMessagingToolDelivery?: boolean;
  deliveryBestEffort: boolean;
  deliveryPayloadHasStructuredContent: boolean;
  deliveryPayloads: ReplyPayload[];
  synthesizedText?: string;
  summary?: string;
  outputText?: string;
  telemetry?: CronRunTelemetry;
  abortSignal?: AbortSignal;
  isAborted: () => boolean;
  abortReason: () => string;
  withRunSession: (
    result: Omit<RunCronAgentTurnResult, "sessionId" | "sessionKey">,
  ) => RunCronAgentTurnResult;
};

export type DispatchCronDeliveryState = {
  result?: RunCronAgentTurnResult;
  delivered: boolean;
  deliveryAttempted: boolean;
  summary?: string;
  outputText?: string;
  synthesizedText?: string;
  deliveryPayloads: ReplyPayload[];
};

const TRANSIENT_DIRECT_CRON_DELIVERY_ERROR_PATTERNS: readonly RegExp[] = [
  /\berrorcode=unavailable\b/i,
  /\bstatus\s*[:=]\s*"?unavailable\b/i,
  /\bUNAVAILABLE\b/,
  /no active .* listener/i,
  /gateway not connected/i,
  /gateway closed \(1006/i,
  /gateway timeout/i,
  /\b(econnreset|econnrefused|etimedout|enotfound|ehostunreach|network error)\b/i,
];

const PERMANENT_DIRECT_CRON_DELIVERY_ERROR_PATTERNS: readonly RegExp[] = [
  /unsupported channel/i,
  /unknown channel/i,
  /chat not found/i,
  /user not found/i,
  /bot was blocked by the user/i,
  /forbidden: bot was kicked/i,
  /recipient is not a valid/i,
  /outbound not configured for channel/i,
];

type CompletedDirectCronDelivery = {
  ts: number;
  results: OutboundDeliveryResult[];
};

const COMPLETED_DIRECT_CRON_DELIVERIES = new Map<string, CompletedDirectCronDelivery>();

function cloneDeliveryResults(
  results: readonly OutboundDeliveryResult[],
): OutboundDeliveryResult[] {
  return results.map((result) => ({
    ...result,
    ...(result.meta ? { meta: { ...result.meta } } : {}),
  }));
}

function pruneCompletedDirectCronDeliveries(now: number) {
  const ttlMs = process.env.OPENCLAW_TEST_FAST === "1" ? 60_000 : 24 * 60 * 60 * 1000;
  for (const [key, entry] of COMPLETED_DIRECT_CRON_DELIVERIES) {
    if (now - entry.ts >= ttlMs) {
      COMPLETED_DIRECT_CRON_DELIVERIES.delete(key);
    }
  }
  const maxEntries = 2000;
  if (COMPLETED_DIRECT_CRON_DELIVERIES.size <= maxEntries) {
    return;
  }
  const entries = [...COMPLETED_DIRECT_CRON_DELIVERIES.entries()].toSorted(
    (a, b) => a[1].ts - b[1].ts,
  );
  const toDelete = COMPLETED_DIRECT_CRON_DELIVERIES.size - maxEntries;
  for (let i = 0; i < toDelete; i += 1) {
    const oldest = entries[i];
    if (!oldest) {
      break;
    }
    COMPLETED_DIRECT_CRON_DELIVERIES.delete(oldest[0]);
  }
}

function rememberCompletedDirectCronDelivery(
  idempotencyKey: string,
  results: readonly OutboundDeliveryResult[],
) {
  const now = Date.now();
  COMPLETED_DIRECT_CRON_DELIVERIES.set(idempotencyKey, {
    ts: now,
    results: cloneDeliveryResults(results),
  });
  pruneCompletedDirectCronDeliveries(now);
}

function getCompletedDirectCronDelivery(
  idempotencyKey: string,
): OutboundDeliveryResult[] | undefined {
  const now = Date.now();
  pruneCompletedDirectCronDeliveries(now);
  const cached = COMPLETED_DIRECT_CRON_DELIVERIES.get(idempotencyKey);
  if (!cached) {
    return undefined;
  }
  return cloneDeliveryResults(cached.results);
}

function buildDirectCronDeliveryIdempotencyKey(params: {
  runSessionId: string;
  delivery: SuccessfulDeliveryTarget;
}): string {
  const threadId =
    params.delivery.threadId == null || params.delivery.threadId === ""
      ? ""
      : String(params.delivery.threadId);
  const accountId = params.delivery.accountId?.trim() ?? "";
  const normalizedTo = normalizeDeliveryTarget(params.delivery.channel, params.delivery.to);
  return `cron-direct-delivery:v1:${params.runSessionId}:${params.delivery.channel}:${accountId}:${normalizedTo}:${threadId}`;
}

export function resetCompletedDirectCronDeliveriesForTests() {
  COMPLETED_DIRECT_CRON_DELIVERIES.clear();
}

export function getCompletedDirectCronDeliveriesCountForTests(): number {
  return COMPLETED_DIRECT_CRON_DELIVERIES.size;
}

function summarizeDirectCronDeliveryError(error: unknown): string {
  if (error instanceof Error) {
    return error.message || "error";
  }
  if (typeof error === "string") {
    return error;
  }
  try {
    return JSON.stringify(error) || String(error);
  } catch {
    return String(error);
  }
}

function isTransientDirectCronDeliveryError(error: unknown): boolean {
  const message = summarizeDirectCronDeliveryError(error);
  if (!message) {
    return false;
  }
  if (PERMANENT_DIRECT_CRON_DELIVERY_ERROR_PATTERNS.some((re) => re.test(message))) {
    return false;
  }
  return TRANSIENT_DIRECT_CRON_DELIVERY_ERROR_PATTERNS.some((re) => re.test(message));
}

function resolveDirectCronRetryDelaysMs(): readonly number[] {
  return process.env.NODE_ENV === "test" && process.env.OPENCLAW_TEST_FAST === "1"
    ? [8, 16, 32]
    : [5_000, 10_000, 20_000];
}

async function retryTransientDirectCronDelivery<T>(params: {
  jobId: string;
  signal?: AbortSignal;
  run: () => Promise<T>;
}): Promise<T> {
  const retryDelaysMs = resolveDirectCronRetryDelaysMs();
  let retryIndex = 0;
  for (;;) {
    if (params.signal?.aborted) {
      throw new Error("cron delivery aborted");
    }
    try {
      return await params.run();
    } catch (err) {
      const delayMs = retryDelaysMs[retryIndex];
      if (delayMs == null || !isTransientDirectCronDeliveryError(err) || params.signal?.aborted) {
        throw err;
      }
      const nextAttempt = retryIndex + 2;
      const maxAttempts = retryDelaysMs.length + 1;
      logWarn(
        `[cron:${params.jobId}] transient direct announce delivery failure, retrying ${nextAttempt}/${maxAttempts} in ${Math.round(delayMs / 1000)}s: ${summarizeDirectCronDeliveryError(err)}`,
      );
      retryIndex += 1;
      await sleepWithAbort(delayMs, params.signal);
    }
  }
}

export async function dispatchCronDelivery(
  params: DispatchCronDeliveryParams,
): Promise<DispatchCronDeliveryState> {
  const skipMessagingToolDelivery = params.skipMessagingToolDelivery === true;
  let summary = params.summary;
  let outputText = params.outputText;
  let synthesizedText = params.synthesizedText;
  let deliveryPayloads = params.deliveryPayloads;

  // Shared callers can treat a matching message-tool send as the completed
  // delivery path. Cron-owned callers keep this false so direct cron delivery
  // remains the only source of delivered state.
  let delivered = skipMessagingToolDelivery;
  let deliveryAttempted = skipMessagingToolDelivery;
  const failDeliveryTarget = (error: string) =>
    params.withRunSession({
      status: "error",
      error,
      errorKind: "delivery-target",
      summary,
      outputText,
      deliveryAttempted,
      ...params.telemetry,
    });

  const deliverViaDirect = async (
    delivery: SuccessfulDeliveryTarget,
    options?: { retryTransient?: boolean },
  ): Promise<RunCronAgentTurnResult | null> => {
    const identity = resolveAgentOutboundIdentity(params.cfgWithAgentDefaults, params.agentId);
    const deliveryIdempotencyKey = buildDirectCronDeliveryIdempotencyKey({
      runSessionId: params.runSessionId,
      delivery,
    });
    try {
      const payloadsForDelivery =
        deliveryPayloads.length > 0
          ? deliveryPayloads
          : synthesizedText
            ? [{ text: synthesizedText }]
            : [];
      if (payloadsForDelivery.length === 0) {
        return null;
      }
      if (params.isAborted()) {
        return params.withRunSession({
          status: "error",
          error: params.abortReason(),
          deliveryAttempted,
          ...params.telemetry,
        });
      }
      deliveryAttempted = true;
      const cachedResults = getCompletedDirectCronDelivery(deliveryIdempotencyKey);
      if (cachedResults) {
        // Cached entries are only recorded after a successful non-empty delivery.
        delivered = true;
        return null;
      }
      const deliverySession = buildOutboundSessionContext({
        cfg: params.cfgWithAgentDefaults,
        agentId: params.agentId,
        sessionKey: params.agentSessionKey,
      });
      const runDelivery = async () =>
        await deliverOutboundPayloads({
          cfg: params.cfgWithAgentDefaults,
          channel: delivery.channel,
          to: delivery.to,
          accountId: delivery.accountId,
          threadId: delivery.threadId,
          payloads: payloadsForDelivery,
          session: deliverySession,
          identity,
          bestEffort: params.deliveryBestEffort,
          deps: createOutboundSendDeps(params.deps),
          abortSignal: params.abortSignal,
          // Isolated cron direct delivery uses its own transient retry loop.
          // Keep all attempts out of the write-ahead delivery queue so a
          // late-successful first send cannot leave behind a failed queue
          // entry that replays on the next restart.
          // See: https://github.com/openclaw/openclaw/issues/40545
          skipQueue: true,
        });
      const deliveryResults = options?.retryTransient
        ? await retryTransientDirectCronDelivery({
            jobId: params.job.id,
            signal: params.abortSignal,
            run: runDelivery,
          })
        : await runDelivery();
      delivered = deliveryResults.length > 0;
      if (delivered) {
        rememberCompletedDirectCronDelivery(deliveryIdempotencyKey, deliveryResults);
      }
      return null;
    } catch (err) {
      if (!params.deliveryBestEffort) {
        return params.withRunSession({
          status: "error",
          summary,
          outputText,
          error: String(err),
          deliveryAttempted,
          ...params.telemetry,
        });
      }
      return null;
    }
  };

  const finalizeTextDelivery = async (
    delivery: SuccessfulDeliveryTarget,
  ): Promise<RunCronAgentTurnResult | null> => {
    const cleanupDirectCronSessionIfNeeded = async (): Promise<void> => {
      if (!params.job.deleteAfterRun) {
        return;
      }
      try {
        await callGateway({
          method: "sessions.delete",
          params: {
            key: params.agentSessionKey,
            deleteTranscript: true,
            emitLifecycleHooks: false,
          },
          timeoutMs: 10_000,
        });
      } catch {
        // Best-effort; direct delivery result should still be returned.
      }
    };

    if (!synthesizedText) {
      return null;
    }
    const initialSynthesizedText = synthesizedText.trim();
    let activeSubagentRuns = countActiveDescendantRuns(params.agentSessionKey);
    const expectedSubagentFollowup = expectsSubagentFollowup(initialSynthesizedText);
    // Also check for already-completed descendants. If the subagent finished
    // before delivery-dispatch runs, activeSubagentRuns is 0 and
    // expectedSubagentFollowup may be false (e.g. cron said "on it" which
    // doesn't match the narrow hint list). We still need to use the
    // descendant's output instead of the interim cron text.
    const completedDescendantReply =
      activeSubagentRuns === 0 && isLikelyInterimCronMessage(initialSynthesizedText)
        ? await readDescendantSubagentFallbackReply({
            sessionKey: params.agentSessionKey,
            runStartedAt: params.runStartedAt,
          })
        : undefined;
    const hadDescendants = activeSubagentRuns > 0 || Boolean(completedDescendantReply);
    if (activeSubagentRuns > 0 || expectedSubagentFollowup) {
      let finalReply = await waitForDescendantSubagentSummary({
        sessionKey: params.agentSessionKey,
        initialReply: initialSynthesizedText,
        timeoutMs: params.timeoutMs,
        observedActiveDescendants: activeSubagentRuns > 0 || expectedSubagentFollowup,
      });
      activeSubagentRuns = countActiveDescendantRuns(params.agentSessionKey);
      if (!finalReply && activeSubagentRuns === 0) {
        finalReply = await readDescendantSubagentFallbackReply({
          sessionKey: params.agentSessionKey,
          runStartedAt: params.runStartedAt,
        });
      }
      if (finalReply && activeSubagentRuns === 0) {
        outputText = finalReply;
        summary = pickSummaryFromOutput(finalReply) ?? summary;
        synthesizedText = finalReply;
        deliveryPayloads = [{ text: finalReply }];
      }
    } else if (completedDescendantReply) {
      // Descendants already finished before we got here. Use their output
      // directly instead of the cron agent's interim text.
      outputText = completedDescendantReply;
      summary = pickSummaryFromOutput(completedDescendantReply) ?? summary;
      synthesizedText = completedDescendantReply;
      deliveryPayloads = [{ text: completedDescendantReply }];
    }
    if (activeSubagentRuns > 0) {
      // Parent orchestration is still in progress; avoid announcing a partial
      // update to the main requester. Mark deliveryAttempted so the timer does
      // not fire a redundant enqueueSystemEvent fallback (double-announce bug).
      deliveryAttempted = true;
      return params.withRunSession({
        status: "ok",
        summary,
        outputText,
        deliveryAttempted,
        ...params.telemetry,
      });
    }
    if (
      hadDescendants &&
      synthesizedText.trim() === initialSynthesizedText &&
      isLikelyInterimCronMessage(initialSynthesizedText) &&
      initialSynthesizedText.toUpperCase() !== SILENT_REPLY_TOKEN.toUpperCase()
    ) {
      // Descendants existed but no post-orchestration synthesis arrived AND
      // no descendant fallback reply was available. Suppress stale parent
      // text like "on it, pulling everything together". Mark deliveryAttempted
      // so the timer does not fire a redundant enqueueSystemEvent fallback.
      deliveryAttempted = true;
      return params.withRunSession({
        status: "ok",
        summary,
        outputText,
        deliveryAttempted,
        ...params.telemetry,
      });
    }
    if (synthesizedText.toUpperCase() === SILENT_REPLY_TOKEN.toUpperCase()) {
      return params.withRunSession({
        status: "ok",
        summary,
        outputText,
        delivered: true,
        ...params.telemetry,
      });
    }
    if (params.isAborted()) {
      return params.withRunSession({
        status: "error",
        error: params.abortReason(),
        deliveryAttempted,
        ...params.telemetry,
      });
    }
    try {
      return await deliverViaDirect(delivery, { retryTransient: true });
    } finally {
      await cleanupDirectCronSessionIfNeeded();
    }
  };

  const deliverViaAnnounce = async (
    delivery: SuccessfulDeliveryTarget,
  ): Promise<RunCronAgentTurnResult | null> => {
    if (!synthesizedText) {
      return null;
    }
    const announceMainSessionKey = resolveAgentMainSessionKey({
      cfg: params.cfg,
      agentId: params.agentId,
    });
    const announceSessionKey = await resolveCronAnnounceSessionKey({
      cfg: params.cfgWithAgentDefaults,
      agentId: params.agentId,
      fallbackSessionKey: announceMainSessionKey,
      delivery: {
        channel: delivery.channel,
        to: delivery.to,
        accountId: delivery.accountId,
        threadId: delivery.threadId,
      },
    });
    const taskLabel =
      typeof params.job.name === "string" && params.job.name.trim()
        ? params.job.name.trim()
        : `cron:${params.job.id}`;
    const initialSynthesizedText = synthesizedText.trim();
    let activeSubagentRuns = countActiveDescendantRuns(params.agentSessionKey);
    const expectedSubagentFollowup = expectsSubagentFollowup(initialSynthesizedText);
    const completedDescendantReply =
      activeSubagentRuns === 0 && isLikelyInterimCronMessage(initialSynthesizedText)
        ? await readDescendantSubagentFallbackReply({
            sessionKey: params.agentSessionKey,
            runStartedAt: params.runStartedAt,
          })
        : undefined;
    const hadDescendants = activeSubagentRuns > 0 || Boolean(completedDescendantReply);
    if (activeSubagentRuns > 0 || expectedSubagentFollowup) {
      let finalReply = await waitForDescendantSubagentSummary({
        sessionKey: params.agentSessionKey,
        initialReply: initialSynthesizedText,
        timeoutMs: params.timeoutMs,
        observedActiveDescendants: activeSubagentRuns > 0 || expectedSubagentFollowup,
      });
      activeSubagentRuns = countActiveDescendantRuns(params.agentSessionKey);
      if (!finalReply && activeSubagentRuns === 0) {
        finalReply = await readDescendantSubagentFallbackReply({
          sessionKey: params.agentSessionKey,
          runStartedAt: params.runStartedAt,
        });
      }
      if (finalReply && activeSubagentRuns === 0) {
        outputText = finalReply;
        summary = pickSummaryFromOutput(finalReply) ?? summary;
        synthesizedText = finalReply;
        deliveryPayloads = [{ text: finalReply }];
      }
    } else if (completedDescendantReply) {
      outputText = completedDescendantReply;
      summary = pickSummaryFromOutput(completedDescendantReply) ?? summary;
      synthesizedText = completedDescendantReply;
      deliveryPayloads = [{ text: completedDescendantReply }];
    }
    if (activeSubagentRuns > 0) {
      deliveryAttempted = true;
      return params.withRunSession({
        status: "ok",
        summary,
        outputText,
        deliveryAttempted,
        ...params.telemetry,
      });
    }
    if (
      hadDescendants &&
      synthesizedText.trim() === initialSynthesizedText &&
      isLikelyInterimCronMessage(initialSynthesizedText) &&
      initialSynthesizedText.toUpperCase() !== SILENT_REPLY_TOKEN.toUpperCase()
    ) {
      deliveryAttempted = true;
      return params.withRunSession({
        status: "ok",
        summary,
        outputText,
        deliveryAttempted,
        ...params.telemetry,
      });
    }
    if (synthesizedText.toUpperCase() === SILENT_REPLY_TOKEN.toUpperCase()) {
      return params.withRunSession({
        status: "ok",
        summary,
        outputText,
        delivered: true,
        ...params.telemetry,
      });
    }
    try {
      if (params.isAborted()) {
        return params.withRunSession({
          status: "error",
          error: params.abortReason(),
          deliveryAttempted,
          ...params.telemetry,
        });
      }
      deliveryAttempted = true;
      const didAnnounce = await runSubagentAnnounceFlow({
        childSessionKey: params.agentSessionKey,
        childRunId: `${params.job.id}:${params.runSessionId}:${params.runStartedAt}`,
        requesterSessionKey: announceSessionKey,
        requesterOrigin: {
          channel: delivery.channel,
          to: delivery.to,
          accountId: delivery.accountId,
          threadId: delivery.threadId,
        },
        requesterDisplayKey: announceSessionKey,
        task: taskLabel,
        timeoutMs: params.timeoutMs,
        cleanup: params.job.deleteAfterRun ? "delete" : "keep",
        roundOneReply: synthesizedText,
        expectsCompletionMessage: true,
        bestEffortDeliver: false,
        waitForCompletion: false,
        startedAt: params.runStartedAt,
        endedAt: params.runEndedAt,
        outcome: { status: "ok" },
        announceType: "cron job",
        signal: params.abortSignal,
      });
      if (didAnnounce) {
        delivered = true;
      } else {
        const message = "cron announce delivery failed";
        logWarn(`[cron:${params.job.id}] ${message}`);
        if (!params.deliveryBestEffort) {
          return params.withRunSession({
            status: "ok",
            summary,
            outputText,
            error: message,
            delivered: false,
            deliveryAttempted,
            ...params.telemetry,
          });
        }
      }
    } catch (err) {
      logWarn(`[cron:${params.job.id}] ${String(err)}`);
      if (!params.deliveryBestEffort) {
        return params.withRunSession({
          status: "ok",
          summary,
          outputText,
          error: String(err),
          delivered: false,
          deliveryAttempted,
          ...params.telemetry,
        });
      }
    }
    return null;
  };

  if (params.deliveryRequested && !params.skipHeartbeatDelivery && !skipMessagingToolDelivery) {
    if (!params.resolvedDelivery.ok) {
      if (!params.deliveryBestEffort) {
        return {
          result: failDeliveryTarget(params.resolvedDelivery.error.message),
          delivered,
          deliveryAttempted,
          summary,
          outputText,
          synthesizedText,
          deliveryPayloads,
        };
      }
      logWarn(`[cron:${params.job.id}] ${params.resolvedDelivery.error.message}`);
      return {
        result: params.withRunSession({
          status: "ok",
          summary,
          outputText,
          deliveryAttempted,
          ...params.telemetry,
        }),
        delivered,
        deliveryAttempted,
        summary,
        outputText,
        synthesizedText,
        deliveryPayloads,
      };
    }

    // Keep summary delivery on the shared announce path so isolated cron jobs
    // still produce the caller-facing rewritten summary. `format=full` and
    // targets that require direct outbound sends bypass that path.
    const useDirectDelivery =
      params.deliveryFormat === "full" ||
      params.deliveryPayloadHasStructuredContent ||
      params.resolvedDelivery.threadId != null;
    if (useDirectDelivery) {
      const directResult = params.deliveryPayloadHasStructuredContent
        ? await deliverViaDirect(params.resolvedDelivery)
        : await finalizeTextDelivery(params.resolvedDelivery);
      if (directResult) {
        return {
          result: directResult,
          delivered,
          deliveryAttempted,
          summary,
          outputText,
          synthesizedText,
          deliveryPayloads,
        };
      }
    } else {
      const announceResult = await deliverViaAnnounce(params.resolvedDelivery);
      if (announceResult) {
        return {
          result: announceResult,
          delivered,
          deliveryAttempted,
          summary,
          outputText,
          synthesizedText,
          deliveryPayloads,
        };
      }
    }
  }

  return {
    delivered,
    deliveryAttempted,
    summary,
    outputText,
    synthesizedText,
    deliveryPayloads,
  };
}

// worker-v15.ts — dispatch worker Telegram, timing adaptativo por grupo
//
// ═══════════════════════════════════════════════════════════════════════════
// HISTÓRICO DE FIXES (v1–v13 preservados para referência)
// ═══════════════════════════════════════════════════════════════════════════
//
// Fix v1:  firingNow Set previne duplo disparo
// Fix v2:  BUG reconnect loop, peerCache stale, backoff, prewarm awaited
// Fix v3:  AUTH_KEY_DUPLICATED via race em getClient() → connectingPromises
// Fix v4:  invoke direto, pre-fetch, pre-resolve peers, noWebpage, randomId
// Fix v5:  sniperFireClosed() — loop ultra-agressivo para grupos fechados
// Fix v6:  duplo disparo, groupType perdido, FloodWait, reloadClient, keepalive
// Fix v7:  timing logs + SNIPER_BEFORE_MS aumentado para 45ms
// Fix v8:  randomId estável por ciclo (deduplicação Telegram)
// Fix v9:  SNIPER_BEFORE_MS=200ms warm-up + SNIPER_AFTER_GUARD_MS=15ms busy-wait
// Fix v10: Adaptive Gate — timing dinâmico p10/p50/p90, SNIPER_SPIN_MAX_MS=2ms
// Fix v11: Smart Loop — sniperAttemptOnce classificado (too_early/fatal/flood/transient),
//          tooEarlyCount, measureTelegramClockOffset, persistGroupDispatchSample estendido
// Fix v12: Multi-probe Clock + Dual-mode Sniper + Rate Limit Shield
// Fix v13: Throttle corrigido (base 1ms) + guardMs sem corte de CONNECTION_BUDGET
//
// ═══════════════════════════════════════════════════════════════════════════
// Fix v14 — Loop sempre agressivo. Gate só bloqueia opens_early antes do horário
// Fix v15 — RTT-aware busy-wait: compensa half-RTT no floor de opens_early
// ═══════════════════════════════════════════════════════════════════════════
//
// FILOSOFIA DO SNIPER (v15):
//
//   O loop de 1ms é sempre ativo desde o momento que o sniper entra.
//   SNIPER_BEFORE_MS serve apenas para aquecer a conexão — não para criar sleep.
//
//   A ÚNICA restrição de timing é para grupos opens_early:
//     invokeNotBefore = scheduledAt - estimatedOneWayRttMs
//
//   POR QUÊ: o invoke tem RTT (ida+volta). Se esperarmos até scheduledAt para
//   COMEÇAR o invoke, o pacote chega ao servidor ~halfRtt ms DEPOIS do horário.
//   Isso é seguro para grupos opens_early que permitem envio apenas a partir
//   do horário — mas o sintoma relatado é o oposto: mensagem chegando ANTES.
//
//   O problema ocorre quando o relógio local está ADIANTADO em relação ao
//   servidor Telegram (telegramClockOffsetMs < 0). Nesse caso Date.now()
//   já passou de scheduledAt, mas o servidor ainda está antes do horário.
//   O busy-wait não ajuda porque compara relógio local com scheduledAt local.
//
//   SOLUÇÃO v15:
//     invokeDeadline = scheduledAt - estimatedOneWayRttMs
//     onde estimatedOneWayRttMs = max(0, avgRtt/2) medido nas clock probes.
//     O invoke começa quando Date.now() >= invokeDeadline, garantindo que
//     o pacote CHEGA ao servidor exatamente em scheduledAt.
//
//     Se clockOffsetQuality = 'high', usa também telegramClockOffsetMs para
//     ajustar scheduledAt para o referencial do servidor antes de subtrair RTT.
//
//   Resumo:
//     opens_early → spin até (scheduledAt_servidor - halfRtt), depois loop
//     qualquer outro caso → loop agressivo imediato, sem sleep
//
// FIXES v13/v14 MANTIDOS:
//   - Throttle progressivo suave (base 1ms, começa em 200 too_early)
//   - guardMs sem CONNECTION_BUDGET
//   - SNIPER_TRANSIENT_INTERVAL_MS = 1ms
//   - Rate limit shield 25 req/s


import { createClient } from "@supabase/supabase-js";
import { TelegramClient, Api } from "telegram";
import { StringSession } from "telegram/sessions";
import bigInt from "big-integer";
import http from "http";

/* ─────────────────────────────────────────────────────────────────────────────
   SUPABASE
   ───────────────────────────────────────────────────────────────────────────── */
const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

/* ─────────────────────────────────────────────────────────────────────────────
   CONSTANTES
   ───────────────────────────────────────────────────────────────────────────── */

const SEND_TIMEOUT_MS               = 15_000;
const RETRY_BUDGET_MS               = 50_000;
const RELOAD_INTERVAL_MS            = 30_000;
const LOOKAHEAD_MS                  = 2 * 60 * 1000;
const KEEPALIVE_INTERVAL_MS         = 45_000;
const KEEPALIVE_JITTER_MAX_MS       = 10_000;
const PREFETCH_BEFORE_MS            = 800;
const MONITOR_DELAY_CLOSED_MS       = 6_000;
const MONITOR_MAX_OPEN_MS           = 5 * 60_000;
const MONITOR_POLL_MS               = 5_000;
const LISTEN_POLL_MS                = 400;
const MONITOR_HISTORY_LIMIT         = 150;
const OPEN_GROUP_LISTEN_TIMEOUT_MS  = 2 * 60 * 60_000;
const SEND_RETRY_BACKOFF_MAX_MS     = 8_000;

// Quanto antes do horário o sniper é acordado — apenas para ter conexão quente.
// Não cria sleep: o invoke começa imediatamente ao entrar (exceto opens_early).
const SNIPER_BEFORE_MS              = 100;

// Spin de precisão no busy-wait do opens_early
const SNIPER_SPIN_MAX_MS            = 2;

const GROUP_PROFILE_SAMPLE_SIZE     = 20;
const CLOCK_OFFSET_REFRESH_MS       = 5 * 60_000;
const CLOCK_PROBE_COUNT             = 5;

const SNIPER_SEND_TIMEOUT_MS        = 800;

// Loop too_early: base 1ms, throttle suave só depois de muitas tentativas
const SNIPER_TOO_EARLY_BASE_INTERVAL_MS = 1;
const SNIPER_TRANSIENT_INTERVAL_MS      = 1;

// Throttle progressivo — só entra depois de 200 too_early para não prejudicar
// grupos que abrem poucos ms depois do horário
const TOO_EARLY_THROTTLE_STEPS: Array<[number, number]> = [
  [200,      2],
  [1000,     5],
  [3000,    10],
  [Infinity, 15],
];

// Rate limit shield: 25 req/s por conta
const SNIPER_MAX_REQ_PER_SECOND_PER_ACCOUNT = 25;

const SNIPER_PAUSE_EVERY_N          = 10;
const SNIPER_PAUSE_MS               = 5;
const SNIPER_INTER_ACCOUNT_DELAY_MS = 1;
const SNIPER_BUDGET_MS              = RETRY_BUDGET_MS;
const SNIPER_DONE_BLOCK_TTL_MS      = 500;

const WORKER_PORT   = parseInt(process.env.PORT ?? "3001", 10);
const WORKER_SECRET = process.env.WORKER_SECRET ?? "";

/* ─────────────────────────────────────────────────────────────────────────────
   TIPOS
   ───────────────────────────────────────────────────────────────────────────── */
interface Account {
  id: string;
  name: string;
  phone_number: string;
  api_id: string;
  api_hash: string;
  session_string: string;
  is_active: boolean;
}

interface GroupMember {
  id: string;
  message_text: string | null;
  position: number;
  is_active: boolean;
  accounts: Account | null;
}

interface Group {
  id: string;
  name: string;
  telegram_chat_id: string | null;
  telegram_chat_name: string | null;
  group_type: "open" | "closed";
  group_members: GroupMember[];
}

interface Schedule {
  id: string;
  cron_expression: string;
  user_id: string;
  group_id: string;
  next_run_at: string;
  retry_window_seconds: number;
  retry_interval_seconds: number;
  retry_interval_max_seconds: number;
  retry_count: number;
  retry_until: string | null;
  last_attempt_at: string | null;
  groups: Group;
}

interface DispatchResult {
  account_id: string;
  message_text: string | null;
  status: "sent" | "failed" | "skipped";
  retryable: boolean;
  error?: string;
}

type SniperOutcome = "sent" | "too_early" | "fatal" | "flood" | "transient";

interface SniperAttemptResult {
  outcome: SniperOutcome;
  sentAt?: number;
  floodWaitSecs?: number;
  errorCode?: string;
}

interface GroupBehaviorProfile {
  group_id:                string;
  offset_p10_ms:           number;
  offset_p50_ms:           number;
  offset_p90_ms:           number;
  opens_early:             boolean;
  min_safe_guard_ms:       number;
  sample_count:            number;
  open_at_start_ratio:     number;
  median_too_early_count:  number;
  estimated_open_delay_ms: number;
  clock_offset_ms:         number;
}

interface TokenBucket {
  tokens:     number;
  lastRefill: number;
}

/* ─────────────────────────────────────────────────────────────────────────────
   ESTADO GLOBAL
   ───────────────────────────────────────────────────────────────────────────── */

const clients               = new Map<string, TelegramClient>();
const sessions              = new Map<string, string>();
const keepaliveTimers       = new Map<string, ReturnType<typeof setInterval>>();
const peerCache             = new Map<string, unknown>();
const accountCache          = new Map<string, Account>();
const scheduledTimers       = new Map<string, ReturnType<typeof setTimeout>>();
const prefetchTimers        = new Map<string, ReturnType<typeof setTimeout>>();
const sniperTimers          = new Map<string, ReturnType<typeof setTimeout>>();
const schedulePrefetchCache = new Map<string, Schedule>();
const listenMap             = new Map<string, AbortController>();
const firingNow             = new Set<string>();
const sniperFiringNow       = new Set<string>();
const connectingPromises    = new Map<string, Promise<TelegramClient>>();
const groupProfileCache     = new Map<string, GroupBehaviorProfile>();
const scheduleGroupMap      = new Map<string, string>();
const sniperTokenBuckets    = new Map<string, TokenBucket>();

let telegramClockOffsetMs = 0;
let clockOffsetQuality: "high" | "low" | "unknown" = "unknown";
let estimatedOneWayRttMs  = 0; // half-RTT médio das clock probes — usado no floor de opens_early

/* ─────────────────────────────────────────────────────────────────────────────
   QUERY REUTILIZADA
   ───────────────────────────────────────────────────────────────────────────── */
const SCHEDULE_SELECT = `
  id, cron_expression, user_id, group_id, next_run_at,
  retry_window_seconds, retry_interval_seconds, retry_interval_max_seconds,
  retry_count, retry_until, last_attempt_at,
  groups(
    id, name, telegram_chat_id, telegram_chat_name, group_type,
    group_members(
      id, message_text, position, is_active,
      accounts(id, name, phone_number, api_id, api_hash, session_string, is_active)
    )
  )
`.trim();

/* ─────────────────────────────────────────────────────────────────────────────
   HELPERS PUROS
   ───────────────────────────────────────────────────────────────────────────── */

function isRetryableError(msg: string): boolean {
  const u = msg.toUpperCase();
  return !u.includes("AUTH_KEY_UNREGISTERED") &&
         !u.includes("USER_DEACTIVATED") &&
         !u.includes("SESSION_REVOKED");
}

function nextWeeklyOccurrence(cron: string): string {
  const parts = cron.trim().split(/\s+/);
  const mi    = parseInt(parts[0], 10);
  const h     = parseInt(parts[1], 10);
  const dow   = parseInt(parts[4], 10);

  if (
    parts.length < 5 ||
    isNaN(mi) || isNaN(h) || isNaN(dow) ||
    mi < 0 || mi > 59 || h < 0 || h > 23 || dow < 0 || dow > 6
  ) {
    throw new Error(`cron_expression inválida: "${cron}"`);
  }

  const now = new Date();
  let daysUntil = (dow - now.getUTCDay() + 7) % 7;

  if (daysUntil === 0) {
    const nowMins  = now.getUTCHours() * 60 + now.getUTCMinutes();
    const targMins = h * 60 + mi;
    if (targMins <= nowMins) daysUntil = 7;
  }

  const next = new Date(now);
  next.setUTCDate(next.getUTCDate() + daysUntil);
  next.setUTCHours(h, mi, 0, 0);
  return next.toISOString();
}

function calcRetryInterval(count: number, base: number, max: number): number {
  return Math.min(base * Math.pow(2, count), max);
}

function isRetryDue(schedule: Schedule, now: Date): boolean {
  if (!schedule.last_attempt_at) return true;
  const last     = new Date(schedule.last_attempt_at);
  const interval = calcRetryInterval(
    schedule.retry_count,
    schedule.retry_interval_seconds,
    schedule.retry_interval_max_seconds
  );
  return now >= new Date(last.getTime() + interval * 1000);
}

function makeRandomId(): bigInt.BigInteger {
  const hi = Math.floor(Math.random() * 0xFFFFFFFF);
  const lo = Math.floor(Math.random() * 0xFFFFFFFF);
  return bigInt(hi).shiftLeft(32).add(bigInt(lo));
}

function getTooEarlyIntervalMs(tooEarlyCount: number): number {
  for (const [limit, intervalMs] of TOO_EARLY_THROTTLE_STEPS) {
    if (tooEarlyCount < limit) return intervalMs;
  }
  return 15;
}

/* ─────────────────────────────────────────────────────────────────────────────
   HELPERS DE PEER CACHE
   ───────────────────────────────────────────────────────────────────────────── */

function evictPeersForAccount(accountId: string): void {
  for (const key of peerCache.keys()) {
    if (key.startsWith(`${accountId}:`)) {
      peerCache.delete(key);
    }
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   TOKEN BUCKET RATE LIMITER
   ───────────────────────────────────────────────────────────────────────────── */

function acquireTokenBucket(accountId: string): number {
  const now   = Date.now();
  let bucket  = sniperTokenBuckets.get(accountId);

  if (!bucket) {
    bucket = { tokens: SNIPER_MAX_REQ_PER_SECOND_PER_ACCOUNT, lastRefill: now };
    sniperTokenBuckets.set(accountId, bucket);
  }

  const elapsed   = now - bucket.lastRefill;
  const newTokens = (elapsed / 1000) * SNIPER_MAX_REQ_PER_SECOND_PER_ACCOUNT;

  if (newTokens >= 1) {
    bucket.tokens    = Math.min(SNIPER_MAX_REQ_PER_SECOND_PER_ACCOUNT, bucket.tokens + Math.floor(newTokens));
    bucket.lastRefill = now;
  }

  if (bucket.tokens >= 1) {
    bucket.tokens--;
    return 0;
  }

  const msPerToken = 1000 / SNIPER_MAX_REQ_PER_SECOND_PER_ACCOUNT;
  return Math.ceil(msPerToken - elapsed % msPerToken);
}

/* ─────────────────────────────────────────────────────────────────────────────
   MEDIÇÃO DE CLOCK OFFSET MULTI-PROBE
   ───────────────────────────────────────────────────────────────────────────── */

async function measureClockOffsetViaSelfSend(client: TelegramClient): Promise<number> {
  const probes: number[] = [];
  const probe_rtt: number[] = [];

  for (let i = 0; i < CLOCK_PROBE_COUNT; i++) {
    try {
      const t0 = Date.now();
      const result = await Promise.race([
        client.invoke(new Api.messages.SendMessage({
          peer:      new Api.InputPeerSelf(),
          message:   `[clock-probe-${Date.now()}]`,
          randomId:  makeRandomId(),
          noWebpage: true,
        })) as Promise<any>,
        new Promise<never>((_, r) => setTimeout(() => r(new Error("PROBE_TIMEOUT")), 3_000)),
      ]);
      const t1  = Date.now();
      const rtt = t1 - t0;

      let serverDateSec: number | null = null;
      if (result?.date) {
        serverDateSec = result.date;
      } else if (result?.updates) {
        const upd = result.updates?.find?.((u: any) => u?.date);
        if (upd?.date) serverDateSec = upd.date;
      }

      if (serverDateSec != null) {
        const serverTimeMs   = serverDateSec * 1000 + 500;
        const localMidpoint  = t0 + rtt / 2;
        const offset         = serverTimeMs - localMidpoint;
        probes.push(offset);
        probe_rtt.push(rtt);
      }

      if (i < CLOCK_PROBE_COUNT - 1) {
        await new Promise(r => setTimeout(r, 200));
      }
    } catch {
      // probe falhou — ignora
    }
  }

  if (probes.length === 0) return telegramClockOffsetMs;

  const avgRtt   = probe_rtt.reduce((a, b) => a + b, 0) / probe_rtt.length;
  const stdRtt   = Math.sqrt(probe_rtt.map(r => (r - avgRtt) ** 2).reduce((a, b) => a + b, 0) / probe_rtt.length);
  const filtered = probes.filter((_, i) => probe_rtt[i] <= avgRtt + 2 * stdRtt);

  if (filtered.length === 0) return telegramClockOffsetMs;

  const sorted = [...filtered].sort((a, b) => a - b);
  const median = sorted[Math.floor(sorted.length / 2)];

  if (Math.abs(median) > 10_000) {
    console.warn(`[clock] Offset mediano descartado (${Math.round(median)}ms > 10s) — mantendo ${telegramClockOffsetMs}ms`);
    return telegramClockOffsetMs;
  }

  // Atualiza RTT estimado (half-RTT = tempo que o pacote leva até o servidor)
  const filteredRtts = probe_rtt.filter((_, i) => probe_rtt[i] <= avgRtt + 2 * stdRtt);
  if (filteredRtts.length > 0) {
    const avgFilteredRtt = filteredRtts.reduce((a, b) => a + b, 0) / filteredRtts.length;
    estimatedOneWayRttMs = Math.max(0, Math.round(avgFilteredRtt / 2));
  }

  console.log(
    `[clock] Multi-probe self-send: ${filtered.length}/${CLOCK_PROBE_COUNT} probes válidos | ` +
    `offsets=[${filtered.map(o => Math.round(o)).join(",")}]ms | ` +
    `mediana=${Math.round(median)}ms | avgRtt=${Math.round(avgRtt)}ms | halfRtt=${estimatedOneWayRttMs}ms`
  );

  return Math.round(median);
}

async function measureTelegramClockOffset(client: TelegramClient): Promise<number> {
  try {
    const offset = await measureClockOffsetViaSelfSend(client);
    clockOffsetQuality = "high";
    return offset;
  } catch (err: any) {
    console.warn(`[clock] Self-send falhou (${err.message}) — caindo em GetConfig (baixa precisão)`);
  }

  try {
    const t0     = Date.now();
    const config = await (client as any).invoke(new Api.help.GetConfig()) as any;
    const t1     = Date.now();

    if (typeof config?.date !== "number") return telegramClockOffsetMs;

    const rttMs           = t1 - t0;
    const serverTimeMs    = config.date * 1000 + 500;
    const localAtMidpoint = t0 + rttMs / 2;
    const offsetMs        = serverTimeMs - localAtMidpoint;

    if (Math.abs(offsetMs) > 5_000) return telegramClockOffsetMs;

    clockOffsetQuality = "low";
    console.log(`[clock] GetConfig fallback: offset=${Math.round(offsetMs)}ms (±500ms precisão) rtt=${rttMs}ms`);
    return Math.round(offsetMs);
  } catch (err: any) {
    console.warn(`[clock] GetConfig também falhou: ${err.message}`);
    return telegramClockOffsetMs;
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   PERFIS DE TIMING ADAPTATIVO
   ───────────────────────────────────────────────────────────────────────────── */

async function loadGroupProfiles(): Promise<void> {
  try {
    const { data, error } = await supabase
      .from("group_profiles")
      .select(
        "group_id, offset_p10_ms, offset_p50_ms, offset_p90_ms, opens_early, " +
        "min_safe_guard_ms, sample_count, " +
        "open_at_start_ratio, median_too_early_count, estimated_open_delay_ms"
      );

    if (error) {
      console.warn("[profiles] Falha ao carregar perfis de grupo:", error.message);
      return;
    }

    for (const row of data ?? []) {
      if (!row || typeof row !== "object" || !('group_id' in row)) continue;
      groupProfileCache.set((row as any).group_id as string, {
        ...(row as any),
        open_at_start_ratio:     (row as any).open_at_start_ratio     ?? 0,
        median_too_early_count:  (row as any).median_too_early_count  ?? 0,
        estimated_open_delay_ms: (row as any).estimated_open_delay_ms ?? 0,
        clock_offset_ms:         0,
      } as GroupBehaviorProfile);
    }

    console.log(`[profiles] ✅ ${groupProfileCache.size} perfis de timing carregados`);
  } catch (err: any) {
    console.warn("[profiles] Erro ao carregar perfis:", err.message);
  }
}

async function persistGroupDispatchSample(
  groupId: string,
  vsHorarioMs: number,
  tooEarlyCount: number
): Promise<void> {
  try {
    const { error: insertErr } = await supabase
      .from("group_dispatch_samples")
      .insert({ group_id: groupId, vs_horario_ms: vsHorarioMs, too_early_count: tooEarlyCount });

    if (insertErr) console.warn(`[profiles] Falha ao inserir amostra para ${groupId}:`, insertErr.message);

    const { data, error: fetchErr } = await supabase
      .from("group_dispatch_samples")
      .select("vs_horario_ms, too_early_count")
      .eq("group_id", groupId)
      .order("created_at", { ascending: false })
      .limit(GROUP_PROFILE_SAMPLE_SIZE);

    if (fetchErr || !data || data.length === 0) return;

    const vsHorarioSamples = data.map(r => r.vs_horario_ms as number);
    const sortedVs         = [...vsHorarioSamples].sort((a, b) => a - b);
    const n                = sortedVs.length;

    const p10 = sortedVs[Math.max(0, Math.floor(n * 0.10))] ?? sortedVs[0];
    const p50 = sortedVs[Math.max(0, Math.floor(n * 0.50))] ?? sortedVs[0];
    const p90 = sortedVs[Math.min(n - 1, Math.floor(n * 0.90))] ?? sortedVs[n - 1];

    const tooEarlyCounts         = data.map(r => (r.too_early_count as number) ?? 0);
    const openAtStartRatio       = tooEarlyCounts.filter(c => c === 0).length / n;
    const sortedTooEarly         = [...tooEarlyCounts].sort((a, b) => a - b);
    const medianTooEarlyCount    = sortedTooEarly[Math.floor(n / 2)] ?? 0;
    const estimatedOpenDelayMs   = medianTooEarlyCount * SNIPER_TOO_EARLY_BASE_INTERVAL_MS;

    // opens_early: grupo historicamente abre ANTES do horário agendado
    // → precisa do floor em scheduledAt para não mandar antes da hora
    const opensEarlyByVsHorario  = p50 < -20;
    const opensEarlyByRatio      = openAtStartRatio > 0.6;
    const opens_early            = opensEarlyByVsHorario || opensEarlyByRatio;

    // min_safe_guard_ms: apenas para referência/logging, não usado no gate v14
    const min_safe_guard_ms = opens_early
      ? Math.max(-30_000, p10 - 50)
      : Math.max(0, estimatedOpenDelayMs - 20);

    const profile: GroupBehaviorProfile = {
      group_id:               groupId,
      offset_p10_ms:          p10,
      offset_p50_ms:          p50,
      offset_p90_ms:          p90,
      opens_early,
      min_safe_guard_ms,
      sample_count:           n,
      open_at_start_ratio:    openAtStartRatio,
      median_too_early_count: medianTooEarlyCount,
      estimated_open_delay_ms: estimatedOpenDelayMs,
      clock_offset_ms:        telegramClockOffsetMs,
    };

    groupProfileCache.set(groupId, profile);

    const { error: upsertErr } = await supabase
      .from("group_profiles")
      .upsert(
        {
          group_id:               groupId,
          offset_p10_ms:          p10,
          offset_p50_ms:          p50,
          offset_p90_ms:          p90,
          opens_early,
          min_safe_guard_ms,
          sample_count:           n,
          open_at_start_ratio:    openAtStartRatio,
          median_too_early_count: medianTooEarlyCount,
          estimated_open_delay_ms: estimatedOpenDelayMs,
          updated_at:             new Date().toISOString(),
        },
        { onConflict: "group_id" }
      );

    if (upsertErr) {
      console.warn(`[profiles] Falha ao salvar perfil para ${groupId}:`, upsertErr.message);
    } else {
      console.log(
        `[profiles] ✅ group=${groupId} ` +
        `p10=${p10}ms p50=${p50}ms p90=${p90}ms ` +
        `opens_early=${opens_early} (vsHorário=${opensEarlyByVsHorario}, ratio=${opensEarlyByRatio}) ` +
        `openAtStart=${(openAtStartRatio * 100).toFixed(0)}% ` +
        `medianTooEarly=${medianTooEarlyCount} openDelay≈${estimatedOpenDelayMs}ms n=${n}`
      );
    }
  } catch (err: any) {
    console.warn(`[profiles] Erro ao persistir amostra para ${groupId}:`, err.message);
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   GERENCIAMENTO DE CONEXÕES TELEGRAM
   ───────────────────────────────────────────────────────────────────────────── */

async function getClient(account: Account): Promise<TelegramClient> {
  const existing       = clients.get(account.id);
  const sessionInUse   = sessions.get(account.id);
  const sessionChanged = sessionInUse !== account.session_string;

  if (existing?.connected && !sessionChanged) return existing;

  const inflight = connectingPromises.get(account.id);
  if (inflight) return inflight;

  const connectPromise = (async () => {
    if (existing) {
      try { await existing.disconnect(); } catch {}
      clients.delete(account.id);
      evictPeersForAccount(account.id);
      const t = keepaliveTimers.get(account.id);
      if (t) { clearInterval(t); keepaliveTimers.delete(account.id); }
    }

    const client = new TelegramClient(
      new StringSession(account.session_string),
      parseInt(account.api_id),
      account.api_hash,
      {
        connectionRetries: 5,
        retryDelay: 1_000,
        autoReconnect: true,
        floodSleepThreshold: 60,
        requestRetries: 3,
      }
    );

    (client as any)._loopStarted = true;
    await client.connect();

    clients.set(account.id, client);
    sessions.set(account.id, account.session_string);

    const jitter   = Math.floor(Math.random() * KEEPALIVE_JITTER_MAX_MS);
    const interval = setInterval(async () => {
      if (!client.connected) {
        clients.delete(account.id);
        evictPeersForAccount(account.id);
        keepaliveTimers.delete(account.id);
        clearInterval(interval);
        return;
      }
      try {
        await Promise.race([
          client.getMe(),
          new Promise<never>((_, r) => setTimeout(() => r(new Error("keepalive timeout")), 10_000)),
        ]);
      } catch (err: any) {
        console.warn(`[keepalive] Ping falhou para ${account.phone_number}: ${err.message}`);
        try { await client.disconnect(); } catch {}
        clients.delete(account.id);
        evictPeersForAccount(account.id);
        keepaliveTimers.delete(account.id);
        clearInterval(interval);

        const authDead =
          err.message?.includes("AUTH_KEY_UNREGISTERED") ||
          err.message?.includes("USER_DEACTIVATED")      ||
          err.message?.includes("SESSION_REVOKED");
        if (authDead) {
          supabase.from("accounts").update({ is_active: false }).eq("id", account.id).then(({ error: e }) => {
            if (e) console.error(`[keepalive] Falha ao desativar ${account.id}:`, e.message);
          });
        }
      }
    }, KEEPALIVE_INTERVAL_MS + jitter);

    keepaliveTimers.set(account.id, interval);
    console.log(`[client] ✓ Conectado: ${account.phone_number}`);
    return client;
  })();

  connectingPromises.set(account.id, connectPromise);
  try {
    return await connectPromise;
  } finally {
    connectingPromises.delete(account.id);
  }
}

async function reloadClient(account: Account): Promise<TelegramClient> {
  const inflight = connectingPromises.get(account.id);
  if (inflight) {
    try { await inflight; } catch {}
  }
  connectingPromises.delete(account.id);

  const existing = clients.get(account.id);
  if (existing) {
    try { await existing.disconnect(); } catch {}
    clients.delete(account.id);
    evictPeersForAccount(account.id);
    const t = keepaliveTimers.get(account.id);
    if (t) { clearInterval(t); keepaliveTimers.delete(account.id); }
  }

  return getClient(account);
}

/* ─────────────────────────────────────────────────────────────────────────────
   RESOLUÇÃO DE PEER TELEGRAM
   ───────────────────────────────────────────────────────────────────────────── */
async function resolvePeer(
  client: TelegramClient,
  telegramChatId: string,
  accountId: string
): Promise<unknown> {
  const key = `${accountId}:${telegramChatId}`;
  if (peerCache.has(key)) return peerCache.get(key)!;

  const chatIdNum = parseInt(telegramChatId, 10);
  if (isNaN(chatIdNum)) throw new Error(`telegram_chat_id inválido: "${telegramChatId}"`);

  try {
    const peer = await client.getInputEntity(chatIdNum);
    peerCache.set(key, peer);
    return peer;
  } catch {}

  const absId     = Math.abs(chatIdNum);
  const channelId = absId > 1_000_000_000_000 ? absId - 1_000_000_000_000 : absId;
  try {
    const result = await client.invoke(
      new Api.channels.GetChannels({
        id: [new Api.InputChannel({ channelId: bigInt(channelId), accessHash: bigInt(0) })],
      })
    ) as any;
    const chat = result?.chats?.[0];
    if (chat?.accessHash != null) {
      const peer = new Api.InputPeerChannel({ channelId: chat.id, accessHash: chat.accessHash });
      peerCache.set(key, peer);
      return peer;
    }
  } catch {}

  await client.getDialogs({ limit: 200 });
  try {
    const peer = await client.getInputEntity(chatIdNum);
    peerCache.set(key, peer);
    return peer;
  } catch (e: any) {
    throw new Error(
      `PEER_UNRESOLVABLE ${telegramChatId}: conta não é membro ou sessão inválida. ` +
      `Último erro: ${e.message}`
    );
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   ENVIO COM RETRY INTERNO
   ───────────────────────────────────────────────────────────────────────────── */
async function sendMessage(
  client: TelegramClient,
  account: Account,
  telegramChatId: string,
  messageText: string
): Promise<void> {
  const budgetEnd      = Date.now() + RETRY_BUDGET_MS;
  const stableRandomId = makeRandomId();
  let attempt          = 0;

  while (Date.now() < budgetEnd) {
    attempt++;
    const timeLeft = budgetEnd - Date.now();
    if (timeLeft < 500) break;

    try {
      await Promise.race([
        (async () => {
          const peer = await resolvePeer(client, telegramChatId, account.id);
          try {
            await client.invoke(new Api.messages.SendMessage({
              peer: peer as any, message: messageText, randomId: stableRandomId, noWebpage: true,
            }));
          } catch (err: any) {
            const errMsg = String(err?.message ?? "");
            if (errMsg.includes("PEER_ID_INVALID") || errMsg.includes("CHANNEL_INVALID") || errMsg.includes("CHANNEL_PRIVATE")) {
              peerCache.delete(`${account.id}:${telegramChatId}`);
            }
            const isFlood = err?.seconds != null || err?.constructor?.name === "FloodWaitError" || /flood/i.test(errMsg);
            if (isFlood) {
              const waitSecs = typeof err.seconds === "number" ? err.seconds : parseInt(errMsg.match(/(\d+)/)?.[1] ?? "30", 10);
              const waitMs   = waitSecs * 1000;
              if (waitMs < budgetEnd - Date.now() - 500) {
                await new Promise(r => setTimeout(r, waitMs));
                peerCache.delete(`${account.id}:${telegramChatId}`);
                const freshPeer = await resolvePeer(client, telegramChatId, account.id);
                await client.invoke(new Api.messages.SendMessage({ peer: freshPeer as any, message: messageText, randomId: stableRandomId, noWebpage: true }));
                return;
              }
              throw new Error(`FLOOD_WAIT_${waitSecs}_EXCEEDS_BUDGET`);
            }
            throw err;
          }
        })(),
        new Promise<never>((_, r) => setTimeout(() => r(new Error(`TIMEOUT tentativa ${attempt}`)), Math.min(SEND_TIMEOUT_MS, timeLeft - 100))),
      ]);
      if (attempt > 1) console.log(`[send] ✓ ${account.phone_number} — enviou na tentativa ${attempt}`);
      return;
    } catch (err: any) {
      const remaining = budgetEnd - Date.now();
      if (remaining > 500) {
        const backoffMs   = Math.min(1_000 * Math.pow(2, attempt - 1), SEND_RETRY_BACKOFF_MAX_MS);
        const safeBackoff = Math.min(backoffMs, remaining - 500);
        if (safeBackoff > 0) await new Promise(r => setTimeout(r, safeBackoff));
      }
    }
  }

  throw new Error(`BUDGET_EXCEEDED após ${attempt} tentativa(s) em ${RETRY_BUDGET_MS / 1000}s`);
}

/* ─────────────────────────────────────────────────────────────────────────────
   SNIPER — TENTATIVA CLASSIFICADA
   ───────────────────────────────────────────────────────────────────────────── */
async function sniperAttemptOnce(
  client: TelegramClient,
  account: Account,
  telegramChatId: string,
  messageText: string,
  randomId: bigInt.BigInteger
): Promise<SniperAttemptResult> {
  try {
    const peer = await resolvePeer(client, telegramChatId, account.id);
    await Promise.race([
      client.invoke(new Api.messages.SendMessage({
        peer: peer as any, message: messageText, randomId, noWebpage: true,
      })),
      new Promise<never>((_, r) => setTimeout(() => r(new Error("SNIPER_TIMEOUT")), SNIPER_SEND_TIMEOUT_MS)),
    ]);
    return { outcome: "sent", sentAt: Date.now() };
  } catch (err: any) {
    const msg = String(err?.message ?? "");

    if (
      msg.includes("CHAT_WRITE_FORBIDDEN")       ||
      msg.includes("CHAT_SEND_PLAIN_FORBIDDEN")  ||
      msg.includes("CHAT_SEND_MEDIA_FORBIDDEN")  ||
      msg.includes("CHAT_RESTRICTED")
    ) {
      return { outcome: "too_early", errorCode: msg };
    }

    if (msg.includes("MESSAGE_ID_INVALID") || msg.includes("RANDOM_ID_DUPLICATE")) {
      return { outcome: "sent", sentAt: Date.now() };
    }

    if (msg.includes("AUTH_KEY_UNREGISTERED") || msg.includes("USER_DEACTIVATED") || msg.includes("SESSION_REVOKED")) {
      return { outcome: "fatal", errorCode: msg };
    }

    const isFlood = err?.seconds != null || err?.constructor?.name === "FloodWaitError" || /flood/i.test(msg);
    if (isFlood) {
      const floodWaitSecs = typeof err.seconds === "number" ? err.seconds : parseInt(msg.match(/(\d+)/)?.[1] ?? "5", 10);
      return { outcome: "flood", floodWaitSecs, errorCode: msg };
    }

    if (msg.includes("PEER_ID_INVALID") || msg.includes("CHANNEL_INVALID") || msg.includes("CHANNEL_PRIVATE")) {
      peerCache.delete(`${account.id}:${telegramChatId}`);
    }

    return { outcome: "transient", errorCode: msg };
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   SNIPER LOOP — GRUPOS FECHADOS
   ═══════════════════════════════════════════════════════════════════════════
   FILOSOFIA v14:
     Loop de 1ms SEMPRE agressivo desde o momento que o sniper entra.
     ÚNICA exceção: opens_early → busy-wait até scheduledAt para não
     mandar antes do horário agendado. Após scheduledAt, loop agressivo normal.

     Grupos sem perfil / grupos normais / grupos com delay:
       → invoke imediato, se vier CHAT_WRITE_FORBIDDEN o loop trata como
         too_early e continua em 1ms até o grupo abrir.
   ───────────────────────────────────────────────────────────────────────────── */
async function sniperFireClosed(scheduleId: string): Promise<void> {
  if (sniperFiringNow.has(scheduleId)) {
    console.warn(`[sniper] Schedule ${scheduleId} já em execução — ignorando disparo duplicado`);
    return;
  }
  sniperFiringNow.add(scheduleId);

  const sniperEnteredAt = Date.now();

  try {
    const now = new Date();

    let schedule = schedulePrefetchCache.get(scheduleId);
    if (schedule) {
      schedulePrefetchCache.delete(scheduleId);
      console.log(`[sniper] ⚡ Schedule ${scheduleId} servido do pre-fetch cache`);
    } else {
      const { data, error } = await supabase
        .from("schedules").select(SCHEDULE_SELECT)
        .eq("id", scheduleId).eq("is_active", true).single();
      if (error || !data) {
        console.warn(`[sniper] Schedule ${scheduleId} não encontrado ou inativo.`);
        return;
      }
      schedule = data as unknown as Schedule;
    }

    const scheduledAtRaw = new Date(schedule.next_run_at).getTime();
    const scheduledAt    = scheduledAtRaw; // clock offset não aplicado ao gate

    const timerLagMs = sniperEnteredAt - (scheduledAt - SNIPER_BEFORE_MS);
    console.log(
      `[sniper][timing] entrou ${Math.abs(timerLagMs)}ms ${timerLagMs >= 0 ? "após" : "antes d"} o planejado | ` +
      `clockOffset(diag)=${telegramClockOffsetMs > 0 ? "+" : ""}${telegramClockOffsetMs}ms ` +
      `(qualidade: ${clockOffsetQuality}) | scheduledAt=${new Date(scheduledAt).toISOString()}`
    );

    const group = schedule.groups;
    if (!group?.telegram_chat_id) {
      console.warn(`[sniper] Schedule ${scheduleId}: sem telegram_chat_id — pulando.`);
      return;
    }

    if (group.group_members) {
      group.group_members = group.group_members.map(m => ({
        ...m,
        accounts: m.accounts ? (accountCache.get(m.accounts.id) ?? m.accounts) : null,
      }));
    }

    const members = (group.group_members ?? [])
      .filter(m => m.is_active && m.accounts?.is_active && m.accounts?.session_string)
      .sort((a, b) => a.position - b.position);

    if (members.length === 0) {
      console.warn(`[sniper] Nenhuma conta ativa no schedule ${scheduleId} — abortando.`);
      return;
    }

    const chatId    = group.telegram_chat_id;
    const budgetEnd = Date.now() + SNIPER_BUDGET_MS;

    const firstMember  = members[0];
    const firstAccount = firstMember.accounts!;
    const firstText    = firstMember.message_text ?? "";

    let firstClient: TelegramClient;
    try {
      firstClient = await getClient(firstAccount);
    } catch (err: any) {
      console.error(`[sniper] Falha ao conectar ${firstAccount.phone_number}: ${err.message}`);
      const results: DispatchResult[] = [{
        account_id: firstAccount.id, message_text: firstMember.message_text,
        status: "failed", retryable: isRetryableError(err.message), error: err.message,
      }];
      await updateScheduleAfterDispatch(schedule, results, now, "closed");
      return;
    }

    // ── Verificar perfil apenas para saber se opens_early ────────────────
    const profile           = groupProfileCache.get(group.id);
    const hasSufficientData = (profile?.sample_count ?? 0) >= 3;
    const isOpensEarly      = hasSufficientData && profile!.opens_early;

    console.log(
      `[sniper][adaptive] opens_early=${isOpensEarly} | ` +
      (hasSufficientData
        ? `p10=${profile!.offset_p10_ms}ms p50=${profile!.offset_p50_ms}ms p90=${profile!.offset_p90_ms}ms ` +
          `openAtStart=${(profile!.open_at_start_ratio * 100).toFixed(0)}% ` +
          `openDelay≈${profile!.estimated_open_delay_ms}ms n=${profile!.sample_count}`
        : `sem perfil suficiente (${profile?.sample_count ?? 0}/3) → loop agressivo imediato`)
    );

    // ── Floor para opens_early: espera até que o invoke CHEGUE ao servidor em scheduledAt ──
    // v15: compensa half-RTT para que o pacote chegue no servidor exatamente no horário.
    //
    // invokeDeadline = scheduledAt (no referencial do servidor) - halfRtt
    // O clock offset (telegramClockOffsetMs) já está em "serverTime - localTime",
    // portanto: scheduledAt_local - clockOffset dá scheduledAt_servidor em tempo local.
    // Subtraindo halfRtt: momento local em que devemos INICIAR o invoke.
    //
    // Se offset = 0 e rtt = 40ms: invokeDeadline = scheduledAt - 20ms
    // Se relógio local adiantado (offset = -50ms) e rtt = 40ms:
    //   scheduledAt_servidor_em_local = scheduledAt - (-50) = scheduledAt + 50ms (mais tarde)
    //   invokeDeadline = scheduledAt + 50ms - 20ms = scheduledAt + 30ms
    //   → espera 30ms a mais antes de invocar, correto!
    if (isOpensEarly) {
      const clockAdj         = clockOffsetQuality === "high" ? telegramClockOffsetMs : 0;
      // scheduledAt no referencial local, mas corrigido para o servidor
      const scheduledAtServer = scheduledAt - clockAdj; // em ms do relógio local
      const invokeDeadline    = scheduledAtServer - estimatedOneWayRttMs;

      const msUntilDeadline = invokeDeadline - Date.now();
      console.log(
        `[sniper][timing] opens_early → clockAdj=${clockAdj > 0 ? "+" : ""}${clockAdj}ms ` +
        `halfRtt=${estimatedOneWayRttMs}ms invokeDeadline=${new Date(invokeDeadline).toISOString()} ` +
        `(${msUntilDeadline > 0 ? "+" : ""}${Math.round(msUntilDeadline)}ms)`
      );

      if (msUntilDeadline > SNIPER_SPIN_MAX_MS) {
        const sleepMs = msUntilDeadline - SNIPER_SPIN_MAX_MS;
        console.log(`[sniper][timing] opens_early → sleep ${sleepMs}ms + spin até invokeDeadline`);
        await new Promise(r => setTimeout(r, sleepMs));
      }
      // busy-spin de precisão até o invokeDeadline
      while (Date.now() < invokeDeadline) { /* spin */ }
      console.log(`[sniper][timing] opens_early → invokeDeadline atingido, iniciando loop`);
    } else {
      const lagVsScheduled = Date.now() - scheduledAt;
      console.log(
        `[sniper][timing] invoke imediato | ` +
        `${lagVsScheduled >= 0 ? "+" : ""}${lagVsScheduled}ms vs scheduledAt`
      );
    }

    sniperTokenBuckets.set(firstAccount.id, {
      tokens:     SNIPER_MAX_REQ_PER_SECOND_PER_ACCOUNT,
      lastRefill: Date.now(),
    });

    // ── Fase 1: loop agressivo na primeira conta ──────────────────────────
    const results: DispatchResult[] = [];
    let attempt        = 0;
    let tooEarlyCount  = 0;
    let firstSentAt: Date | null = null;
    const firstRandomId = makeRandomId();

    while (Date.now() < budgetEnd) {
      attempt++;

      // Rate limit shield
      const waitForToken = acquireTokenBucket(firstAccount.id);
      if (waitForToken > 0) {
        await new Promise(r => setTimeout(r, waitForToken));
      }

      const result = await sniperAttemptOnce(firstClient, firstAccount, chatId, firstText, firstRandomId);

      if (result.outcome === "sent") {
        firstSentAt = new Date(result.sentAt!);

        const invokeRttMs = firstSentAt.getTime() - sniperEnteredAt;
        const vsHorarioMs = firstSentAt.getTime() - scheduledAtRaw;

        console.log(`[sniper][timing] invoke RTT total: ${invokeRttMs}ms`);
        console.log(
          `[sniper][timing] vs horário: ${vsHorarioMs > 0 ? "+" : ""}${vsHorarioMs}ms ` +
          `tentativa=${attempt} tooEarly=${tooEarlyCount} opens_early=${isOpensEarly} ` +
          `(clockOffset_diag=${telegramClockOffsetMs}ms qual=${clockOffsetQuality})`
        );
        console.log(`[sniper] ✓ ${firstAccount.phone_number} enviou na tentativa ${attempt} (${firstSentAt.toISOString()})`);

        results.push({
          account_id: firstAccount.id, message_text: firstMember.message_text,
          status: "sent", retryable: false,
        });

        persistGroupDispatchSample(group.id, vsHorarioMs, tooEarlyCount).catch(e =>
          console.warn("[profiles] Erro ao persistir amostra:", e.message)
        );

        break;
      }

      if (result.outcome === "too_early") {
        tooEarlyCount++;
        const intervalMs = getTooEarlyIntervalMs(tooEarlyCount);
        if (tooEarlyCount % 200 === 0) {
          console.log(`[sniper] ⏳ too_early×${tooEarlyCount} — intervalo atual: ${intervalMs}ms`);
        }
        await new Promise(r => setTimeout(r, intervalMs));
        continue;
      }

      if (result.outcome === "fatal") {
        console.error(`[sniper] Erro fatal na conta ${firstAccount.phone_number}: ${result.errorCode}`);
        results.push({
          account_id: firstAccount.id, message_text: firstMember.message_text,
          status: "failed", retryable: false, error: result.errorCode,
        });
        await updateScheduleAfterDispatch(schedule, results, now, "closed");
        return;
      }

      if (result.outcome === "flood") {
        const waitMs = (result.floodWaitSecs ?? 5) * 1000;
        console.warn(`[sniper] FloodWait ${result.floodWaitSecs}s`);
        if (waitMs < budgetEnd - Date.now() - 500) {
          await new Promise(r => setTimeout(r, waitMs));
          continue;
        } else {
          console.warn(`[sniper] FloodWait excede budget — encerrando loop`);
          break;
        }
      }

      // transient: 1ms base
      if (attempt % SNIPER_PAUSE_EVERY_N === 0) {
        await new Promise(r => setTimeout(r, SNIPER_PAUSE_MS));
      } else {
        await new Promise(r => setTimeout(r, SNIPER_TRANSIENT_INTERVAL_MS));
      }
    }

    // Log da primeira conta
    supabase.from("dispatch_logs").insert({
      user_id: schedule.user_id, group_id: group.id,
      account_id: firstAccount.id, schedule_id: schedule.id,
      status: firstSentAt ? "sent" : "failed",
      message_text: firstMember.message_text, position_rank: 1,
      group_name_snapshot: group.name, chat_name_snapshot: group.telegram_chat_name,
      sent_at: firstSentAt ? firstSentAt.toISOString() : null,
      error_message: firstSentAt ? null : `BUDGET_EXCEEDED após ${attempt} tentativas`,
    }).then(({ error: e }) => {
      if (e) console.error(`[sniper][log] Falha ao inserir log para ${firstAccount.id}:`, e.message);
    });

    if (!firstSentAt) {
      console.warn(`[sniper] Budget esgotado para schedule ${scheduleId} após ${attempt} tentativas`);
      results.push({
        account_id: firstAccount.id, message_text: firstMember.message_text,
        status: "failed", retryable: true,
        error: `SNIPER_BUDGET_EXCEEDED após ${attempt} tentativas`,
      });
      await updateScheduleAfterDispatch(schedule, results, now, "closed");
      return;
    }

    // ── Fase 2: demais contas em sequência ───────────────────────────────
    for (let i = 1; i < members.length; i++) {
      await new Promise(r => setTimeout(r, SNIPER_INTER_ACCOUNT_DELAY_MS));

      const member  = members[i];
      const account = member.accounts!;
      const text    = member.message_text ?? "";
      let   sentAt: Date | null = null;
      let   error: string | undefined;

      try {
        const client         = await getClient(account);
        const memberRandomId = makeRandomId();

        sniperTokenBuckets.set(account.id, {
          tokens:     SNIPER_MAX_REQ_PER_SECOND_PER_ACCOUNT,
          lastRefill: Date.now(),
        });

        const waitForToken = acquireTokenBucket(account.id);
        if (waitForToken > 0) await new Promise(r => setTimeout(r, waitForToken));

        const res = await sniperAttemptOnce(client, account, chatId, text, memberRandomId);
        if (res.outcome === "sent") {
          sentAt = new Date(res.sentAt!);
          console.log(`[sniper] ✓ Conta ${i + 1}/${members.length} ${account.phone_number} enviou`);
          results.push({ account_id: account.id, message_text: member.message_text, status: "sent", retryable: false });
        } else {
          throw new Error(res.errorCode ?? res.outcome);
        }
      } catch (err: any) {
        error = String(err?.message ?? "");
        console.error(`[sniper] ✗ Conta ${i + 1}/${members.length} ${account.phone_number}: ${error}`);
        results.push({ account_id: account.id, message_text: member.message_text, status: "failed", retryable: isRetryableError(error), error });
      }

      supabase.from("dispatch_logs").insert({
        user_id: schedule.user_id, group_id: group.id,
        account_id: account.id, schedule_id: schedule.id,
        status: sentAt ? "sent" : "failed", message_text: member.message_text,
        position_rank: i + 1, group_name_snapshot: group.name,
        chat_name_snapshot: group.telegram_chat_name,
        sent_at: sentAt ? sentAt.toISOString() : null, error_message: error ?? null,
      }).then(({ error: e }) => {
        if (e) console.error(`[sniper][log] Falha ao inserir log para ${account.id}:`, e.message);
      });
    }

    // ── Fase 3: monitoramento de posições ────────────────────────────────
    const sentForMonitor = results
      .filter(r => r.status === "sent")
      .map(r => ({ account_id: r.account_id, message_text: r.message_text ?? "" }))
      .filter(r => r.message_text);

    if (sentForMonitor.length > 0) {
      monitorPositions(chatId, sentForMonitor, scheduleId, firstSentAt, "closed")
        .catch(err => console.error("[sniper][monitor] Erro:", err.message));
    }

    await updateScheduleAfterDispatch(schedule, results, firstSentAt, "closed");

  } finally {
    for (const m of (schedulePrefetchCache.get(scheduleId)?.groups?.group_members ?? [])) {
      if (m.accounts?.id) sniperTokenBuckets.delete(m.accounts.id);
    }
    sniperFiringNow.delete(scheduleId);
    firingNow.add(scheduleId);
    setTimeout(() => firingNow.delete(scheduleId), SNIPER_DONE_BLOCK_TTL_MS);
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   DEDUPLICAÇÃO
   ───────────────────────────────────────────────────────────────────────────── */
async function getAlreadySentIds(schedule: Schedule): Promise<Set<string>> {
  const cycleStart = schedule.retry_until
    ? new Date(new Date(schedule.retry_until).getTime() - schedule.retry_window_seconds * 1000).toISOString()
    : schedule.next_run_at;

  const { data, error } = await supabase
    .from("dispatch_logs").select("account_id")
    .eq("schedule_id", schedule.id).eq("status", "sent").gte("sent_at", cycleStart);

  if (error) {
    console.warn(`[dedup] Falha ao buscar enviados do schedule ${schedule.id}:`, error.message);
    return new Set();
  }
  return new Set((data ?? []).map(r => r.account_id as string));
}

/* ─────────────────────────────────────────────────────────────────────────────
   MONITORAMENTO DE POSIÇÃO
   ───────────────────────────────────────────────────────────────────────────── */
async function monitorPositions(
  telegramChatId: string,
  sentMembers: Array<{ account_id: string; message_text: string }>,
  scheduleId: string,
  dispatchedAt: Date,
  groupType: "open" | "closed"
): Promise<void> {
  if (sentMembers.length === 0) return;

  const account = accountCache.get(sentMembers[0].account_id);
  if (!account) return;

  const client = await getClient(account).catch(() => null);
  if (!client) return;

  const windowStartUnix = Math.floor((dispatchedAt.getTime() - 15_000) / 1000);
  const deadline        = Date.now() + (groupType === "closed"
    ? MONITOR_DELAY_CLOSED_MS + 10_000
    : MONITOR_MAX_OPEN_MS);
  const ourTexts = new Set(sentMembers.map(m => m.message_text).filter(Boolean));

  if (groupType === "closed") await new Promise(r => setTimeout(r, MONITOR_DELAY_CLOSED_MS));

  while (Date.now() < deadline) {
    try {
      const peer   = await resolvePeer(client, telegramChatId, account.id);
      const result = await client.invoke(
        new Api.messages.GetHistory({
          peer: peer as any, limit: MONITOR_HISTORY_LIMIT,
          offsetDate: 0, offsetId: 0, maxId: 0, minId: 0, hash: bigInt(0), addOffset: 0,
        })
      ) as any;

      const windowMsgs = (result.messages ?? [])
        .filter((m: any) => m._ === "message" && m.date >= windowStartUnix)
        .reverse();

      if (windowMsgs.length === 0) {
        if (groupType === "closed") return;
        await new Promise(r => setTimeout(r, MONITOR_POLL_MS));
        continue;
      }

      if (groupType === "open" && !windowMsgs.some((m: any) => ourTexts.has(m.message))) {
        await new Promise(r => setTimeout(r, MONITOR_POLL_MS));
        continue;
      }

      const cutoff = new Date(dispatchedAt.getTime() - 60_000).toISOString();
      await Promise.allSettled(sentMembers.map(sm => {
        if (!sm.message_text) return;
        const idx = windowMsgs.findIndex((m: any) => m.message === sm.message_text);
        if (idx < 0) return;
        const rank = idx + 1;
        return supabase.from("dispatch_logs")
          .update({ position_rank: rank })
          .eq("schedule_id", scheduleId).eq("account_id", sm.account_id)
          .eq("status", "sent").gte("sent_at", cutoff);
      }));

      console.log(`[monitor] ✓ Posições salvas para schedule ${scheduleId}`);
      return;
    } catch (err: any) {
      if (groupType === "closed") return;
      await new Promise(r => setTimeout(r, MONITOR_POLL_MS));
    }
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   LISTENER DE GRUPO ABERTO
   ───────────────────────────────────────────────────────────────────────────── */
function startGroupListener(schedule: Schedule, group: Group, account: Account): void {
  const existing = listenMap.get(group.id);
  if (existing) existing.abort();

  const ctrl = new AbortController();
  listenMap.set(group.id, ctrl);

  const deadline    = Date.now() + OPEN_GROUP_LISTEN_TIMEOUT_MS;
  const startUnix   = Math.floor((Date.now() - 10_000) / 1000);
  let lastSeenMsgId = 0;

  console.log(`[listen] 👂 Aguardando sinal do admin em ${group.telegram_chat_id} para schedule ${schedule.id}`);

  (async () => {
    try {
      let client = await getClient(account).catch(() => null);
      if (!client) { listenMap.delete(group.id); return; }

      try { await resolvePeer(client, group.telegram_chat_id!, account.id); } catch {}

      while (Date.now() < deadline && !ctrl.signal.aborted) {
        try {
          if (!client.connected) {
            client = await getClient(account);
            try { await resolvePeer(client, group.telegram_chat_id!, account.id); } catch {}
          }

          const peer   = await resolvePeer(client, group.telegram_chat_id!, account.id);
          const result = await client.invoke(
            new Api.messages.GetHistory({
              peer: peer as any, limit: 10,
              offsetDate: 0, offsetId: 0, maxId: 0, minId: 0, hash: bigInt(0), addOffset: 0,
            })
          ) as any;

          const recentMsgs = (result.messages ?? []).filter(
            (m: any) => (m.className === "Message" || m._ === "message") && m.date >= startUnix && m.id > lastSeenMsgId
          );
          if (recentMsgs.length > 0) {
            lastSeenMsgId = Math.max(lastSeenMsgId, ...recentMsgs.map((m: any) => m.id as number));
          }

          const gotSignal = recentMsgs.some((m: any) => {
            const isOk    = typeof m.message === "string" && m.message.trim().toLowerCase() === "ok";
            const isMedia = m.media != null && m.media.className !== "MessageMediaEmpty";
            return isOk || isMedia;
          });

          if (gotSignal && !ctrl.signal.aborted) {
            listenMap.delete(group.id);
            const dispatchedAt = new Date();
            const alreadySent  = await getAlreadySentIds(schedule);
            const results      = await dispatchToGroup(schedule, group, alreadySent);

            const sentForMonitor = results
              .filter(r => r.status === "sent")
              .map(r => ({ account_id: r.account_id, message_text: r.message_text ?? "" }))
              .filter(r => r.message_text);
            if (sentForMonitor.length > 0) {
              monitorPositions(group.telegram_chat_id!, sentForMonitor, schedule.id, dispatchedAt, "open")
                .catch(err => console.error("[listen] Erro no monitoramento:", err.message));
            }
            await updateScheduleAfterDispatch(schedule, results, dispatchedAt, "open");
            return;
          }
        } catch (err: any) {
          if (!ctrl.signal.aborted) await new Promise(r => setTimeout(r, 2_000));
        }
        if (!ctrl.signal.aborted) await new Promise(r => setTimeout(r, LISTEN_POLL_MS));
      }

      listenMap.delete(group.id);
      if (ctrl.signal.aborted) return;

      const nowISO = new Date().toISOString();
      let nextRun: string;
      try { nextRun = nextWeeklyOccurrence(schedule.cron_expression); }
      catch {
        await supabase.from("schedules").update({ is_active: false }).eq("id", schedule.id);
        return;
      }
      await supabase.from("schedules").update({
        next_run_at: nextRun, retry_until: null, retry_count: 0,
        last_attempt_at: nowISO, last_attempt_status: "timeout",
        last_attempt_error: "Timeout aguardando sinal do admin",
      }).eq("id", schedule.id);
      scheduleTimer(schedule.id, nextRun, "open", schedule.group_id);
    } catch (err: any) {
      console.error(`[listen] Erro inesperado para schedule ${schedule.id}:`, err.message);
      listenMap.delete(group.id);
    }
  })();
}

/* ─────────────────────────────────────────────────────────────────────────────
   DESPACHO PARA O GRUPO
   ───────────────────────────────────────────────────────────────────────────── */
async function dispatchToGroup(
  schedule: Schedule,
  group: Group,
  alreadySent: Set<string>
): Promise<DispatchResult[]> {
  const members = (group.group_members ?? [])
    .filter(m => m.is_active && m.accounts?.is_active && m.accounts?.session_string)
    .sort((a, b) => a.position - b.position);

  return Promise.all(members.map(async (member, i) => {
    const account      = member.accounts!;
    const positionRank = i + 1;

    if (alreadySent.has(account.id)) {
      return { account_id: account.id, message_text: member.message_text, status: "skipped" as const, retryable: false };
    }

    let status: "sent" | "failed" = "failed";
    let error: string | undefined;
    let retryable = false;

    try {
      const client = await getClient(account);
      await sendMessage(client, account, group.telegram_chat_id!, member.message_text ?? "");
      status = "sent";
      alreadySent.add(account.id);
    } catch (err) {
      error     = err instanceof Error ? err.message : String(err);
      retryable = isRetryableError(error);
    }

    supabase.from("dispatch_logs").insert({
      user_id: schedule.user_id, group_id: group.id,
      account_id: account.id, schedule_id: schedule.id,
      status, message_text: member.message_text, position_rank: positionRank,
      group_name_snapshot: group.name, chat_name_snapshot: group.telegram_chat_name,
      sent_at: status === "sent" ? new Date().toISOString() : null, error_message: error ?? null,
    }).then(({ error: e }) => {
      if (e) console.error(`[log] Falha ao inserir dispatch_log para ${account.id}:`, e.message);
    });

    return { account_id: account.id, message_text: member.message_text, status, retryable, error };
  }));
}

/* ─────────────────────────────────────────────────────────────────────────────
   ATUALIZAÇÃO DO SCHEDULE APÓS DISPARO
   ───────────────────────────────────────────────────────────────────────────── */
async function updateScheduleAfterDispatch(
  schedule: Schedule,
  results: DispatchResult[],
  now: Date,
  groupType?: "open" | "closed"
): Promise<void> {
  const nowISO = now.toISOString();

  const sentCount      = results.filter(r => r.status === "sent").length;
  const skippedCount   = results.filter(r => r.status === "skipped").length;
  const retryableFails = results.filter(r => r.status === "failed" && r.retryable);
  const permanentFails = results.filter(r => r.status === "failed" && !r.retryable);

  const hasActiveMembers = results.length > 0;
  const allOk =
    hasActiveMembers &&
    retryableFails.length === 0 &&
    permanentFails.length === 0 &&
    (sentCount + skippedCount) > 0;

  const resolvedGroupType = groupType ?? schedule.groups?.group_type;
  const groupId           = schedule.group_id ?? schedule.groups?.id;
  if (groupId) scheduleGroupMap.set(schedule.id, groupId);

  if (allOk) {
    let nextRun: string;
    try {
      nextRun = nextWeeklyOccurrence(schedule.cron_expression);
    } catch (err) {
      await supabase.from("schedules").update({ is_active: false }).eq("id", schedule.id);
      return;
    }

    supabase.from("schedules").update({
      next_run_at: nextRun, last_run_at: nowISO, retry_until: null, retry_count: 0,
      last_attempt_at: nowISO, last_attempt_status: "sent", last_attempt_error: null,
    }).eq("id", schedule.id).then(({ error: e }) => {
      if (e) console.error(`[schedule] Falha ao atualizar ${schedule.id}:`, e.message);
    });

    scheduleTimer(schedule.id, nextRun, resolvedGroupType, groupId);
  } else {
    const newRetryCount = schedule.retry_count + 1;
    const retryUntil    = schedule.retry_until ??
      new Date(now.getTime() + schedule.retry_window_seconds * 1000).toISOString();
    const interval      = calcRetryInterval(
      newRetryCount, schedule.retry_interval_seconds, schedule.retry_interval_max_seconds
    );
    const failErrors = results.filter(r => r.error).map(r => `[${r.account_id}] ${r.error}`).join("; ");

    await supabase.from("schedules").update({
      retry_until: retryUntil, retry_count: newRetryCount,
      last_attempt_at: nowISO, last_attempt_status: "retrying",
      last_attempt_error: failErrors || null,
    }).eq("id", schedule.id);

    const retryAt = new Date(now.getTime() + interval * 1000);
    if (retryAt < new Date(retryUntil)) {
      scheduleTimer(schedule.id, retryAt.toISOString(), resolvedGroupType, groupId);
    }
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   DISPARO DE SCHEDULE (grupos abertos + fallback fechados)
   ───────────────────────────────────────────────────────────────────────────── */
async function fireSchedule(scheduleId: string): Promise<void> {
  if (sniperFiringNow.has(scheduleId)) return;
  if (firingNow.has(scheduleId)) return;
  firingNow.add(scheduleId);

  try {
    const now = new Date();

    let schedule = schedulePrefetchCache.get(scheduleId);
    if (schedule) {
      schedulePrefetchCache.delete(scheduleId);
    } else {
      const { data, error } = await supabase
        .from("schedules").select(SCHEDULE_SELECT)
        .eq("id", scheduleId).eq("is_active", true).single();
      if (error || !data) return;
      schedule = data as unknown as Schedule;
    }

    const group = schedule.groups;
    if (!group?.telegram_chat_id) return;

    if (group.group_members) {
      group.group_members = group.group_members.map(m => ({
        ...m,
        accounts: m.accounts ? (accountCache.get(m.accounts.id) ?? m.accounts) : null,
      }));
    }

    if (group.group_type === "open") {
      if (listenMap.has(group.id)) return;

      const firstAccount = (group.group_members ?? [])
        .filter(m => m.is_active && m.accounts?.is_active)
        .sort((a, b) => a.position - b.position)[0]?.accounts ?? null;

      if (!firstAccount) return;

      startGroupListener(schedule, group, firstAccount as Account);

      await supabase.from("schedules").update({
        retry_until: new Date(now.getTime() + OPEN_GROUP_LISTEN_TIMEOUT_MS).toISOString(),
        last_attempt_at: now.toISOString(),
        last_attempt_status: "waiting_admin", last_attempt_error: null,
      }).eq("id", scheduleId);
      return;
    }

    const alreadySent = schedule.retry_until ? await getAlreadySentIds(schedule) : new Set<string>();
    const results     = await dispatchToGroup(schedule, group, alreadySent);

    const sentForMonitor = results
      .filter(r => r.status === "sent")
      .map(r => ({ account_id: r.account_id, message_text: r.message_text ?? "" }))
      .filter(r => r.message_text);
    if (sentForMonitor.length > 0) {
      monitorPositions(group.telegram_chat_id, sentForMonitor, scheduleId, now, group.group_type ?? "closed")
        .catch(err => console.error("[monitor] Erro:", err.message));
    }

    await updateScheduleAfterDispatch(schedule, results, now, group.group_type);
  } finally {
    firingNow.delete(scheduleId);
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   TIMER DE PRECISÃO + PRE-FETCH + SNIPER
   ─────────────────────────────────────────────────────────────────────────────
   scheduleTimer acorda o sniper SNIPER_BEFORE_MS antes do horário apenas para
   ter a conexão quente. O invoke começa imediatamente (exceto opens_early).
   Para opens_early, acorda mais cedo com extraLeadMs para garantir que o
   busy-wait até scheduledAt comece com folga.
   ───────────────────────────────────────────────────────────────────────────── */
function scheduleTimer(
  scheduleId: string,
  nextRunAt: string,
  groupType?: "open" | "closed",
  groupId?: string
): void {
  const delay = new Date(nextRunAt).getTime() - Date.now();

  if (delay < -5_000) {
    console.warn(`[timer] Schedule ${scheduleId} ignorado — muito no passado (${nextRunAt})`);
    return;
  }

  if (groupId) scheduleGroupMap.set(scheduleId, groupId);

  const prev = scheduledTimers.get(scheduleId);
  if (prev) clearTimeout(prev);
  const prevPrefetch = prefetchTimers.get(scheduleId);
  if (prevPrefetch) { clearTimeout(prevPrefetch); prefetchTimers.delete(scheduleId); }
  const prevSniper = sniperTimers.get(scheduleId);
  if (prevSniper) { clearTimeout(prevSniper); sniperTimers.delete(scheduleId); }

  const effectiveDelay = Math.max(0, delay);

  if (effectiveDelay > PREFETCH_BEFORE_MS) {
    const prefetchDelay = effectiveDelay - PREFETCH_BEFORE_MS;
    const pt = setTimeout(async () => {
      prefetchTimers.delete(scheduleId);
      try {
        const { data, error } = await supabase
          .from("schedules").select(SCHEDULE_SELECT)
          .eq("id", scheduleId).eq("is_active", true).single();
        if (error || !data) return;

        const s = data as unknown as Schedule;
        if (s.groups?.group_members) {
          s.groups.group_members = s.groups.group_members.map(m => ({
            ...m,
            accounts: m.accounts ? (accountCache.get(m.accounts.id) ?? m.accounts) : null,
          }));
        }
        schedulePrefetchCache.set(scheduleId, s);
      } catch (err: any) {
        console.warn(`[prefetch] Falha ao pré-carregar ${scheduleId}: ${err.message}`);
      }
    }, prefetchDelay);
    prefetchTimers.set(scheduleId, pt);
  }

  if (groupType === "closed") {
    const resolvedGroupId   = groupId ?? scheduleGroupMap.get(scheduleId);
    const profile           = resolvedGroupId ? groupProfileCache.get(resolvedGroupId) : undefined;
    const hasSufficientData = (profile?.sample_count ?? 0) >= 3;
    const isOpensEarly      = hasSufficientData && profile!.opens_early;

    // Para opens_early: acorda mais cedo para ter margem no busy-wait até scheduledAt
    // Para demais: SNIPER_BEFORE_MS é suficiente (invoke é imediato)
    const extraLeadMs       = isOpensEarly ? Math.max(0, -profile!.offset_p10_ms) + 100 : 0;
    const totalSniperLeadMs = SNIPER_BEFORE_MS + extraLeadMs;

    if (effectiveDelay > totalSniperLeadMs) {
      const sniperDelay = effectiveDelay - totalSniperLeadMs;
      const st = setTimeout(async () => {
        sniperTimers.delete(scheduleId);
        const cached = schedulePrefetchCache.get(scheduleId);
        if (cached && cached.groups?.group_type !== "closed") return;
        try {
          await sniperFireClosed(scheduleId);
        } catch (err) {
          console.error(`[sniper] Erro inesperado ao disparar ${scheduleId}:`, err);
        }
      }, sniperDelay);
      sniperTimers.set(scheduleId, st);

      if (isOpensEarly) {
        console.log(`[sniper] ⏰ Opens_early — lead total=${totalSniperLeadMs}ms, acorda em ${Math.round(sniperDelay / 1000)}s`);
      } else {
        console.log(`[sniper] ⏰ Agendado para schedule ${scheduleId} em ${Math.round(sniperDelay / 1000)}s`);
      }
    }
  }

  const timer = setTimeout(async () => {
    scheduledTimers.delete(scheduleId);
    try { await fireSchedule(scheduleId); }
    catch (err) { console.error(`[timer] Erro inesperado ao disparar ${scheduleId}:`, err); }
  }, effectiveDelay);

  scheduledTimers.set(scheduleId, timer);
  console.log(`[timer] ⏰ Schedule ${scheduleId} — dispara em ${Math.round(effectiveDelay / 1000)}s`);
}

/* ─────────────────────────────────────────────────────────────────────────────
   RELOAD PERIÓDICO
   ───────────────────────────────────────────────────────────────────────────── */
async function reloadSchedules(): Promise<void> {
  const now          = new Date();
  const nowISO       = now.toISOString();
  const lookaheadISO = new Date(now.getTime() + LOOKAHEAD_MS).toISOString();

  const [
    { data: futureSchedules },
    { data: retrySchedules },
    { data: expiredRetries },
  ] = await Promise.all([
    supabase.from("schedules")
      .select("id, next_run_at, group_id, groups(id, group_type)")
      .eq("is_active", true).is("retry_until", null).lte("next_run_at", lookaheadISO),

    supabase.from("schedules")
      .select(SCHEDULE_SELECT)
      .eq("is_active", true).not("retry_until", "is", null).gt("retry_until", nowISO),

    supabase.from("schedules")
      .select("id, cron_expression, group_id")
      .eq("is_active", true).not("retry_until", "is", null).lte("retry_until", nowISO),
  ]);

  await Promise.all((expiredRetries ?? []).map(async expired => {
    if (!expired || typeof expired !== "object" || "message" in expired) return;
    const expGroupId = (expired as any).group_id as string | undefined;
    if (expGroupId) {
      const ctrl = listenMap.get(expGroupId);
      if (ctrl) { ctrl.abort(); listenMap.delete(expGroupId); }
    }

    let nextRun: string;
    try { nextRun = nextWeeklyOccurrence(expired.cron_expression); }
    catch {
      await supabase.from("schedules").update({ is_active: false }).eq("id", expired.id);
      return;
    }

    await supabase.from("schedules").update({
      next_run_at: nextRun, last_run_at: nowISO, retry_until: null, retry_count: 0,
      last_attempt_at: nowISO, last_attempt_status: "failed",
      last_attempt_error: "Retry expirou sem sucesso total",
    }).eq("id", expired.id);
    scheduleTimer(expired.id, nextRun, undefined, expGroupId);
  }));

  for (const s of futureSchedules ?? []) {
    if (!scheduledTimers.has(s.id)) {
      const gType = (s as any).groups?.group_type as "open" | "closed" | undefined;
      const gId   = (s as any).group_id as string | undefined ?? (s as any).groups?.id as string | undefined;
      scheduleTimer(s.id, s.next_run_at, gType, gId);
    }
  }

  for (const s of retrySchedules ?? []) {
    const schedule = s as unknown as Schedule;
    if (listenMap.has(schedule.group_id)) continue;
    if (
      isRetryDue(schedule, now) &&
      !scheduledTimers.has(schedule.id) &&
      !firingNow.has(schedule.id) &&
      !sniperFiringNow.has(schedule.id)
    ) {
      fireSchedule(schedule.id).catch(err =>
        console.error(`[reload] Erro no retry do schedule ${schedule.id}:`, err)
      );
    }
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   RECONEXÃO LEVE
   ───────────────────────────────────────────────────────────────────────────── */
async function reconnectDeadClients(): Promise<void> {
  const reconnectPromises: Promise<void>[] = [];

  for (const [accountId, client] of clients.entries()) {
    if (!client.connected) {
      const account = accountCache.get(accountId);
      if (!account) continue;
      reconnectPromises.push(
        getClient(account)
          .then(() => console.log(`[reconnect] ✓ ${account.phone_number} reconectado`))
          .catch(err => console.warn(`[reconnect] Falha: ${err.message}`))
      );
    }
  }

  if (reconnectPromises.length > 0) await Promise.allSettled(reconnectPromises);
}

/* ─────────────────────────────────────────────────────────────────────────────
   PRE-WARM DE CONTAS + PERFIS + CLOCK
   ───────────────────────────────────────────────────────────────────────────── */
let prewarmRunning = false;
async function prewarmAccounts(): Promise<void> {
  if (prewarmRunning) return;
  prewarmRunning = true;
  try {
    const { data, error } = await supabase
      .from("accounts").select("id, name, phone_number, api_id, api_hash, session_string, is_active")
      .eq("is_active", true);

    if (error) { console.warn("[prewarm] Falha ao buscar contas:", error.message); return; }

    const accounts = (data ?? []) as Account[];
    for (const account of accounts) accountCache.set(account.id, account);

    await Promise.allSettled(accounts.map(async account => {
      try {
        const client = await getClient(account);
        await client.getDialogs({ limit: 100 });
        console.log(`[prewarm] ✓ ${account.phone_number}`);
      } catch (err: any) {
        const authDead =
          err.message?.includes("AUTH_KEY_UNREGISTERED") ||
          err.message?.includes("USER_DEACTIVATED")      ||
          err.message?.includes("SESSION_REVOKED");
        if (authDead) {
          await supabase.from("accounts").update({ is_active: false }).eq("id", account.id);
        }
      }
    }));

    try {
      const { data: groups } = await supabase
        .from("groups").select("telegram_chat_id, group_members(accounts(id))")
        .not("telegram_chat_id", "is", null).eq("group_members.is_active", true);

      const resolvePromises: Promise<unknown>[] = [];
      for (const group of groups ?? []) {
        if (!group.telegram_chat_id) continue;
        const chatId = String(group.telegram_chat_id);
        for (const member of (group as any).group_members ?? []) {
          const accountId = member?.accounts?.id ?? member?.accounts?.[0]?.id;
          if (!accountId) continue;
          const acc = accountCache.get(accountId);
          if (!acc) continue;
          const cl = clients.get(acc.id);
          if (!cl?.connected) continue;
          resolvePromises.push(resolvePeer(cl, chatId, acc.id).catch(() => {}));
        }
      }
      await Promise.allSettled(resolvePromises);
      console.log(`[prewarm] ✓ Pre-resolve de peers concluído (${resolvePromises.length} entradas)`);
    } catch (err: any) {
      console.warn(`[prewarm] Falha no pre-resolve: ${err.message}`);
    }

    await loadGroupProfiles();

    const anyClient = [...clients.values()].find(c => c.connected);
    if (anyClient) {
      telegramClockOffsetMs = await measureTelegramClockOffset(anyClient);
      console.log(`[clock] ✅ Clock offset inicial: ${telegramClockOffsetMs > 0 ? "+" : ""}${telegramClockOffsetMs}ms (${clockOffsetQuality})`);
    }

  } finally {
    prewarmRunning = false;
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   HTTP SERVER
   ───────────────────────────────────────────────────────────────────────────── */
function jsonResponse(res: http.ServerResponse, status: number, body: unknown) {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(body));
}

const httpServer = http.createServer(async (req, res) => {
  if (WORKER_SECRET && req.headers["x-worker-secret"] !== WORKER_SECRET) {
    return jsonResponse(res, 401, { error: "Unauthorized" });
  }

  const url = new URL(req.url ?? "/", `http://localhost:${WORKER_PORT}`);

  const chatsMatch = url.pathname.match(/^\/accounts\/([^/]+)\/chats$/);
  if (req.method === "GET" && chatsMatch) {
    const account = accountCache.get(chatsMatch[1]);
    if (!account) return jsonResponse(res, 404, { error: "Conta não encontrada no cache" });
    try {
      const client  = await getClient(account);
      const dialogs = await client.getDialogs({ limit: 200 });
      const chats   = dialogs
        .filter(d => d.isGroup || d.isChannel)
        .map(d => ({ id: String(d.id), name: d.title ?? d.name ?? "Sem nome", type: d.isChannel ? "channel" : "group", accessHash: null }))
        .sort((a, b) => a.name.localeCompare(b.name));
      return jsonResponse(res, 200, chats);
    } catch (err: any) {
      return jsonResponse(res, 500, { error: err.message });
    }
  }

  const chatCountMatch = url.pathname.match(/^\/accounts\/([^/]+)\/chat-count$/);
  if (req.method === "GET" && chatCountMatch) {
    const chatId  = url.searchParams.get("chat_id");
    const account = accountCache.get(chatCountMatch[1]);
    if (!chatId)  return jsonResponse(res, 400, { error: "chat_id é obrigatório" });
    if (!account) return jsonResponse(res, 404, { error: "Conta não encontrada no cache" });

    try {
      const client = await getClient(account);
      const rawId  = chatId.replace(/^-100/, "").replace(/^-/, "");
      let count: number | null = null;

      try {
        const result = await client.invoke(
          new Api.channels.GetFullChannel({ channel: new Api.InputChannel({ channelId: bigInt(rawId), accessHash: bigInt(0) }) })
        ) as any;
        if (typeof result?.fullChat?.participantsCount === "number") count = result.fullChat.participantsCount;
      } catch {}

      if (count === null) {
        try {
          const dialogs = await client.getDialogs({ limit: 500 });
          const absRaw  = rawId.replace(/^100/, "");
          const dialog  = dialogs.find(d => {
            const s = String(d.id).replace(/^-/, "");
            return s === rawId || s === absRaw || String(d.id) === chatId || `-100${s}` === chatId || `-${s}` === chatId;
          });
          if (dialog?.entity) {
            const ent = dialog.entity as any;
            count = typeof ent.participantsCount === "number" ? ent.participantsCount : null;
          }
        } catch {}
      }

      return jsonResponse(res, 200, { count });
    } catch (err: any) {
      return jsonResponse(res, 500, { error: err.message });
    }
  }

  const membersMatch = url.pathname.match(/^\/accounts\/([^/]+)\/chat-members$/);
  if (req.method === "GET" && membersMatch) {
    const chatId  = url.searchParams.get("chat_id");
    const account = accountCache.get(membersMatch[1]);
    if (!chatId)  return jsonResponse(res, 400, { error: "chat_id é obrigatório" });
    if (!account) return jsonResponse(res, 404, { error: "Conta não encontrada no cache" });

    type MemberOut = { id: string; name: string | null; username: string | null; phone: string | null };

    try {
      const client       = await getClient(account);
      const rawId        = chatId.replace(/^-/, "");
      const isSupergroup = chatId.startsWith("-100");
      let members: MemberOut[] = [];

      if (isSupergroup) {
        try {
          const dialogs = await client.getDialogs({ limit: 500 });
          const dialog  = dialogs.find(d => {
            const dId = String(d.id);
            return dId === rawId || dId === chatId || dId === rawId.replace(/^100/, "");
          });
          const entity = dialog?.entity;
          if (entity && (entity.className === "Channel" || entity.className === "Chat")) {
            const result = await client.invoke(
              new Api.channels.GetParticipants({
                channel: entity as Api.Channel,
                filter: new Api.ChannelParticipantsRecent(),
                offset: 0, limit: 200, hash: bigInt(0),
              })
            );
            if (result.className === "channels.ChannelParticipants") {
              members = result.users
                .filter((u): u is Api.User => u.className === "User" && !u.bot)
                .map(u => ({
                  id: String(u.id),
                  name: [u.firstName, u.lastName].filter(Boolean).join(" ") || null,
                  username: u.username ? `@${u.username}` : null,
                  phone: u.phone ? `+${u.phone}` : null,
                }));
            }
          }
        } catch {}
      }

      if (members.length === 0) {
        try {
          const full     = await client.invoke(new Api.messages.GetFullChat({ chatId: bigInt(rawId) }));
          const chatFull = full.fullChat as Api.ChatFull;
          const parts    = chatFull.participants;
          if (parts && parts.className === "ChatParticipants") {
            const userMap = new Map<string, Api.User>();
            for (const u of full.users) {
              if (u.className === "User") userMap.set(String(u.id), u as Api.User);
            }
            members = parts.participants
              .map(p => {
                const u = userMap.get(String((p as any).userId));
                if (!u || u.bot) return null;
                return {
                  id: String(u.id),
                  name: [u.firstName, u.lastName].filter(Boolean).join(" ") || null,
                  username: u.username ? `@${u.username}` : null,
                  phone: u.phone ? `+${u.phone}` : null,
                };
              })
              .filter((m): m is MemberOut => m !== null);
          }
        } catch {}
      }

      members.sort((a, b) => (a.name ?? "").localeCompare(b.name ?? ""));
      return jsonResponse(res, 200, members);
    } catch (err: any) {
      return jsonResponse(res, 500, { error: err.message });
    }
  }

  const reloadMatch = url.pathname.match(/^\/accounts\/([^/]+)\/reload$/);
  if (req.method === "POST" && reloadMatch) {
    const accountId = reloadMatch[1];
    const { data: row, error } = await supabase
      .from("accounts").select("id, name, phone_number, api_id, api_hash, session_string, is_active")
      .eq("id", accountId).single();
    if (error || !row) return jsonResponse(res, 404, { error: "Conta não encontrada" });

    const account = row as Account;
    accountCache.set(accountId, account);
    if (!account.is_active || !account.session_string) {
      return jsonResponse(res, 200, { ok: true, skipped: true, reason: "conta inativa ou sem sessão" });
    }

    try {
      const client = await reloadClient(account);
      await client.getDialogs({ limit: 100 });
      return jsonResponse(res, 200, { ok: true });
    } catch (err: any) {
      return jsonResponse(res, 500, { error: err.message });
    }
  }

  const profileMatch = url.pathname.match(/^\/groups\/([^/]+)\/profile$/);
  if (req.method === "GET" && profileMatch) {
    const groupId = profileMatch[1];
    return jsonResponse(res, 200, {
      profile: groupProfileCache.get(groupId) ?? null,
      clock_offset_ms: telegramClockOffsetMs,
      clock_quality: clockOffsetQuality,
    });
  }

  if (req.method === "DELETE" && profileMatch) {
    const groupId = profileMatch[1];
    groupProfileCache.delete(groupId);
    await supabase.from("group_profiles").delete().eq("group_id", groupId);
    await supabase.from("group_dispatch_samples").delete().eq("group_id", groupId);
    return jsonResponse(res, 200, { ok: true, message: "Perfil resetado — próximos 3 disparos reaprendem o timing" });
  }

  if (req.method === "GET" && url.pathname === "/clock") {
    return jsonResponse(res, 200, {
      clock_offset_ms:        telegramClockOffsetMs,
      clock_quality:          clockOffsetQuality,
      estimated_one_way_rtt:  estimatedOneWayRttMs,
      local_time:             new Date().toISOString(),
      adjusted_time:          new Date(Date.now() + telegramClockOffsetMs).toISOString(),
    });
  }

  if (req.method === "POST" && url.pathname === "/clock/remeasure") {
    const anyClient = [...clients.values()].find(c => c.connected);
    if (!anyClient) return jsonResponse(res, 503, { error: "Sem client conectado" });
    const newOffset = await measureTelegramClockOffset(anyClient);
    telegramClockOffsetMs = newOffset;
    return jsonResponse(res, 200, { ok: true, clock_offset_ms: newOffset, clock_quality: clockOffsetQuality, estimated_one_way_rtt: estimatedOneWayRttMs });
  }

  const listenMatch = url.pathname.match(/^\/groups\/([^/]+)\/listen$/);
  if (listenMatch) {
    const groupId = listenMatch[1];

    if (req.method === "DELETE") {
      const ctrl = listenMap.get(groupId);
      if (ctrl) { ctrl.abort(); listenMap.delete(groupId); }
      return jsonResponse(res, 200, { ok: true });
    }

    if (req.method === "POST") {
      const existing = listenMap.get(groupId);
      if (existing) existing.abort();

      const ctrl = new AbortController();
      listenMap.set(groupId, ctrl);

      (async () => {
        try {
          const { data: grpRow } = await supabase
            .from("groups")
            .select(`id, telegram_chat_id, group_type, group_members(id, message_text, position, is_active, accounts(id, name, phone_number, api_id, api_hash, session_string, is_active))`)
            .eq("id", groupId).single();

          if (!grpRow) return;

          const chatId  = String(grpRow.telegram_chat_id);
          const members: GroupMember[] = (grpRow.group_members ?? []).map((m: any) => ({
            ...m,
            accounts: Array.isArray(m.accounts) ? (m.accounts[0] ?? null) : (m.accounts ?? null),
          }));

          const firstMember = members.find(m => m.is_active && m.accounts?.is_active);
          if (!firstMember?.accounts) return;

          const account = accountCache.get(firstMember.accounts.id) ?? firstMember.accounts as unknown as Account;
          const client  = await getClient(account);

          const deadline    = Date.now() + 2 * 60 * 60_000;
          const startUnix   = Math.floor((Date.now() - 10_000) / 1000);
          let lastSeenMsgId = 0;

          try { await resolvePeer(client, chatId, account.id); } catch {}

          while (Date.now() < deadline && !ctrl.signal.aborted) {
            try {
              const peer   = await resolvePeer(client, chatId, account.id);
              const result = await client.invoke(
                new Api.messages.GetHistory({
                  peer: peer as any, limit: 10,
                  offsetDate: 0, offsetId: 0, maxId: 0, minId: 0, hash: bigInt(0), addOffset: 0,
                })
              ) as any;

              const recentMsgs = (result.messages ?? []).filter(
                (m: any) => (m.className === "Message" || m._ === "message") && m.date >= startUnix && m.id > lastSeenMsgId
              );
              if (recentMsgs.length > 0) {
                lastSeenMsgId = Math.max(lastSeenMsgId, ...recentMsgs.map((m: any) => m.id as number));
              }

              const gotSignal = recentMsgs.some((m: any) => {
                const text = typeof m.message === "string" ? m.message.trim().toLowerCase() : "";
                return text === "ok" || (m.media != null && m.media.className !== "MessageMediaEmpty");
              });

              if (gotSignal && !ctrl.signal.aborted) {
                listenMap.delete(groupId);
                await supabase.from("groups").update({ listener_session_id: null }).eq("id", groupId);

                const { data: grpFull } = await supabase.from("groups").select("name, telegram_chat_name, user_id").eq("id", groupId).single();

                const scheduleStub = {
                  id: `manual-${groupId}-${Date.now()}`,
                  user_id: grpFull?.user_id ?? "",
                  group_id: groupId,
                  cron_expression: "0 0 * * 0",
                  next_run_at: new Date().toISOString(),
                  retry_window_seconds: 60, retry_interval_seconds: 5, retry_interval_max_seconds: 30,
                  retry_count: 0, retry_until: null, last_attempt_at: null,
                  groups: {
                    id: groupId, name: grpFull?.name ?? groupId,
                    telegram_chat_id: chatId, telegram_chat_name: grpFull?.telegram_chat_name ?? null,
                    group_type: "open" as const, group_members: members,
                  },
                };

                const dispatchedAt = new Date();
                const results      = await dispatchToGroup(scheduleStub as any, scheduleStub.groups, new Set());
                const sentForMonitor = results
                  .filter(r => r.status === "sent")
                  .map(r => ({ account_id: r.account_id, message_text: r.message_text ?? "" }))
                  .filter(r => r.message_text);
                if (sentForMonitor.length > 0) {
                  monitorPositions(chatId, sentForMonitor, scheduleStub.id, dispatchedAt, "open").catch(() => {});
                }
                return;
              }
            } catch (err: any) {
              if (!ctrl.signal.aborted) await new Promise(r => setTimeout(r, 2_000));
            }
            if (!ctrl.signal.aborted) await new Promise(r => setTimeout(r, LISTEN_POLL_MS));
          }

          if (!ctrl.signal.aborted) await supabase.from("groups").update({ listener_session_id: null }).eq("id", groupId);
          listenMap.delete(groupId);
        } catch (err: any) {
          console.error(`[listen-manual] Erro para grupo ${groupId}:`, err.message);
          listenMap.delete(groupId);
        }
      })();

      return jsonResponse(res, 200, { ok: true });
    }
  }

  const dispatchMatch = url.pathname.match(/^\/groups\/([^/]+)\/dispatch$/);
  if (req.method === "POST" && dispatchMatch) {
    const groupId = dispatchMatch[1];

    let body: { user_id?: string; send_to_self?: boolean } = {};
    try {
      const raw = await new Promise<string>((resolve, reject) => {
        let data = "";
        req.on("data", chunk => { data += chunk; });
        req.on("end", () => resolve(data));
        req.on("error", reject);
      });
      if (raw) body = JSON.parse(raw);
    } catch {}

    const sendToSelf = !!body.send_to_self;

    try {
      const { data, error } = await supabase
        .from("groups")
        .select(`id, name, telegram_chat_id, telegram_chat_name, group_type, group_members(id, message_text, position, is_active, accounts(id, name, phone_number, api_id, api_hash, session_string, is_active))`)
        .eq("id", groupId).single();

      if (error || !data) return jsonResponse(res, 404, { error: "Grupo não encontrado" });

      const group   = data as unknown as Group;
      const members = (group.group_members ?? [])
        .filter(m => m.is_active && m.accounts?.is_active && m.accounts?.session_string)
        .sort((a, b) => a.position - b.position);

      if (members.length === 0) return jsonResponse(res, 200, { ok: true, sent: 0, failed: 0, results: [] });

      let sent = 0, failed = 0;
      const results: Array<{ account_id: string; status: string; error?: string }> = [];

      for (const member of members) {
        const account = member.accounts
          ? (accountCache.get(member.accounts.id) ?? member.accounts as unknown as Account)
          : null;
        if (!account) continue;

        try {
          const client = await getClient(account);
          const text   = member.message_text ?? "";

          if (sendToSelf) {
            await Promise.race([
              client.invoke(new Api.messages.SendMessage({
                peer: new Api.InputPeerSelf(), message: text || "[teste de aquecimento]",
                randomId: makeRandomId(), noWebpage: true,
              })),
              new Promise<never>((_, r) => setTimeout(() => r(new Error("TIMEOUT")), SEND_TIMEOUT_MS)),
            ]);
          } else {
            if (!group.telegram_chat_id) throw new Error("telegram_chat_id não configurado");
            await sendMessage(client, account, String(group.telegram_chat_id), text);
          }

          sent++;
          results.push({ account_id: account.id, status: "sent" });
        } catch (err: any) {
          failed++;
          results.push({ account_id: account.id, status: "failed", error: err.message });
        }
      }

      return jsonResponse(res, 200, { ok: true, sent, failed, results });
    } catch (err: any) {
      return jsonResponse(res, 500, { error: err.message });
    }
  }

  jsonResponse(res, 404, { error: "Not found" });
});

httpServer.listen(WORKER_PORT, () => {
  console.log(`[worker] HTTP interno escutando na porta ${WORKER_PORT}`);
});

/* ─────────────────────────────────────────────────────────────────────────────
   GRACEFUL SHUTDOWN
   ───────────────────────────────────────────────────────────────────────────── */
async function shutdown() {
  console.log("[worker] Encerrando...");

  for (const t of prefetchTimers.values()) clearTimeout(t);
  prefetchTimers.clear();
  for (const t of sniperTimers.values()) clearTimeout(t);
  sniperTimers.clear();
  for (const t of scheduledTimers.values()) clearTimeout(t);
  scheduledTimers.clear();
  for (const t of keepaliveTimers.values()) clearInterval(t);
  keepaliveTimers.clear();

  httpServer.close();

  await Promise.all([...clients.entries()].map(async ([id, client]) => {
    try { await client.disconnect(); } catch {}
  }));
  clients.clear();

  process.exit(0);
}
process.on("SIGTERM", shutdown);
process.on("SIGINT",  shutdown);

/* ─────────────────────────────────────────────────────────────────────────────
   INICIALIZAÇÃO
   ───────────────────────────────────────────────────────────────────────────── */
async function init(): Promise<void> {
  console.log("[worker] Iniciando v14 — Loop sempre agressivo. Gate só bloqueia opens_early antes do horário...");
  await prewarmAccounts();
  await reloadSchedules();

  setInterval(async () => {
    try {
      await Promise.allSettled([reloadSchedules(), reconnectDeadClients()]);
    } catch (err) {
      console.error("[reload] Erro no reload periódico:", err);
    }
  }, RELOAD_INTERVAL_MS);

  setInterval(async () => {
    const anyClient = [...clients.values()].find(c => c.connected);
    if (!anyClient) return;
    const newOffset = await measureTelegramClockOffset(anyClient);
    if (newOffset !== telegramClockOffsetMs) {
      console.log(`[clock] Offset atualizado: ${telegramClockOffsetMs}ms → ${newOffset}ms (${clockOffsetQuality})`);
      telegramClockOffsetMs = newOffset;
    }
  }, CLOCK_OFFSET_REFRESH_MS);

  console.log("[worker] Pronto v15. RTT-aware floor para opens_early. Loop agressivo para demais grupos.");
}

init().catch(err => {
  console.error("[worker] Falha na inicialização:", err);
  process.exit(1);
});

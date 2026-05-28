// worker-v10.ts — dispatch worker Telegram, timing adaptativo por grupo
//
// ═══════════════════════════════════════════════════════════════════════════
// HISTÓRICO DE FIXES (v1–v9 preservados para referência)
// ═══════════════════════════════════════════════════════════════════════════
//
// Fix v1: firingNow Set previne duplo disparo (race condition RETRY_BUDGET_MS vs RELOAD_INTERVAL_MS)
// Fix v2: BUG #1 reconnect loop; BUG #2 peerCache stale; BUG #3 backoff; BUG #4 prewarm awaited
// Fix v3: BUG #5 AUTH_KEY_DUPLICATED via race em getClient() → connectingPromises serializa conexões
// Otimizações v4: invoke direto, pre-fetch, pre-resolve peers, noWebpage, randomId único
// Fix v5: sniperFireClosed() — loop ultra-agressivo para grupos fechados
// Fix v6: BUG #A duplo disparo; BUG #B groupType perdido; BUG #C FloodWait; BUG #D reloadClient;
//         BUG #E prewarm a cada 30s; BUG #F makeRandomId; BUG #G keepalive jitter
// Fix v7: timing logs + SNIPER_BEFORE_MS aumentado para 45ms
// Fix v8: BUG #H randomId estável por ciclo (deduplicação Telegram)
// Fix v9: BUG #I SNIPER_BEFORE_MS=45 → pacote chegava antes da virada →
//         SNIPER_BEFORE_MS virou warm-up (200ms) + SNIPER_AFTER_GUARD_MS=15ms busy-wait
//
// ═══════════════════════════════════════════════════════════════════════════
// Fix v10 — Adaptive Gate: timing dinâmico por perfil de grupo
// ═══════════════════════════════════════════════════════════════════════════
//
// PROBLEMA v9:
//   SNIPER_AFTER_GUARD_MS=15 + busy-spin de até 15ms + disparos simultâneos
//   congestionando o event loop → latência observada de 60–100ms no disparo.
//   Antes da v9 (sem guard): 3–5ms. Guard resolveu o carimbo errado mas
//   quebrou a latência para 10–20x pior.
//
// RAIZ DO PROBLEMA:
//   Guard fixo ignora que cada grupo tem comportamento diferente:
//   - Grupos "opens_early": abrem antes do horário nominal → guard desnecessário
//   - Grupos "on-time": abrem no horário → guard pequeno ou zero
//   - Grupos "opens_late": abrem depois → guard negativo (esperar mais)
//   Um offset fixo de 15ms penaliza todos para cobrir o pior caso de um.
//
// SOLUÇÃO v10 — 3 camadas:
//
// CAMADA 1 — Spin reduzido (imediato, zero infra):
//   SNIPER_SPIN_MAX_MS = 2 → spin máximo de 2ms em vez dos atuais ≤15ms.
//   sleep(msUntilFire - 2ms) libera event loop → sem congestionamento em
//   disparos simultâneos. Ganho esperado: recupera 10–13ms imediatamente.
//
// CAMADA 2 — Guard adaptativo por grupo (requer 2 novas tabelas no Supabase):
//   Cada disparo grava vsHorárioMs em group_dispatch_samples.
//   Após ≥3 amostras, computa p10/p50/p90 e atualiza group_profiles.
//   O próximo disparo usa esses percentis para calcular guardMs:
//     opens_early (p50 < -20ms): guardMs = p10 - 50ms → tenta antes do horário
//     on-time/late:               guardMs = max(0, p50 - 20ms) → próximo da mediana
//     sem dados:                  guardMs = 0 → dispara em scheduledAt exato
//
// CAMADA 3 — Sniper timer mais cedo para opens_early:
//   scheduleTimer() consulta o perfil do grupo. Para opens_early, o sniperTimer
//   é agendado com extra lead = abs(p10) + 100ms. Garante que o sniper já está
//   rodando quando o grupo abrir, mesmo que seja muito antes do horário nominal.
//
// SQL NECESSÁRIO (rodar no Supabase antes do deploy):
//
//   create table if not exists group_dispatch_samples (
//     id            uuid primary key default gen_random_uuid(),
//     group_id      uuid not null references groups(id) on delete cascade,
//     vs_horario_ms int not null,
//     created_at    timestamptz default now()
//   );
//   create index if not exists idx_gds_group_created
//     on group_dispatch_samples(group_id, created_at desc);
//
//   create table if not exists group_profiles (
//     group_id          uuid primary key references groups(id) on delete cascade,
//     offset_p10_ms     int not null default 0,
//     offset_p50_ms     int not null default 0,
//     offset_p90_ms     int not null default 0,
//     opens_early       bool not null default false,
//     min_safe_guard_ms int not null default 0,
//     sample_count      int not null default 0,
//     updated_at        timestamptz default now()
//   );

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

// v10: warm-up do loop sniper — começa 100ms antes do horário (mais agressivo que os 200ms da v9,
// o extra lead por opens_early é adicionado dinamicamente em scheduleTimer()).
const SNIPER_BEFORE_MS              = 100;

// v10: REMOVIDO SNIPER_AFTER_GUARD_MS fixo. Agora é calculado dinamicamente por grupo
// em sniperFireClosed() via groupProfileCache. Default = 0 (dispara em scheduledAt exato).
// O RTT one-way Railway US East → Miami DC (~25ms) garante que o pacote chega APÓS a virada.

// v10: spin máximo no busy-wait de precisão.
// Antes: spin podia durar até 15ms (SNIPER_AFTER_GUARD_MS).
// Agora: spin máximo de 2ms — o resto do wait é sleep() que libera o event loop.
// Isso resolve o congestionamento em disparos simultâneos.
const SNIPER_SPIN_MAX_MS            = 2;

// v10: tamanho da janela rolling para cálculo de percentis de timing.
const GROUP_PROFILE_SAMPLE_SIZE     = 20;

const SNIPER_SEND_TIMEOUT_MS        = 800;
const SNIPER_ATTEMPT_INTERVAL_MS    = 1;
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

// v10: perfil de comportamento de timing de um grupo fechado.
// Alimentado por persistGroupDispatchSample() após cada disparo do sniper.
interface GroupBehaviorProfile {
  group_id:          string;
  offset_p10_ms:     number;  // percentil 10 de vsHorárioMs (abre mais cedo)
  offset_p50_ms:     number;  // mediana de vsHorárioMs
  offset_p90_ms:     number;  // percentil 90 de vsHorárioMs (abre mais tarde)
  opens_early:       boolean; // true se p50 < -20ms (grupo historicamente abre antes)
  min_safe_guard_ms: number;  // guardMs calculado: p10 - 50ms (opens_early) ou 0
  sample_count:      number;  // amostras coletadas (mínimo 3 para ativar o adaptive gate)
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

// v10: cache de perfis de timing por group_id.
// Carregado no boot por loadGroupProfiles() e atualizado após cada disparo.
const groupProfileCache     = new Map<string, GroupBehaviorProfile>();

// v10: mapa scheduleId → groupId para scheduleTimer() consultar o perfil.
// Populado em reloadSchedules() e updateScheduleAfterDispatch().
const scheduleGroupMap      = new Map<string, string>();

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
   v10: PERFIS DE TIMING ADAPTATIVO
   ───────────────────────────────────────────────────────────────────────────── */

// Carrega group_profiles do banco no boot. Chamado uma vez em prewarmAccounts().
async function loadGroupProfiles(): Promise<void> {
  try {
    const { data, error } = await supabase
      .from("group_profiles")
      .select("group_id, offset_p10_ms, offset_p50_ms, offset_p90_ms, opens_early, min_safe_guard_ms, sample_count");

    if (error) {
      console.warn("[profiles] Falha ao carregar perfis de grupo:", error.message);
      return;
    }

    for (const row of data ?? []) {
      groupProfileCache.set(row.group_id as string, row as GroupBehaviorProfile);
    }

    console.log(`[profiles] ✅ ${groupProfileCache.size} perfis de timing carregados`);
  } catch (err: any) {
    console.warn("[profiles] Erro inesperado ao carregar perfis:", err.message);
  }
}

// Persiste uma nova amostra de vsHorárioMs e recomputa os percentis do grupo.
// Fire-and-forget: chamado no finally do sniper após confirmação de envio.
// Não bloqueia o caminho crítico.
async function persistGroupDispatchSample(groupId: string, vsHorarioMs: number): Promise<void> {
  try {
    // 1. Grava a nova amostra
    const { error: insertErr } = await supabase
      .from("group_dispatch_samples")
      .insert({ group_id: groupId, vs_horario_ms: vsHorarioMs });

    if (insertErr) {
      console.warn(`[profiles] Falha ao inserir amostra para ${groupId}:`, insertErr.message);
    }

    // 2. Busca as últimas N amostras (rolling window)
    const { data, error: fetchErr } = await supabase
      .from("group_dispatch_samples")
      .select("vs_horario_ms")
      .eq("group_id", groupId)
      .order("created_at", { ascending: false })
      .limit(GROUP_PROFILE_SAMPLE_SIZE);

    if (fetchErr || !data || data.length === 0) return;

    // 3. Computa percentis
    const samples = data.map(r => r.vs_horario_ms as number);
    const sorted  = [...samples].sort((a, b) => a - b);
    const n       = sorted.length;

    const p10 = sorted[Math.max(0, Math.floor(n * 0.10))] ?? sorted[0];
    const p50 = sorted[Math.max(0, Math.floor(n * 0.50))] ?? sorted[0];
    const p90 = sorted[Math.min(n - 1, Math.floor(n * 0.90))] ?? sorted[n - 1];

    // opens_early: mediana histórica está mais de 20ms antes do horário nominal
    const opens_early       = p50 < -20;
    // min_safe_guard_ms: para opens_early, tenta 50ms antes do p10 (mais cedo já observado)
    //                    para os demais, dispara em scheduledAt (guard = 0)
    const min_safe_guard_ms = opens_early
      ? Math.max(-30_000, p10 - 50)
      : 0;

    const profile: GroupBehaviorProfile = {
      group_id: groupId,
      offset_p10_ms: p10,
      offset_p50_ms: p50,
      offset_p90_ms: p90,
      opens_early,
      min_safe_guard_ms,
      sample_count: n,
    };

    // 4. Atualiza cache local imediatamente
    groupProfileCache.set(groupId, profile);

    // 5. Persiste no banco (upsert)
    const { error: upsertErr } = await supabase
      .from("group_profiles")
      .upsert(
        { ...profile, updated_at: new Date().toISOString() },
        { onConflict: "group_id" }
      );

    if (upsertErr) {
      console.warn(`[profiles] Falha ao salvar perfil para ${groupId}:`, upsertErr.message);
    } else {
      console.log(
        `[profiles] ✅ Perfil atualizado: group=${groupId} ` +
        `p10=${p10}ms p50=${p50}ms p90=${p90}ms ` +
        `opens_early=${opens_early} n=${n}`
      );
    }
  } catch (err: any) {
    console.warn(`[profiles] Erro inesperado ao persistir amostra para ${groupId}:`, err.message);
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
        console.warn(`[keepalive] ${account.phone_number} desconectou — removendo do pool`);
        clients.delete(account.id);
        evictPeersForAccount(account.id);
        keepaliveTimers.delete(account.id);
        clearInterval(interval);
        return;
      }
      try {
        await Promise.race([
          client.getMe(),
          new Promise<never>((_, r) =>
            setTimeout(() => r(new Error("keepalive timeout")), 10_000)
          ),
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
          console.warn(`[keepalive] Sessão morta: ${account.phone_number} — desativando no banco`);
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
              peer:      peer as any,
              message:   messageText,
              randomId:  stableRandomId,
              noWebpage: true,
            }));
          } catch (err: any) {
            const errMsg = String(err?.message ?? "");

            if (
              errMsg.includes("PEER_ID_INVALID") ||
              errMsg.includes("CHANNEL_INVALID") ||
              errMsg.includes("CHANNEL_PRIVATE")
            ) {
              peerCache.delete(`${account.id}:${telegramChatId}`);
            }

            const isFlood =
              err?.seconds != null ||
              err?.constructor?.name === "FloodWaitError" ||
              /flood/i.test(errMsg);

            if (isFlood) {
              const waitSecs: number =
                typeof err.seconds === "number"
                  ? err.seconds
                  : parseInt(errMsg.match(/(\d+)/)?.[1] ?? "30", 10);
              const waitMs = waitSecs * 1000;
              console.warn(`[send] FloodWait ${waitSecs}s — ${account.phone_number}`);

              if (waitMs < budgetEnd - Date.now() - 500) {
                await new Promise(r => setTimeout(r, waitMs));
                peerCache.delete(`${account.id}:${telegramChatId}`);
                const freshPeer = await resolvePeer(client, telegramChatId, account.id);
                await client.invoke(new Api.messages.SendMessage({
                  peer:      freshPeer as any,
                  message:   messageText,
                  randomId:  stableRandomId,
                  noWebpage: true,
                }));
                return;
              }
              throw new Error(`FLOOD_WAIT_${waitSecs}_EXCEEDS_BUDGET`);
            }

            throw err;
          }
        })(),
        new Promise<never>((_, r) =>
          setTimeout(
            () => r(new Error(`TIMEOUT tentativa ${attempt}`)),
            Math.min(SEND_TIMEOUT_MS, timeLeft - 100)
          )
        ),
      ]);

      if (attempt > 1) console.log(`[send] ✓ ${account.phone_number} — enviou na tentativa ${attempt}`);
      return;

    } catch (err: any) {
      const remaining = budgetEnd - Date.now();
      if (remaining > 500) {
        const backoffMs   = Math.min(1_000 * Math.pow(2, attempt - 1), SEND_RETRY_BACKOFF_MAX_MS);
        const safeBackoff = Math.min(backoffMs, remaining - 500);
        console.warn(
          `[send] tentativa ${attempt} falhou — aguardando ${safeBackoff}ms ` +
          `(${Math.round(remaining / 1000)}s restantes): ${err.message}`
        );
        if (safeBackoff > 0) {
          await new Promise(r => setTimeout(r, safeBackoff));
        }
      }
    }
  }

  throw new Error(`BUDGET_EXCEEDED após ${attempt} tentativa(s) em ${RETRY_BUDGET_MS / 1000}s`);
}

/* ─────────────────────────────────────────────────────────────────────────────
   SNIPER — ENVIO ÚNICO COM TIMEOUT CURTO
   ───────────────────────────────────────────────────────────────────────────── */
async function sniperSendOnce(
  client: TelegramClient,
  account: Account,
  telegramChatId: string,
  messageText: string,
  randomId: bigInt.BigInteger
): Promise<void> {
  const peer = await resolvePeer(client, telegramChatId, account.id);

  await Promise.race([
    client.invoke(new Api.messages.SendMessage({
      peer:      peer as any,
      message:   messageText,
      randomId,
      noWebpage: true,
    })),
    new Promise<never>((_, r) =>
      setTimeout(() => r(new Error("SNIPER_TIMEOUT")), SNIPER_SEND_TIMEOUT_MS)
    ),
  ]);
}

/* ─────────────────────────────────────────────────────────────────────────────
   SNIPER LOOP — GRUPOS FECHADOS (v10: adaptive gate)
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
        .from("schedules")
        .select(SCHEDULE_SELECT)
        .eq("id", scheduleId)
        .eq("is_active", true)
        .single();
      if (error || !data) {
        console.warn(`[sniper] Schedule ${scheduleId} não encontrado ou inativo.`);
        return;
      }
      schedule = data as unknown as Schedule;
    }

    const scheduledAt     = new Date(schedule.next_run_at).getTime();
    const plannedSniperAt = scheduledAt - SNIPER_BEFORE_MS;
    const timerLagMs      = sniperEnteredAt - plannedSniperAt;
    console.log(`[sniper][timing] timer lag: ${timerLagMs}ms (ideal=0, SNIPER_BEFORE_MS=${SNIPER_BEFORE_MS})`);

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

    // ── FASE 0: conecta a primeira conta antes do gate ────────────────────
    const firstMember  = members[0];
    const firstAccount = firstMember.accounts!;
    const firstText    = firstMember.message_text ?? "";

    let firstClient: TelegramClient;
    try {
      firstClient = await getClient(firstAccount);
    } catch (err: any) {
      console.error(`[sniper] Falha ao conectar ${firstAccount.phone_number}: ${err.message}`);
      const results: DispatchResult[] = [{
        account_id:   firstAccount.id,
        message_text: firstMember.message_text,
        status:       "failed",
        retryable:    isRetryableError(err.message),
        error:        err.message,
      }];
      await updateScheduleAfterDispatch(schedule, results, now, "closed");
      return;
    }

    // ── v10: ADAPTIVE GATE ────────────────────────────────────────────────
    // Calcula guardMs dinamicamente com base no perfil histórico do grupo.
    // guardMs negativo = invokeNotBefore está ANTES de scheduledAt (opens_early).
    // guardMs zero     = dispara em scheduledAt (padrão para grupos sem dados).
    // guardMs positivo = aguarda até scheduledAt + guardMs (opens_late).
    const profile           = groupProfileCache.get(group.id);
    const hasSufficientData = (profile?.sample_count ?? 0) >= 3;

    let guardMs: number;
    if (hasSufficientData) {
      if (profile!.opens_early) {
        // Grupo historicamente abre antes do horário.
        // Tenta 50ms antes do p10 (mais cedo já observado) para pegar a abertura.
        // Exemplo: p10 = -400ms → guardMs = -450ms → invokeNotBefore = scheduledAt - 450ms
        guardMs = Math.max(-30_000, profile!.offset_p10_ms - 50);
      } else {
        // Grupo abre no horário ou depois.
        // Dispara 20ms antes da mediana histórica (chega um pouco antes do ponto típico).
        // Exemplo: p50 = +80ms → guardMs = 60ms → aguarda scheduledAt + 60ms
        guardMs = Math.max(0, profile!.offset_p50_ms - 20);
      }
    } else {
      // Sem dados suficientes: dispara no scheduledAt exato.
      // O RTT one-way Railway US East → Miami DC (~25ms) garante que o pacote
      // chega ao Telegram APÓS scheduledAt — sem risco de carimbo no minuto anterior.
      guardMs = 0;
    }

    console.log(
      `[sniper][adaptive] guardMs=${guardMs}ms | ` +
      (hasSufficientData
        ? `p10=${profile!.offset_p10_ms}ms p50=${profile!.offset_p50_ms}ms p90=${profile!.offset_p90_ms}ms opens_early=${profile!.opens_early} n=${profile!.sample_count}`
        : `sem perfil (requer ≥3 amostras, atual: ${profile?.sample_count ?? 0})`)
    );

    const invokeNotBefore = scheduledAt + guardMs;

    // ── v10: SLEEP + SPIN (máx 2ms de busy-spin) ─────────────────────────
    // Antes (v9): spin de até 15ms → congestionava event loop em disparos simultâneos.
    // Agora: sleep libera event loop → spin apenas nos últimos 2ms para precisão.
    {
      const msUntilFire = invokeNotBefore - Date.now();
      if (msUntilFire > SNIPER_SPIN_MAX_MS) {
        const sleepMs = msUntilFire - SNIPER_SPIN_MAX_MS;
        console.log(
          `[sniper][timing] aguardando gate: sleep ${sleepMs}ms + spin ≤${SNIPER_SPIN_MAX_MS}ms ` +
          `(invokeNotBefore=${new Date(invokeNotBefore).toISOString()})`
        );
        await new Promise(r => setTimeout(r, sleepMs));
      } else if (msUntilFire > 0) {
        console.log(`[sniper][timing] spin ≤${msUntilFire}ms (já próximo do gate)`);
      } else {
        console.log(`[sniper][timing] gate já passou em ${-msUntilFire}ms — invocando imediatamente`);
      }
      // busy-spin de precisão sub-ms — máx 2ms
      while (Date.now() < invokeNotBefore) { /* spin */ }
    }

    // ── FASE 1: loop agressivo na primeira conta ──────────────────────────
    const results: DispatchResult[] = [];
    let attempt        = 0;
    let firstSentAt: Date | null = null;
    const firstRandomId = makeRandomId();

    while (Date.now() < budgetEnd) {
      attempt++;

      try {
        await sniperSendOnce(firstClient, firstAccount, chatId, firstText, firstRandomId);
        firstSentAt = new Date();

        // v10: métricas de timing para diagnóstico e alimentar o perfil
        const invokeRttMs       = firstSentAt.getTime() - sniperEnteredAt;
        const vsHorarioMs       = firstSentAt.getTime() - scheduledAt;
        const vsGateMs          = firstSentAt.getTime() - invokeNotBefore;
        console.log(`[sniper][timing] invoke RTT: ${invokeRttMs}ms (tempo total desde entrada do sniper)`);
        console.log(`[sniper][timing] vs horário: ${vsHorarioMs > 0 ? "+" : ""}${vsHorarioMs}ms (negativo=antes, positivo=atrasado) tentativa=${attempt}`);
        console.log(`[sniper][timing] vs gate: +${vsGateMs}ms desde invokeNotBefore (guardMs=${guardMs}ms)`);

        console.log(`[sniper] ✓ Primeira conta ${firstAccount.phone_number} enviou na tentativa ${attempt} (${firstSentAt.toISOString()})`);
        results.push({
          account_id:   firstAccount.id,
          message_text: firstMember.message_text,
          status:       "sent",
          retryable:    false,
        });

        // v10: persiste a amostra de timing fire-and-forget (não bloqueia)
        persistGroupDispatchSample(group.id, vsHorarioMs).catch(e =>
          console.warn("[profiles] Erro ao persistir amostra:", e.message)
        );

        break;
      } catch (err: any) {
        const errMsg = String(err?.message ?? "");
        const isFatal =
          errMsg.includes("AUTH_KEY_UNREGISTERED") ||
          errMsg.includes("USER_DEACTIVATED")      ||
          errMsg.includes("SESSION_REVOKED");

        if (isFatal) {
          console.error(`[sniper] Erro fatal na conta ${firstAccount.phone_number}: ${errMsg}`);
          results.push({
            account_id:   firstAccount.id,
            message_text: firstMember.message_text,
            status:       "failed",
            retryable:    false,
            error:        errMsg,
          });
          await updateScheduleAfterDispatch(schedule, results, now, "closed");
          return;
        }

        const isFlood =
          err?.seconds != null ||
          err?.constructor?.name === "FloodWaitError" ||
          /flood/i.test(errMsg);

        if (isFlood) {
          const waitSecs = typeof err.seconds === "number"
            ? err.seconds
            : parseInt(errMsg.match(/(\d+)/)?.[1] ?? "5", 10);
          const waitMs = waitSecs * 1000;
          console.warn(`[sniper] FloodWait ${waitSecs}s — pausando loop (budget restante: ${Math.round((budgetEnd - Date.now()) / 1000)}s)`);
          if (waitMs < budgetEnd - Date.now() - 500) {
            await new Promise(r => setTimeout(r, waitMs));
            continue;
          } else {
            console.warn(`[sniper] FloodWait ${waitSecs}s excede budget — encerrando loop`);
            break;
          }
        }

        if (
          errMsg.includes("PEER_ID_INVALID") ||
          errMsg.includes("CHANNEL_INVALID") ||
          errMsg.includes("CHANNEL_PRIVATE")
        ) {
          peerCache.delete(`${firstAccount.id}:${chatId}`);
        }

        if (attempt % SNIPER_PAUSE_EVERY_N === 0) {
          await new Promise(r => setTimeout(r, SNIPER_PAUSE_MS));
        } else {
          await new Promise(r => setTimeout(r, SNIPER_ATTEMPT_INTERVAL_MS));
        }
      }
    }

    // Log da primeira conta em background
    supabase.from("dispatch_logs").insert({
      user_id:             schedule.user_id,
      group_id:            group.id,
      account_id:          firstAccount.id,
      schedule_id:         schedule.id,
      status:              firstSentAt ? "sent" : "failed",
      message_text:        firstMember.message_text,
      position_rank:       1,
      group_name_snapshot: group.name,
      chat_name_snapshot:  group.telegram_chat_name,
      sent_at:             firstSentAt ? firstSentAt.toISOString() : null,
      error_message:       firstSentAt ? null : `BUDGET_EXCEEDED após ${attempt} tentativas`,
    }).then(({ error: e }) => {
      if (e) console.error(`[sniper][log] Falha ao inserir log para ${firstAccount.id}:`, e.message);
    });

    if (!firstSentAt) {
      console.warn(`[sniper] Budget esgotado para schedule ${scheduleId} após ${attempt} tentativas`);
      results.push({
        account_id:   firstAccount.id,
        message_text: firstMember.message_text,
        status:       "failed",
        retryable:    true,
        error:        `SNIPER_BUDGET_EXCEEDED após ${attempt} tentativas`,
      });
      await updateScheduleAfterDispatch(schedule, results, now, "closed");
      return;
    }

    // ── FASE 2: demais contas em sequência com delay de 1ms ──────────────
    for (let i = 1; i < members.length; i++) {
      await new Promise(r => setTimeout(r, SNIPER_INTER_ACCOUNT_DELAY_MS));

      const member  = members[i];
      const account = member.accounts!;
      const text    = member.message_text ?? "";
      let   sentAt: Date | null = null;
      let   error: string | undefined;

      try {
        const client = await getClient(account);
        const memberRandomId = makeRandomId();
        await sniperSendOnce(client, account, chatId, text, memberRandomId);
        sentAt = new Date();
        console.log(`[sniper] ✓ Conta ${i + 1}/${members.length} ${account.phone_number} enviou`);
        results.push({
          account_id:   account.id,
          message_text: member.message_text,
          status:       "sent",
          retryable:    false,
        });
      } catch (err: any) {
        error = String(err?.message ?? "");
        console.error(`[sniper] ✗ Conta ${i + 1}/${members.length} ${account.phone_number}: ${error}`);
        results.push({
          account_id:   account.id,
          message_text: member.message_text,
          status:       "failed",
          retryable:    isRetryableError(error),
          error,
        });
      }

      supabase.from("dispatch_logs").insert({
        user_id:             schedule.user_id,
        group_id:            group.id,
        account_id:          account.id,
        schedule_id:         schedule.id,
        status:              sentAt ? "sent" : "failed",
        message_text:        member.message_text,
        position_rank:       i + 1,
        group_name_snapshot: group.name,
        chat_name_snapshot:  group.telegram_chat_name,
        sent_at:             sentAt ? sentAt.toISOString() : null,
        error_message:       error ?? null,
      }).then(({ error: e }) => {
        if (e) console.error(`[sniper][log] Falha ao inserir log para ${account.id}:`, e.message);
      });
    }

    // ── FASE 3: monitoramento de posições ─────────────────────────────────
    const sentForMonitor = results
      .filter(r => r.status === "sent")
      .map(r => ({ account_id: r.account_id, message_text: r.message_text ?? "" }))
      .filter(r => r.message_text);

    if (sentForMonitor.length > 0) {
      monitorPositions(chatId, sentForMonitor, scheduleId, firstSentAt, "closed")
        .catch(err => console.error("[sniper][monitor] Erro:", err.message));
    }

    // ── FASE 4: atualiza schedule no banco ────────────────────────────────
    await updateScheduleAfterDispatch(schedule, results, firstSentAt, "closed");

  } finally {
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
    ? new Date(
        new Date(schedule.retry_until).getTime() - schedule.retry_window_seconds * 1000
      ).toISOString()
    : schedule.next_run_at;

  const { data, error } = await supabase
    .from("dispatch_logs")
    .select("account_id")
    .eq("schedule_id", schedule.id)
    .eq("status", "sent")
    .gte("sent_at", cycleStart);

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
  if (!account) { console.warn("[monitor] Conta não encontrada no cache — ignorando"); return; }

  const client = await getClient(account).catch(() => null);
  if (!client) { console.warn("[monitor] Sem client — ignorando monitoramento"); return; }

  const windowStartUnix = Math.floor((dispatchedAt.getTime() - 15_000) / 1000);
  const deadline        = Date.now() + (groupType === "closed"
    ? MONITOR_DELAY_CLOSED_MS + 10_000
    : MONITOR_MAX_OPEN_MS);
  const ourTexts = new Set(sentMembers.map(m => m.message_text).filter(Boolean));

  if (groupType === "closed") await new Promise(r => setTimeout(r, MONITOR_DELAY_CLOSED_MS));

  console.log(`[monitor] Iniciando para schedule ${scheduleId} (${groupType})`);

  while (Date.now() < deadline) {
    try {
      const peer   = await resolvePeer(client, telegramChatId, account.id);
      const result = await client.invoke(
        new Api.messages.GetHistory({
          peer: peer as any,
          limit: MONITOR_HISTORY_LIMIT,
          offsetDate: 0, offsetId: 0, maxId: 0, minId: 0,
          hash: bigInt(0), addOffset: 0,
        })
      ) as any;

      const windowMsgs = (result.messages ?? [])
        .filter((m: any) => m._ === "message" && m.date >= windowStartUnix)
        .reverse();

      if (windowMsgs.length === 0) {
        if (groupType === "closed") {
          console.warn("[monitor] Sem mensagens na janela (grupo fechado) — abortando");
          return;
        }
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
        console.log(`[monitor] ${sm.account_id}: posição #${rank} em ${telegramChatId}`);
        return supabase.from("dispatch_logs")
          .update({ position_rank: rank })
          .eq("schedule_id", scheduleId)
          .eq("account_id", sm.account_id)
          .eq("status", "sent")
          .gte("sent_at", cutoff);
      }));

      console.log(`[monitor] ✓ Posições salvas para schedule ${scheduleId}`);
      return;

    } catch (err: any) {
      console.warn(`[monitor] Erro ao buscar histórico: ${err.message}`);
      if (groupType === "closed") return;
      await new Promise(r => setTimeout(r, MONITOR_POLL_MS));
    }
  }

  console.warn(`[monitor] Timeout — posições não registradas para schedule ${scheduleId}`);
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
      if (!client) {
        console.warn(`[listen] Sem client — abortando listener para ${schedule.id}`);
        listenMap.delete(group.id);
        return;
      }

      try { await resolvePeer(client, group.telegram_chat_id!, account.id); } catch {}

      while (Date.now() < deadline && !ctrl.signal.aborted) {
        try {
          if (!client.connected) {
            console.warn(`[listen] Client desconectou — reconectando para ${schedule.id}`);
            client = await getClient(account);
            try { await resolvePeer(client, group.telegram_chat_id!, account.id); } catch {}
          }

          const peer   = await resolvePeer(client, group.telegram_chat_id!, account.id);
          const result = await client.invoke(
            new Api.messages.GetHistory({
              peer: peer as any, limit: 10,
              offsetDate: 0, offsetId: 0, maxId: 0, minId: 0,
              hash: bigInt(0), addOffset: 0,
            })
          ) as any;

          const recentMsgs = (result.messages ?? []).filter(
            (m: any) =>
              (m.className === "Message" || m._ === "message") &&
              m.date >= startUnix &&
              m.id > lastSeenMsgId
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
            console.log(`[listen] ✓ Sinal detectado — disparando schedule ${schedule.id}`);
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
          if (!ctrl.signal.aborted) {
            console.warn(`[listen] Erro ao buscar histórico (${schedule.id}): ${err.message}`);
            await new Promise(r => setTimeout(r, 2_000));
          }
        }

        if (!ctrl.signal.aborted) await new Promise(r => setTimeout(r, LISTEN_POLL_MS));
      }

      listenMap.delete(group.id);

      if (ctrl.signal.aborted) {
        console.log(`[listen] ⏹ Listener abortado para schedule ${schedule.id}`);
        return;
      }

      console.warn(`[listen] ⏰ Timeout 2h — nenhum sinal para schedule ${schedule.id}`);
      const nowISO = new Date().toISOString();
      let nextRun: string;
      try { nextRun = nextWeeklyOccurrence(schedule.cron_expression); }
      catch {
        await supabase.from("schedules").update({ is_active: false }).eq("id", schedule.id);
        return;
      }
      await supabase.from("schedules").update({
        next_run_at:         nextRun,
        retry_until:         null,
        retry_count:         0,
        last_attempt_at:     nowISO,
        last_attempt_status: "timeout",
        last_attempt_error:  "Timeout aguardando sinal do admin",
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
      console.log(`[dispatch] ↷ ${account.phone_number} — já enviou neste ciclo`);
      return {
        account_id:   account.id,
        message_text: member.message_text,
        status:       "skipped" as const,
        retryable:    false,
      };
    }

    let status: "sent" | "failed" = "failed";
    let error: string | undefined;
    let retryable = false;

    try {
      const client = await getClient(account);
      await sendMessage(client, account, group.telegram_chat_id!, member.message_text ?? "");
      status = "sent";
      alreadySent.add(account.id);
      console.log(`[dispatch] ✓ ${account.phone_number}`);
    } catch (err) {
      error     = err instanceof Error ? err.message : String(err);
      retryable = isRetryableError(error);
      console.error(
        `[dispatch] ✗ ${account.phone_number} [${retryable ? "retryável" : "permanente"}]: ${error}`
      );
    }

    supabase.from("dispatch_logs").insert({
      user_id:             schedule.user_id,
      group_id:            group.id,
      account_id:          account.id,
      schedule_id:         schedule.id,
      status,
      message_text:        member.message_text,
      position_rank:       positionRank,
      group_name_snapshot: group.name,
      chat_name_snapshot:  group.telegram_chat_name,
      sent_at:             status === "sent" ? new Date().toISOString() : null,
      error_message:       error ?? null,
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

  // v10: mantém mapa scheduleId → groupId atualizado
  if (groupId) scheduleGroupMap.set(schedule.id, groupId);

  if (allOk) {
    let nextRun: string;
    try {
      nextRun = nextWeeklyOccurrence(schedule.cron_expression);
    } catch (err) {
      console.error(`[schedule] cron inválido em ${schedule.id}, desativando:`, err);
      await supabase.from("schedules").update({ is_active: false }).eq("id", schedule.id);
      return;
    }

    supabase.from("schedules").update({
      next_run_at:         nextRun,
      last_run_at:         nowISO,
      retry_until:         null,
      retry_count:         0,
      last_attempt_at:     nowISO,
      last_attempt_status: "sent",
      last_attempt_error:  null,
    }).eq("id", schedule.id).then(({ error: e }) => {
      if (e) console.error(`[schedule] Falha ao atualizar ${schedule.id}:`, e.message);
    });

    console.log(`[schedule] ✓ Schedule ${schedule.id} OK. Próxima: ${nextRun}`);
    scheduleTimer(schedule.id, nextRun, resolvedGroupType, groupId);

  } else {
    const newRetryCount = schedule.retry_count + 1;
    const retryUntil    = schedule.retry_until ??
      new Date(now.getTime() + schedule.retry_window_seconds * 1000).toISOString();
    const interval      = calcRetryInterval(
      newRetryCount,
      schedule.retry_interval_seconds,
      schedule.retry_interval_max_seconds
    );
    const failErrors = results
      .filter(r => r.error)
      .map(r => `[${r.account_id}] ${r.error}`)
      .join("; ");

    console.warn(
      `[schedule] ⚠ ${schedule.id}: ${retryableFails.length} falha(s) retryável(eis), ` +
      `${permanentFails.length} permanente(s). Retry #${newRetryCount} em ~${interval}s`
    );

    await supabase.from("schedules").update({
      retry_until:         retryUntil,
      retry_count:         newRetryCount,
      last_attempt_at:     nowISO,
      last_attempt_status: "retrying",
      last_attempt_error:  failErrors || null,
    }).eq("id", schedule.id);

    const retryAt = new Date(now.getTime() + interval * 1000);
    if (retryAt < new Date(retryUntil)) {
      scheduleTimer(schedule.id, retryAt.toISOString(), resolvedGroupType, groupId);
    }
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   DISPARO DE SCHEDULE (grupos abertos + fallback fechados sem sniper)
   ───────────────────────────────────────────────────────────────────────────── */
async function fireSchedule(scheduleId: string): Promise<void> {
  if (sniperFiringNow.has(scheduleId)) {
    console.warn(`[fire] Schedule ${scheduleId} em execução no sniper — ignorando fireSchedule`);
    return;
  }

  if (firingNow.has(scheduleId)) {
    console.warn(`[fire] Schedule ${scheduleId} já em execução — ignorando disparo duplicado`);
    return;
  }
  firingNow.add(scheduleId);

  try {
    const now = new Date();

    let schedule = schedulePrefetchCache.get(scheduleId);
    if (schedule) {
      schedulePrefetchCache.delete(scheduleId);
      console.log(`[fire] ⚡ Schedule ${scheduleId} servido do pre-fetch cache`);
    } else {
      const { data, error } = await supabase
        .from("schedules")
        .select(SCHEDULE_SELECT)
        .eq("id", scheduleId)
        .eq("is_active", true)
        .single();

      if (error || !data) {
        console.warn(`[fire] Schedule ${scheduleId} não encontrado ou inativo.`);
        return;
      }
      schedule = data as unknown as Schedule;
    }

    const group = schedule.groups;

    if (!group?.telegram_chat_id) {
      console.warn(`[fire] Schedule ${scheduleId}: sem telegram_chat_id — pulando.`);
      return;
    }

    if (group.group_members) {
      group.group_members = group.group_members.map(m => ({
        ...m,
        accounts: m.accounts ? (accountCache.get(m.accounts.id) ?? m.accounts) : null,
      }));
    }

    console.log(`[fire] ⚡ Disparando schedule ${scheduleId} às ${now.toISOString()}`);

    if (group.group_type === "open") {
      if (listenMap.has(group.id)) {
        console.log(`[fire] Listener já ativo para grupo ${group.id} — ignorando`);
        return;
      }

      const firstAccount = (group.group_members ?? [])
        .filter(m => m.is_active && m.accounts?.is_active)
        .sort((a, b) => a.position - b.position)[0]?.accounts ?? null;

      if (!firstAccount) {
        console.warn(`[fire] Nenhuma conta ativa no grupo — abortando.`);
        return;
      }

      startGroupListener(schedule, group, firstAccount as Account);

      await supabase.from("schedules").update({
        retry_until:         new Date(now.getTime() + OPEN_GROUP_LISTEN_TIMEOUT_MS).toISOString(),
        last_attempt_at:     now.toISOString(),
        last_attempt_status: "waiting_admin",
        last_attempt_error:  null,
      }).eq("id", scheduleId);
      return;
    }

    // Grupo fechado sem sniper (retry via banco, boot tardio, etc.)
    const alreadySent = schedule.retry_until
      ? await getAlreadySentIds(schedule)
      : new Set<string>();

    if (alreadySent.size > 0) {
      console.log(`[dedup] ${alreadySent.size} account(s) já enviaram neste ciclo — pulando.`);
    }

    const results = await dispatchToGroup(schedule, group, alreadySent);

    const sentForMonitor = results
      .filter(r => r.status === "sent")
      .map(r => ({ account_id: r.account_id, message_text: r.message_text ?? "" }))
      .filter(r => r.message_text);
    if (sentForMonitor.length > 0) {
      monitorPositions(
        group.telegram_chat_id,
        sentForMonitor,
        scheduleId,
        now,
        group.group_type ?? "closed"
      ).catch(err => console.error("[monitor] Erro não capturado:", err.message));
    }

    await updateScheduleAfterDispatch(schedule, results, now, group.group_type);

  } finally {
    firingNow.delete(scheduleId);
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   TIMER DE PRECISÃO + PRE-FETCH + SNIPER (v10: adaptive lead para opens_early)
   ───────────────────────────────────────────────────────────────────────────── */
function scheduleTimer(
  scheduleId: string,
  nextRunAt: string,
  groupType?: "open" | "closed",
  groupId?: string   // v10: usado para consultar o perfil do grupo
): void {
  const delay = new Date(nextRunAt).getTime() - Date.now();

  if (delay < -5_000) {
    console.warn(`[timer] Schedule ${scheduleId} ignorado — muito no passado (${nextRunAt})`);
    return;
  }

  // v10: mantém o mapa scheduleId → groupId para uso futuro
  if (groupId) scheduleGroupMap.set(scheduleId, groupId);

  const prev = scheduledTimers.get(scheduleId);
  if (prev) clearTimeout(prev);
  const prevPrefetch = prefetchTimers.get(scheduleId);
  if (prevPrefetch) { clearTimeout(prevPrefetch); prefetchTimers.delete(scheduleId); }
  const prevSniper = sniperTimers.get(scheduleId);
  if (prevSniper) { clearTimeout(prevSniper); sniperTimers.delete(scheduleId); }

  const effectiveDelay = Math.max(0, delay);

  // OPT #2: pre-fetch 800ms antes
  if (effectiveDelay > PREFETCH_BEFORE_MS) {
    const prefetchDelay = effectiveDelay - PREFETCH_BEFORE_MS;
    const pt = setTimeout(async () => {
      prefetchTimers.delete(scheduleId);
      try {
        const { data, error } = await supabase
          .from("schedules")
          .select(SCHEDULE_SELECT)
          .eq("id", scheduleId)
          .eq("is_active", true)
          .single();

        if (error || !data) {
          console.warn(`[prefetch] Schedule ${scheduleId} inativo ou removido — ignorando`);
          return;
        }

        const s = data as unknown as Schedule;
        if (s.groups?.group_members) {
          s.groups.group_members = s.groups.group_members.map(m => ({
            ...m,
            accounts: m.accounts ? (accountCache.get(m.accounts.id) ?? m.accounts) : null,
          }));
        }

        schedulePrefetchCache.set(scheduleId, s);
        console.log(`[prefetch] ✅ Schedule ${scheduleId} pré-carregado (${PREFETCH_BEFORE_MS}ms antes do fire)`);
      } catch (err: any) {
        console.warn(`[prefetch] Falha ao pré-carregar schedule ${scheduleId}: ${err.message}`);
      }
    }, prefetchDelay);
    prefetchTimers.set(scheduleId, pt);
  }

  // v10: sniper com lead adaptativo para grupos closed
  if (groupType === "closed") {
    // Para grupos opens_early com perfil suficiente, inicia o sniper mais cedo.
    // O extra lead garante que o loop já está rodando quando o grupo abrir,
    // mesmo que seja muito antes do horário nominal.
    const resolvedGroupId = groupId ?? scheduleGroupMap.get(scheduleId);
    const profile         = resolvedGroupId ? groupProfileCache.get(resolvedGroupId) : undefined;
    const hasSufficientData = (profile?.sample_count ?? 0) >= 3;

    // Extra lead: abs(p10) + 100ms de margem para o sniper estar pronto
    // Exemplo: p10 = -500ms → extra = 600ms → sniper inicia 700ms antes do horário
    const extraLeadMs = (hasSufficientData && profile!.opens_early)
      ? Math.max(0, -profile!.offset_p10_ms) + 100
      : 0;

    const totalSniperLeadMs = SNIPER_BEFORE_MS + extraLeadMs;

    if (effectiveDelay > totalSniperLeadMs) {
      const sniperDelay = effectiveDelay - totalSniperLeadMs;
      const st = setTimeout(async () => {
        sniperTimers.delete(scheduleId);
        const cached = schedulePrefetchCache.get(scheduleId);
        if (cached && cached.groups?.group_type !== "closed") {
          console.log(`[sniper] Schedule ${scheduleId} não é closed — pulando sniper`);
          return;
        }
        try {
          await sniperFireClosed(scheduleId);
        } catch (err) {
          console.error(`[sniper] Erro inesperado ao disparar ${scheduleId}:`, err);
        }
      }, sniperDelay);
      sniperTimers.set(scheduleId, st);

      if (extraLeadMs > 0) {
        console.log(
          `[sniper] ⏰ Sniper agendado (opens_early) — lead total=${totalSniperLeadMs}ms ` +
          `(base=${SNIPER_BEFORE_MS}ms + extra=${extraLeadMs}ms) em ${Math.round(sniperDelay / 1000)}s`
        );
      } else {
        console.log(`[sniper] ⏰ Sniper agendado para schedule ${scheduleId} em ${Math.round(sniperDelay / 1000)}s`);
      }
    }
  }

  const timer = setTimeout(async () => {
    scheduledTimers.delete(scheduleId);
    try {
      await fireSchedule(scheduleId);
    } catch (err) {
      console.error(`[timer] Erro inesperado ao disparar ${scheduleId}:`, err);
    }
  }, effectiveDelay);

  scheduledTimers.set(scheduleId, timer);

  const fireAt = new Date(Date.now() + effectiveDelay).toISOString();
  console.log(`[timer] ⏰ Schedule ${scheduleId} — dispara em ${Math.round(effectiveDelay / 1000)}s (${fireAt})`);
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
      .select("id, next_run_at, group_id, groups(id, group_type)")  // v10: group_id incluído
      .eq("is_active", true)
      .is("retry_until", null)
      .lte("next_run_at", lookaheadISO),

    supabase.from("schedules")
      .select(SCHEDULE_SELECT)
      .eq("is_active", true)
      .not("retry_until", "is", null)
      .gt("retry_until", nowISO),

    supabase.from("schedules")
      .select("id, cron_expression, group_id")
      .eq("is_active", true)
      .not("retry_until", "is", null)
      .lte("retry_until", nowISO),
  ]);

  await Promise.all((expiredRetries ?? []).map(async expired => {
    console.warn(`[reload] Schedule ${expired.id}: retry expirou sem sucesso. Avançando.`);

    const expGroupId = (expired as any).group_id as string | undefined;
    if (expGroupId) {
      const ctrl = listenMap.get(expGroupId);
      if (ctrl) {
        ctrl.abort();
        listenMap.delete(expGroupId);
        console.log(`[reload] Listener cancelado para grupo ${expGroupId}`);
      }
    }

    let nextRun: string;
    try { nextRun = nextWeeklyOccurrence(expired.cron_expression); }
    catch {
      await supabase.from("schedules").update({ is_active: false }).eq("id", expired.id);
      return;
    }

    await supabase.from("schedules").update({
      next_run_at:         nextRun,
      last_run_at:         nowISO,
      retry_until:         null,
      retry_count:         0,
      last_attempt_at:     nowISO,
      last_attempt_status: "failed",
      last_attempt_error:  "Retry expirou sem sucesso total",
    }).eq("id", expired.id);
    scheduleTimer(expired.id, nextRun, undefined, expGroupId);
  }));

  for (const s of futureSchedules ?? []) {
    if (!scheduledTimers.has(s.id)) {
      const gType   = (s as any).groups?.group_type as "open" | "closed" | undefined;
      const gId     = (s as any).group_id as string | undefined ?? (s as any).groups?.id as string | undefined;
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
      console.log(`[reload] Schedule ${schedule.id} em retry — disparando agora.`);
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
      console.warn(`[reconnect] ${account.phone_number} offline — reconectando`);
      reconnectPromises.push(
        getClient(account)
          .then(() => console.log(`[reconnect] ✓ ${account.phone_number} reconectado`))
          .catch(err => console.warn(`[reconnect] Falha ao reconectar ${account.phone_number}: ${err.message}`))
      );
    }
  }

  if (reconnectPromises.length > 0) {
    await Promise.allSettled(reconnectPromises);
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   PRE-WARM DE CONTAS + PERFIS (v10: loadGroupProfiles no boot)
   ───────────────────────────────────────────────────────────────────────────── */
let prewarmRunning = false;
async function prewarmAccounts(): Promise<void> {
  if (prewarmRunning) return;
  prewarmRunning = true;
  try {
    const { data, error } = await supabase
      .from("accounts")
      .select("id, name, phone_number, api_id, api_hash, session_string, is_active")
      .eq("is_active", true);

    if (error) { console.warn("[prewarm] Falha ao buscar contas:", error.message); return; }

    const accounts = (data ?? []) as Account[];
    for (const account of accounts) accountCache.set(account.id, account);

    await Promise.allSettled(accounts.map(async account => {
      try {
        const client = await getClient(account);
        await client.getDialogs({ limit: 100 });
        console.log(`[prewarm] ✓ Dialogs prontos: ${account.phone_number}`);
      } catch (err: any) {
        const authDead =
          err.message?.includes("AUTH_KEY_UNREGISTERED") ||
          err.message?.includes("USER_DEACTIVATED")      ||
          err.message?.includes("SESSION_REVOKED");
        if (authDead) {
          console.warn(`[prewarm] Sessão morta: ${account.phone_number} — desativando.`);
          await supabase.from("accounts").update({ is_active: false }).eq("id", account.id);
        } else {
          console.warn(`[prewarm] Falha ao conectar ${account.phone_number}: ${err.message}`);
        }
      }
    }));

    // OPT #3: pre-resolve de peers para todos os grupos ativos
    try {
      const { data: groups } = await supabase
        .from("groups")
        .select("telegram_chat_id, group_members(accounts(id))")
        .not("telegram_chat_id", "is", null)
        .eq("group_members.is_active", true);

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

          resolvePromises.push(
            resolvePeer(cl, chatId, acc.id)
              .then(() => console.log(`[prewarm] ✓ Peer pré-resolvido: ${chatId} via ${acc.phone_number}`))
              .catch(() => {})
          );
        }
      }

      await Promise.allSettled(resolvePromises);
      console.log(`[prewarm] ✓ Pre-resolve de peers concluído (${resolvePromises.length} entradas)`);
    } catch (err: any) {
      console.warn(`[prewarm] Falha no pre-resolve de peers: ${err.message}`);
    }

    // v10: carrega perfis de timing no boot
    await loadGroupProfiles();

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
        .map(d => ({
          id:         String(d.id),
          name:       d.title ?? d.name ?? "Sem nome",
          type:       d.isChannel ? "channel" : "group",
          accessHash: null,
        }))
        .sort((a, b) => a.name.localeCompare(b.name));
      return jsonResponse(res, 200, chats);
    } catch (err: any) {
      console.error("[http] /chats erro:", err.message);
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
          new Api.channels.GetFullChannel({
            channel: new Api.InputChannel({ channelId: bigInt(rawId), accessHash: bigInt(0) }),
          })
        ) as any;
        if (typeof result?.fullChat?.participantsCount === "number") {
          count = result.fullChat.participantsCount;
        }
      } catch {}

      if (count === null) {
        try {
          const dialogs = await client.getDialogs({ limit: 500 });
          const absRaw  = rawId.replace(/^100/, "");
          const dialog  = dialogs.find(d => {
            const s = String(d.id).replace(/^-/, "");
            return s === rawId || s === absRaw ||
                   String(d.id) === chatId ||
                   `-100${s}` === chatId ||
                   `-${s}` === chatId;
          });
          if (dialog?.entity) {
            const ent = dialog.entity as any;
            count = typeof ent.participantsCount === "number" ? ent.participantsCount : null;
          }
          if (count === null && dialog) {
            const p = (dialog as any).participantsCount;
            if (typeof p === "number") count = p;
          }
        } catch {}
      }

      if (count === null) {
        try {
          const full = await client.invoke(
            new Api.messages.GetFullChat({ chatId: bigInt(rawId.replace(/^100/, "")) })
          ) as any;
          if (typeof full?.fullChat?.participantsCount === "number") {
            count = full.fullChat.participantsCount;
          } else if (full?.fullChat?.participants?.participants) {
            count = full.fullChat.participants.participants.length;
          }
        } catch {}
      }

      return jsonResponse(res, 200, { count });
    } catch (err: any) {
      console.error("[http] /chat-count erro:", err.message);
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
                filter:  new Api.ChannelParticipantsRecent(),
                offset: 0, limit: 200, hash: bigInt(0),
              })
            );
            if (result.className === "channels.ChannelParticipants") {
              members = result.users
                .filter((u): u is Api.User => u.className === "User" && !u.bot)
                .map(u => ({
                  id:       String(u.id),
                  name:     [u.firstName, u.lastName].filter(Boolean).join(" ") || null,
                  username: u.username ? `@${u.username}` : null,
                  phone:    u.phone ? `+${u.phone}` : null,
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
                  id:       String(u.id),
                  name:     [u.firstName, u.lastName].filter(Boolean).join(" ") || null,
                  username: u.username ? `@${u.username}` : null,
                  phone:    u.phone ? `+${u.phone}` : null,
                };
              })
              .filter((m): m is MemberOut => m !== null);
          }
        } catch {}
      }

      members.sort((a, b) => (a.name ?? "").localeCompare(b.name ?? ""));
      return jsonResponse(res, 200, members);
    } catch (err: any) {
      console.error("[http] /chat-members erro:", err.message);
      return jsonResponse(res, 500, { error: err.message });
    }
  }

  const reloadMatch = url.pathname.match(/^\/accounts\/([^/]+)\/reload$/);
  if (req.method === "POST" && reloadMatch) {
    const accountId = reloadMatch[1];
    const { data: row, error } = await supabase
      .from("accounts")
      .select("id, name, phone_number, api_id, api_hash, session_string, is_active")
      .eq("id", accountId)
      .single();

    if (error || !row) return jsonResponse(res, 404, { error: "Conta não encontrada" });

    const account = row as Account;
    accountCache.set(accountId, account);

    if (!account.is_active || !account.session_string) {
      return jsonResponse(res, 200, { ok: true, skipped: true, reason: "conta inativa ou sem sessão" });
    }

    try {
      const client = await reloadClient(account);
      await client.getDialogs({ limit: 100 });
      console.log(`[http] /reload ✓ ${account.phone_number} recarregada`);
      return jsonResponse(res, 200, { ok: true });
    } catch (err: any) {
      console.error("[http] /reload erro:", err.message);
      return jsonResponse(res, 500, { error: err.message });
    }
  }

  // v10: GET /groups/:id/profile — consulta o perfil de timing de um grupo
  const profileMatch = url.pathname.match(/^\/groups\/([^/]+)\/profile$/);
  if (req.method === "GET" && profileMatch) {
    const groupId = profileMatch[1];
    const profile = groupProfileCache.get(groupId) ?? null;
    return jsonResponse(res, 200, { profile });
  }

  // v10: DELETE /groups/:id/profile — reseta o perfil de um grupo (force relearn)
  if (req.method === "DELETE" && profileMatch) {
    const groupId = profileMatch[1];
    groupProfileCache.delete(groupId);
    await supabase.from("group_profiles").delete().eq("group_id", groupId);
    await supabase.from("group_dispatch_samples").delete().eq("group_id", groupId);
    console.log(`[profiles] 🗑 Perfil resetado para grupo ${groupId}`);
    return jsonResponse(res, 200, { ok: true, message: "Perfil resetado — próximos 3 disparos reaprendem o timing" });
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
            .select(`
              id, telegram_chat_id, group_type,
              group_members(id, message_text, position, is_active,
                accounts(id, name, phone_number, api_id, api_hash, session_string, is_active))
            `)
            .eq("id", groupId)
            .single();

          if (!grpRow) { console.warn(`[listen-manual] Grupo ${groupId} não encontrado`); return; }

          const chatId  = String(grpRow.telegram_chat_id);
          const members: GroupMember[] = (grpRow.group_members ?? []).map((m: any) => ({
            ...m,
            accounts: Array.isArray(m.accounts) ? (m.accounts[0] ?? null) : (m.accounts ?? null),
          }));

          const firstMember = members.find(m => m.is_active && m.accounts?.is_active);
          if (!firstMember?.accounts) {
            console.warn(`[listen-manual] Sem conta ativa em ${groupId}`);
            return;
          }

          const account = accountCache.get(firstMember.accounts.id) ?? firstMember.accounts as unknown as Account;
          const client  = await getClient(account);

          const deadline    = Date.now() + 2 * 60 * 60_000;
          const startUnix   = Math.floor((Date.now() - 10_000) / 1000);
          let lastSeenMsgId = 0;

          try { await resolvePeer(client, chatId, account.id); } catch {}
          console.log(`[listen-manual] 👂 Aguardando OK em ${chatId} (grupo ${groupId})`);

          while (Date.now() < deadline && !ctrl.signal.aborted) {
            try {
              const peer   = await resolvePeer(client, chatId, account.id);
              const result = await client.invoke(
                new Api.messages.GetHistory({
                  peer: peer as any, limit: 10,
                  offsetDate: 0, offsetId: 0, maxId: 0, minId: 0,
                  hash: bigInt(0), addOffset: 0,
                })
              ) as any;

              const recentMsgs = (result.messages ?? []).filter(
                (m: any) =>
                  (m.className === "Message" || m._ === "message") &&
                  m.date >= startUnix &&
                  m.id > lastSeenMsgId
              );
              if (recentMsgs.length > 0) {
                lastSeenMsgId = Math.max(lastSeenMsgId, ...recentMsgs.map((m: any) => m.id as number));
              }

              const gotSignal = recentMsgs.some((m: any) => {
                const text = typeof m.message === "string" ? m.message.trim().toLowerCase() : "";
                return text === "ok" || (m.media != null && m.media.className !== "MessageMediaEmpty");
              });

              if (gotSignal && !ctrl.signal.aborted) {
                console.log(`[listen-manual] ✓ Sinal detectado para grupo ${groupId}`);
                listenMap.delete(groupId);
                await supabase.from("groups").update({ listener_session_id: null }).eq("id", groupId);

                const { data: grpFull } = await supabase
                  .from("groups")
                  .select("name, telegram_chat_name, user_id")
                  .eq("id", groupId)
                  .single();

                const scheduleStub = {
                  id:                         `manual-${groupId}-${Date.now()}`,
                  user_id:                    grpFull?.user_id ?? "",
                  group_id:                   groupId,
                  cron_expression:            "0 0 * * 0",
                  next_run_at:                new Date().toISOString(),
                  retry_window_seconds:       60,
                  retry_interval_seconds:     5,
                  retry_interval_max_seconds: 30,
                  retry_count:                0,
                  retry_until:                null,
                  last_attempt_at:            null,
                  groups: {
                    id:                 groupId,
                    name:               grpFull?.name ?? groupId,
                    telegram_chat_id:   chatId,
                    telegram_chat_name: grpFull?.telegram_chat_name ?? null,
                    group_type:         "open" as const,
                    group_members:      members,
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

                const sent = results.filter(r => r.status === "sent").length;
                console.log(`[listen-manual] ✓ ${sent} mensagem(ns) enviada(s) para grupo ${groupId}`);
                return;
              }
            } catch (err: any) {
              if (!ctrl.signal.aborted) await new Promise(r => setTimeout(r, 2_000));
            }

            if (!ctrl.signal.aborted) await new Promise(r => setTimeout(r, LISTEN_POLL_MS));
          }

          if (!ctrl.signal.aborted) {
            await supabase.from("groups").update({ listener_session_id: null }).eq("id", groupId);
          }
          listenMap.delete(groupId);

        } catch (err: any) {
          console.error(`[listen-manual] Erro inesperado para grupo ${groupId}:`, err.message);
          listenMap.delete(groupId);
        }
      })();

      return jsonResponse(res, 200, { ok: true });
    }
  }

  // POST /groups/:id/dispatch
  const dispatchMatch = url.pathname.match(/^\/groups\/([^/]+)\/dispatch$/);
  if (req.method === "POST" && dispatchMatch) {
    const groupId = dispatchMatch[1];

    let body: { user_id?: string; send_to_self?: boolean } = {};
    try {
      const raw = await new Promise<string>((resolve, reject) => {
        let data = "";
        req.on("data", chunk => { data += chunk; });
        req.on("end",  () => resolve(data));
        req.on("error", reject);
      });
      if (raw) body = JSON.parse(raw);
    } catch {}

    const sendToSelf = !!body.send_to_self;

    try {
      const { data, error } = await supabase
        .from("groups")
        .select(`
          id, name, telegram_chat_id, telegram_chat_name, group_type,
          group_members(
            id, message_text, position, is_active,
            accounts(id, name, phone_number, api_id, api_hash, session_string, is_active)
          )
        `)
        .eq("id", groupId)
        .single();

      if (error || !data) {
        return jsonResponse(res, 404, { error: "Grupo não encontrado" });
      }

      const group = data as unknown as Group;
      const members = (group.group_members ?? [])
        .filter(m => m.is_active && m.accounts?.is_active && m.accounts?.session_string)
        .sort((a, b) => a.position - b.position);

      if (members.length === 0) {
        return jsonResponse(res, 200, { ok: true, sent: 0, failed: 0, results: [] });
      }

      let sent = 0, failed = 0;
      const results: Array<{ account_id: string; status: string; error?: string }> = [];

      for (const member of members) {
        const account = member.accounts
          ? (accountCache.get(member.accounts.id) ?? member.accounts as unknown as Account)
          : null;
        if (!account) continue;

        try {
          const client = await getClient(account);
          const text = member.message_text ?? "";

          if (sendToSelf) {
            const stableId = makeRandomId();
            await Promise.race([
              client.invoke(new Api.messages.SendMessage({
                peer:      new Api.InputPeerSelf(),
                message:   text || "[teste de aquecimento]",
                randomId:  stableId,
                noWebpage: true,
              })),
              new Promise<never>((_, r) => setTimeout(() => r(new Error("TIMEOUT")), SEND_TIMEOUT_MS)),
            ]);
          } else {
            if (!group.telegram_chat_id) throw new Error("telegram_chat_id não configurado");
            await sendMessage(client, account, String(group.telegram_chat_id), text);
          }

          sent++;
          results.push({ account_id: account.id, status: "sent" });
          console.log(`[dispatch-http] ✓ ${account.phone_number}${sendToSelf ? " (self)" : ""}`);
        } catch (err: any) {
          failed++;
          results.push({ account_id: account.id, status: "failed", error: err.message });
          console.error(`[dispatch-http] ✗ ${account?.phone_number}: ${err.message}`);
        }
      }

      return jsonResponse(res, 200, { ok: true, sent, failed, results });
    } catch (err: any) {
      console.error("[dispatch-http] Erro inesperado:", err.message);
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
    console.log(`[client] Desconectado: ${id}`);
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
  console.log("[worker] Iniciando v10 — Adaptive Gate...");
  await prewarmAccounts();   // inclui loadGroupProfiles() no boot
  await reloadSchedules();
  setInterval(async () => {
    try {
      await Promise.allSettled([
        reloadSchedules(),
        reconnectDeadClients(),
      ]);
    } catch (err) {
      console.error("[reload] Erro no reload periódico:", err);
    }
  }, RELOAD_INTERVAL_MS);
  console.log("[worker] Pronto. Adaptive Gate ativo.");
}

init().catch(err => {
  console.error("[worker] Falha na inicialização:", err);
  process.exit(1);
});

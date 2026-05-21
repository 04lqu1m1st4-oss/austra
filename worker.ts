// worker.ts — high-precision Telegram dispatch worker
// v12 — PUSH > POLL EDITION
//
// O gargalo real era GetHistory ter RTT de 80-150ms pro DC do Telegram.
// Polling a 5ms não adianta se cada chamada demora 100ms pra voltar.
//
// Solução: addEventHandler (update push) — o Telegram EMPURRA a mensagem
// pro cliente em <5ms assim que ela chega no servidor. Zero polling.
//
// Arquitetura de pools separadas:
//   • listenerPool — clientes com update loop ATIVO, usados só pra escutar
//   • clientPool   — clientes sem update loop, usados só pra enviar
//
// Isso evita AUTH_KEY_DUPLICATED (duas sessões ativas = duas conexões MTProto
// com a mesma auth_key) porque cada conta tem exatamente UMA conexão.
// A mesma conexão do listenerPool é usada pro envio quando chega o sinal.
//
// Fallback: se push falhar (erro de update loop), volta pra polling 5ms.

import { createClient }          from "@supabase/supabase-js";
import { TelegramClient, Api }   from "telegram";
import { NewMessage }            from "telegram/events";
import { NewMessageEvent }       from "telegram/events/NewMessage";
import { StringSession }         from "telegram/sessions";
import bigInt                    from "big-integer";
import http                      from "http";

/* ─── Supabase ─── */
const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

/* ─── Constantes ─── */
const SEND_TIMEOUT_MS           = 3_000;
const RETRY_BUDGET_MS           = 50_000;
const RELOAD_INTERVAL_MS        = 30_000;
const LOOKAHEAD_MS              = 2 * 60 * 1000;
const KEEPALIVE_INTERVAL_MS     = 20_000;
const PREFETCH_BEFORE_MS        = 15_000;

// Monitoramento de posição
const MONITOR_DELAY_CLOSED_MS      = 2_000;
const MONITOR_MAX_OPEN_MS          = 5 * 60_000;
const MONITOR_POLL_MS              = 5;       // fallback poll
const LISTEN_POLL_MS               = 5;       // fallback poll
const MONITOR_HISTORY_LIMIT        = 150;
const OPEN_GROUP_LISTEN_TIMEOUT_MS = 2 * 60 * 60_000;

/* ─── Instance lock ─── */
const INSTANCE_ID = `${Date.now()}-${Math.random().toString(36).slice(2)}`;
const LOCK_KEY    = "worker_instance_lock";
const LOCK_TTL_MS = 20_000;

async function acquireInstanceLock(): Promise<boolean> {
  try {
    const { data, error } = await supabase
      .from("worker_locks")
      .upsert(
        { key: LOCK_KEY, instance_id: INSTANCE_ID, updated_at: new Date().toISOString() },
        { onConflict: "key" }
      )
      .select("instance_id")
      .single();
    if (error) { console.warn("[lock] Tabela worker_locks não encontrada — sem lock"); return true; }
    return data?.instance_id === INSTANCE_ID;
  } catch { return true; }
}

async function renewInstanceLock(): Promise<void> {
  try {
    await supabase
      .from("worker_locks")
      .update({ instance_id: INSTANCE_ID, updated_at: new Date().toISOString() })
      .eq("key", LOCK_KEY).eq("instance_id", INSTANCE_ID);
  } catch {}
}

async function checkInstanceLock(): Promise<boolean> {
  try {
    const { data } = await supabase
      .from("worker_locks").select("instance_id, updated_at").eq("key", LOCK_KEY).single();
    if (!data) return true;
    if (data.instance_id !== INSTANCE_ID) {
      const age = Date.now() - new Date(data.updated_at).getTime();
      if (age < LOCK_TTL_MS) {
        console.warn(`[lock] Outra instância ativa (${data.instance_id}) — encerrando ${INSTANCE_ID}`);
        return false;
      }
      await acquireInstanceLock();
    }
    return true;
  } catch { return true; }
}

/* ─── Tipos ─── */
interface Account {
  id: string; name: string; phone_number: string;
  api_id: string; api_hash: string; session_string: string; is_active: boolean;
}
interface GroupMember {
  id: string; message_text: string | null; position: number;
  is_active: boolean; accounts: Account | null;
}
interface Group {
  id: string; name: string; telegram_chat_id: string | null;
  telegram_chat_name: string | null; group_type: "open" | "closed"; group_members: GroupMember[];
}
interface Schedule {
  id: string; cron_expression: string; user_id: string; group_id: string;
  next_run_at: string; retry_window_seconds: number; retry_interval_seconds: number;
  retry_interval_max_seconds: number; retry_count: number;
  retry_until: string | null; last_attempt_at: string | null; groups: Group;
}
interface MemberResult {
  member_id: string; account_id: string; position_rank: number;
  status: "sent" | "failed" | "skipped"; retryable: boolean; error?: string;
}

/* ─── Caches globais ─── */
const peerCache             = new Map<string, unknown>();
const accountCache          = new Map<string, Account>();
const schedulePrefetchCache = new Map<string, Schedule>();
const prefetchTimers        = new Map<string, ReturnType<typeof setTimeout>>();
const scheduledTimers       = new Map<string, ReturnType<typeof setTimeout>>();

/* ─── Query reutilizada ─── */
const SCHEDULE_SELECT = `
  id, cron_expression, user_id, group_id, next_run_at,
  retry_window_seconds, retry_interval_seconds, retry_interval_max_seconds,
  retry_count, retry_until, last_attempt_at,
  groups(id, name, telegram_chat_id, telegram_chat_name, group_type,
    group_members(id, message_text, position, is_active,
      accounts(id, name, phone_number, api_id, api_hash, session_string, is_active)))
`.trim();

/* ─── Resolve peer ─── */
async function getOrResolvePeer(
  client: TelegramClient, telegramChatId: string, accountId: string
): Promise<unknown> {
  const cacheKey = `${accountId}:${telegramChatId}`;
  if (peerCache.has(cacheKey)) return peerCache.get(cacheKey)!;

  const chatIdNum = parseInt(telegramChatId, 10);
  if (isNaN(chatIdNum)) throw new Error(`telegram_chat_id inválido: "${telegramChatId}"`);

  try {
    const peer = await client.getInputEntity(chatIdNum);
    peerCache.set(cacheKey, peer);
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
      peerCache.set(cacheKey, peer);
      return peer;
    }
  } catch {}

  try {
    await client.getDialogs({ limit: 200 });
    const peer = await client.getInputEntity(chatIdNum);
    peerCache.set(cacheKey, peer);
    return peer;
  } catch (e3: any) {
    throw new Error(`PEER_UNRESOLVABLE ${telegramChatId}: ${e3.message}`);
  }
}

async function prewarmPeersForAccounts(accounts: Account[], chatId: string): Promise<void> {
  await Promise.allSettled(
    accounts.map(async (account) => {
      try {
        const client = await clientPool.get(account);
        await getOrResolvePeer(client, chatId, account.id);
        console.log(`[peer-prewarm] ✓ ${account.phone_number} → ${chatId}`);
      } catch (err: any) {
        console.warn(`[peer-prewarm] ✗ ${account.phone_number}: ${err.message}`);
      }
    })
  );
}

/* ══════════════════════════════════════════════════════════════════════════
   POOL DE ENVIO — update loop DESATIVADO
   Usado exclusivamente para sendMessage. Sem recepção de updates.
   ══════════════════════════════════════════════════════════════════════════ */
class TelegramClientPool {
  private clients            = new Map<string, TelegramClient>();
  private sessions           = new Map<string, string>();
  private keepaliveTimers    = new Map<string, ReturnType<typeof setInterval>>();
  private connectingPromises = new Map<string, Promise<TelegramClient>>();

  private startKeepalive(accountId: string, client: TelegramClient): void {
    const existing = this.keepaliveTimers.get(accountId);
    if (existing) clearInterval(existing);
    const interval = setInterval(async () => {
      if (!client.connected) { this._evict(accountId, interval); return; }
      try {
        await Promise.race([
          client.getMe(),
          new Promise<never>((_, r) => setTimeout(() => r(new Error("timeout")), 5_000)),
        ]);
      } catch (err: any) {
        try { await client.disconnect(); } catch {}
        this._evict(accountId, interval);
        const authDead =
          err.message?.includes("AUTH_KEY_UNREGISTERED") ||
          err.message?.includes("USER_DEACTIVATED") ||
          err.message?.includes("SESSION_REVOKED");
        if (authDead) {
          supabase.from("accounts").update({ is_active: false }).eq("id", accountId).then(() => {});
        }
      }
    }, KEEPALIVE_INTERVAL_MS);
    this.keepaliveTimers.set(accountId, interval);
  }

  _evict(accountId: string, interval?: ReturnType<typeof setInterval>): void {
    if (interval) clearInterval(interval);
    this.keepaliveTimers.delete(accountId);
    this.clients.delete(accountId);
  }

  async get(account: Account): Promise<TelegramClient> {
    const existing     = this.clients.get(account.id);
    const sessionInUse = this.sessions.get(account.id);
    if (existing?.connected && sessionInUse === account.session_string) return existing;

    const inflight = this.connectingPromises.get(account.id);
    if (inflight) return inflight;

    const connectPromise = (async () => {
      if (existing) { try { await existing.disconnect(); } catch {} this._evict(account.id); }

      const client = new TelegramClient(
        new StringSession(account.session_string),
        parseInt(account.api_id), account.api_hash,
        { connectionRetries: 5, retryDelay: 50, autoReconnect: true, floodSleepThreshold: 60, requestRetries: 3 }
      );
      // Update loop DESATIVADO nesta pool — apenas envia, nunca recebe updates
      (client as any)._updateLoop = () => Promise.resolve();
      await client.connect();
      this.clients.set(account.id, client);
      this.sessions.set(account.id, account.session_string);
      this.startKeepalive(account.id, client);
      console.log(`[pool:send] Conectado: ${account.phone_number}`);
      return client;
    })();

    this.connectingPromises.set(account.id, connectPromise);
    try { return await connectPromise; }
    finally { this.connectingPromises.delete(account.id); }
  }

  async reload(account: Account): Promise<TelegramClient> {
    const existing = this.clients.get(account.id);
    if (existing) { try { await existing.disconnect(); } catch {} this._evict(account.id); }
    this.connectingPromises.delete(account.id);
    return this.get(account);
  }

  async prewarm(accounts: Account[]): Promise<void> {
    await Promise.allSettled(accounts.map((a) => this.get(a)));
  }

  async disconnectAll(): Promise<void> {
    for (const t of this.keepaliveTimers.values()) clearInterval(t);
    this.keepaliveTimers.clear();
    await Promise.all(
      [...this.clients.entries()].map(async ([, client]) => {
        try { await client.disconnect(); } catch {}
      })
    );
    this.clients.clear();
  }
}

/* ══════════════════════════════════════════════════════════════════════════
   POOL DE ESCUTA — update loop ATIVO
   Clientes dedicados que recebem updates push do Telegram.
   Uma conta = uma conexão = um update loop = zero conflito de auth_key.
   O mesmo client é reutilizado para o envio quando o sinal chega,
   evitando qualquer latência de setup no momento crítico.
   ══════════════════════════════════════════════════════════════════════════ */
class TelegramListenerPool {
  private clients            = new Map<string, TelegramClient>();
  private sessions           = new Map<string, string>();
  private keepaliveTimers    = new Map<string, ReturnType<typeof setInterval>>();
  private connectingPromises = new Map<string, Promise<TelegramClient>>();

  private startKeepalive(accountId: string, client: TelegramClient): void {
    const existing = this.keepaliveTimers.get(accountId);
    if (existing) clearInterval(existing);
    const interval = setInterval(async () => {
      if (!client.connected) { this._evict(accountId, interval); return; }
      try {
        await Promise.race([
          client.getMe(),
          new Promise<never>((_, r) => setTimeout(() => r(new Error("timeout")), 5_000)),
        ]);
      } catch (err: any) {
        try { await client.disconnect(); } catch {}
        this._evict(accountId, interval);
      }
    }, KEEPALIVE_INTERVAL_MS);
    this.keepaliveTimers.set(accountId, interval);
  }

  _evict(accountId: string, interval?: ReturnType<typeof setInterval>): void {
    if (interval) clearInterval(interval);
    this.keepaliveTimers.delete(accountId);
    this.clients.delete(accountId);
  }

  async get(account: Account): Promise<TelegramClient> {
    const existing     = this.clients.get(account.id);
    const sessionInUse = this.sessions.get(account.id);
    if (existing?.connected && sessionInUse === account.session_string) return existing;

    const inflight = this.connectingPromises.get(account.id);
    if (inflight) return inflight;

    const connectPromise = (async () => {
      if (existing) { try { await existing.disconnect(); } catch {} this._evict(account.id); }

      const client = new TelegramClient(
        new StringSession(account.session_string),
        parseInt(account.api_id), account.api_hash,
        { connectionRetries: 5, retryDelay: 50, autoReconnect: true, floodSleepThreshold: 60, requestRetries: 3 }
      );
      // Update loop ATIVO — necessário para receber eventos push
      // NÃO suprime _updateLoop aqui
      await client.connect();
      this.clients.set(account.id, client);
      this.sessions.set(account.id, account.session_string);
      this.startKeepalive(account.id, client);
      console.log(`[pool:listen] Conectado com push: ${account.phone_number}`);
      return client;
    })();

    this.connectingPromises.set(account.id, connectPromise);
    try { return await connectPromise; }
    finally { this.connectingPromises.delete(account.id); }
  }

  async disconnectAll(): Promise<void> {
    for (const t of this.keepaliveTimers.values()) clearInterval(t);
    this.keepaliveTimers.clear();
    await Promise.all(
      [...this.clients.entries()].map(async ([, client]) => {
        try { await client.disconnect(); } catch {}
      })
    );
    this.clients.clear();
  }
}

const clientPool   = new TelegramClientPool();
const listenerPool = new TelegramListenerPool();

/* ─── Graceful shutdown ─── */
async function shutdown() {
  console.log("[worker] Encerrando...");
  for (const t of prefetchTimers.values()) clearTimeout(t);
  prefetchTimers.clear();
  for (const t of scheduledTimers.values()) clearTimeout(t);
  scheduledTimers.clear();
  httpServer.close();
  await Promise.all([clientPool.disconnectAll(), listenerPool.disconnectAll()]);
  process.exit(0);
}
process.on("SIGTERM", shutdown);
process.on("SIGINT",  shutdown);

/* ─── Helpers ─── */
function isRetryableError(msg: string): boolean {
  const u = msg.toUpperCase();
  return !u.includes("AUTH_KEY_UNREGISTERED") && !u.includes("USER_DEACTIVATED") && !u.includes("SESSION_REVOKED");
}

function nextWeeklyOccurrence(cronExpression: string): string {
  const parts = cronExpression.trim().split(/\s+/);
  const mi = parseInt(parts[0], 10), h = parseInt(parts[1], 10), dow = parseInt(parts[4], 10);
  if (parts.length < 5 || isNaN(mi) || isNaN(h) || isNaN(dow) ||
      mi < 0 || mi > 59 || h < 0 || h > 23 || dow < 0 || dow > 6)
    throw new Error(`cron_expression inválida: "${cronExpression}"`);
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
  const interval = calcRetryInterval(schedule.retry_count, schedule.retry_interval_seconds, schedule.retry_interval_max_seconds);
  return now >= new Date(last.getTime() + interval * 1000);
}

/* ─── Deduplicação ─── */
async function getAlreadySentAccountIds(schedule: Schedule): Promise<Set<string>> {
  const cycleStart = schedule.retry_until
    ? new Date(new Date(schedule.retry_until).getTime() - schedule.retry_window_seconds * 1000).toISOString()
    : schedule.next_run_at;
  const { data, error } = await supabase
    .from("dispatch_logs").select("account_id")
    .eq("schedule_id", schedule.id).eq("status", "sent").gte("sent_at", cycleStart);
  if (error) { console.warn(`[dedup] Falha:`, error.message); return new Set(); }
  return new Set((data ?? []).map((r) => r.account_id as string));
}

/* ─── Envio agressivo — retry a 5ms, timeout 3s ─── */
async function sendAggressively(
  client: TelegramClient, account: Account, telegramChatId: string, messageText: string
): Promise<void> {
  const budgetEnd = Date.now() + RETRY_BUDGET_MS;
  let attempt = 0;

  while (Date.now() < budgetEnd) {
    attempt++;
    const timeLeft = budgetEnd - Date.now();
    if (timeLeft < 500) break;

    try {
      await Promise.race([
        (async () => {
          const peer = await getOrResolvePeer(client, telegramChatId, account.id);
          try {
            await client.sendMessage(peer as any, { message: messageText });
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
                await new Promise((r) => setTimeout(r, waitMs));
                peerCache.delete(`${account.id}:${telegramChatId}`);
                const freshPeer = await getOrResolvePeer(client, telegramChatId, account.id);
                await client.sendMessage(freshPeer as any, { message: messageText });
                return;
              }
              throw new Error(`FLOOD_WAIT_${waitSecs}_EXCEEDS_BUDGET`);
            }
            throw err;
          }
        })(),
        new Promise<never>((_, r) => setTimeout(() => r(new Error(`TIMEOUT ${attempt}`)), Math.min(SEND_TIMEOUT_MS, timeLeft - 100))),
      ]);
      if (attempt > 1) console.log(`[retry] ✓ ${account.phone_number} na tentativa ${attempt}`);
      return;
    } catch (err: unknown) {
      const errMsg    = (err as any)?.message ?? String(err);
      const remaining = budgetEnd - Date.now();
      if (remaining > 500) console.warn(`[retry] tentativa ${attempt} (${Math.round(remaining / 1000)}s restantes): ${errMsg}`);
      // 5ms entre tentativas — mantém pressão máxima
      await new Promise((r) => setTimeout(r, 5));
    }
  }
  throw new Error(`BUDGET_EXCEEDED após ${attempt} tentativa(s)`);
}

/* ─── Tenta enviar um membro — log é fire-and-forget ─── */
async function trySendMember(
  member: GroupMember, account: Account, group: Group,
  schedule: Schedule, alreadySent: Set<string>, positionRank: number
): Promise<MemberResult> {
  if (alreadySent.has(account.id)) {
    return { member_id: member.id, account_id: account.id, position_rank: positionRank, status: "skipped", retryable: false };
  }

  let logStatus: "sent" | "failed" = "failed";
  let errorMsg: string | undefined;
  let retryable = false;

  try {
    // Usa o clientPool (sem update loop) para enviar
    const client = await clientPool.get(account);
    await sendAggressively(client, account, group.telegram_chat_id!, member.message_text ?? "");
    logStatus = "sent";
    alreadySent.add(account.id);
    console.log(`[worker] ✓ ${member.id} (${account.phone_number})`);
  } catch (err) {
    errorMsg  = err instanceof Error ? err.message : String(err);
    retryable = isRetryableError(errorMsg);
    console.error(`[worker] ✗ ${member.id} [${retryable ? "retryável" : "permanente"}] (${account.phone_number}): ${errorMsg}`);
  }

  // FIRE-AND-FORGET — não bloqueia o caminho crítico
  supabase.from("dispatch_logs").insert({
    user_id: schedule.user_id, group_id: group.id, account_id: account.id,
    schedule_id: schedule.id, status: logStatus, message_text: member.message_text,
    position_rank: positionRank, group_name_snapshot: group.name,
    chat_name_snapshot: group.telegram_chat_name,
    sent_at: logStatus === "sent" ? new Date().toISOString() : null,
    error_message: errorMsg ?? null,
  }).then(({ error: e }) => { if (e) console.error(`[log] Falha dispatch_log:`, e.message); });

  return { member_id: member.id, account_id: account.id, position_rank: positionRank, status: logStatus, retryable, error: errorMsg };
}

async function processMembersOf(schedule: Schedule, alreadySent: Set<string>): Promise<MemberResult[]> {
  const group   = schedule.groups;
  const members = (group.group_members ?? [])
    .filter((m) => m.is_active && m.accounts?.is_active && m.accounts?.session_string)
    .sort((a, b) => a.position - b.position);
  return Promise.all(members.map((m, i) => trySendMember(m, m.accounts!, group, schedule, alreadySent, i + 1)));
}

/* ─── Monitor de posições ─── */
async function monitorPositions(
  telegramChatId: string,
  sentMembers: Array<{ account_id: string; message_text: string }>,
  scheduleId: string, dispatchedAt: Date, groupType: "open" | "closed",
  allGroupAccounts: Account[]
): Promise<void> {
  if (sentMembers.length === 0) return;

  let client: TelegramClient | null = null;
  let monitorAccount: Account | null = null;
  for (const acc of allGroupAccounts) {
    const c = await clientPool.get(acc).catch(() => null);
    if (c) { client = c; monitorAccount = acc; break; }
  }
  if (!client || !monitorAccount) return;

  if (groupType === "closed") await new Promise((r) => setTimeout(r, MONITOR_DELAY_CLOSED_MS));

  const windowStartUnix = Math.floor((dispatchedAt.getTime() - 2 * 60_000) / 1000);
  const deadline        = Date.now() + (groupType === "closed" ? MONITOR_DELAY_CLOSED_MS + 15_000 : MONITOR_MAX_OPEN_MS);
  const ourTexts        = new Set(sentMembers.map((m) => m.message_text).filter(Boolean));

  while (Date.now() < deadline) {
    try {
      const peer   = await getOrResolvePeer(client, telegramChatId, monitorAccount.id);
      const result = await client.invoke(
        new Api.messages.GetHistory({
          peer: peer as any, limit: MONITOR_HISTORY_LIMIT,
          offsetDate: 0, offsetId: 0, maxId: 0, minId: 0, hash: bigInt(0), addOffset: 0,
        })
      ) as any;
      const windowMsgs = (result.messages ?? [])
        .filter((m: any) => m._ === "message" && m.date >= windowStartUnix).reverse();

      if (windowMsgs.length === 0) {
        if (groupType === "closed") return;
        await new Promise((r) => setTimeout(r, MONITOR_POLL_MS));
        continue;
      }
      if (groupType === "open" && !windowMsgs.some((m: any) => ourTexts.has(m.message))) {
        await new Promise((r) => setTimeout(r, MONITOR_POLL_MS));
        continue;
      }

      const cutoff  = new Date(dispatchedAt.getTime() - 60_000).toISOString();
      const updates = sentMembers
        .filter((sm) => sm.message_text)
        .map((sm) => {
          const idx = windowMsgs.findIndex((m: any) => m.message === sm.message_text);
          if (idx < 0) return null;
          console.log(`[monitor] ${sm.account_id}: #${idx + 1} em ${telegramChatId}`);
          return supabase.from("dispatch_logs")
            .update({ position_rank: idx + 1 })
            .eq("schedule_id", scheduleId).eq("account_id", sm.account_id)
            .eq("status", "sent").gte("sent_at", cutoff);
        }).filter(Boolean);

      await Promise.allSettled(updates);
      console.log(`[monitor] ✓ Posições salvas para schedule ${scheduleId}`);
      return;
    } catch (err: any) {
      if (groupType === "closed") return;
      await new Promise((r) => setTimeout(r, MONITOR_POLL_MS));
    }
  }
}

/* ─── Dispatch após sinal ─── */
async function dispatchAfterSignal(
  schedule: Schedule, group: Group, chatId: string,
  scheduleId: string, signalDetectedAt: Date
): Promise<void> {
  const alreadySent = await getAlreadySentAccountIds(schedule);
  const results     = await processMembersOf(schedule, alreadySent);

  const allGroupAccounts = (group.group_members ?? [])
    .filter((m) => m.is_active && m.accounts?.is_active)
    .map((m) => accountCache.get(m.accounts!.id) ?? m.accounts!);

  const sentForMonitor = results
    .filter((r) => r.status === "sent")
    .map((r) => {
      const member = (group.group_members ?? []).find((m) => m.accounts?.id === r.account_id);
      return { account_id: r.account_id, message_text: member?.message_text ?? "" };
    }).filter((r) => r.message_text);

  if (sentForMonitor.length > 0) {
    monitorPositions(chatId, sentForMonitor, scheduleId, signalDetectedAt, "open", allGroupAccounts)
      .catch((err) => console.error("[dispatch] Erro no monitoramento:", err.message));
  }

  const sentCount         = results.filter((r) => r.status === "sent").length;
  const skippedCount      = results.filter((r) => r.status === "skipped").length;
  const retryableFailures = results.filter((r) => r.status === "failed" && r.retryable);
  const permanentFailures = results.filter((r) => r.status === "failed" && !r.retryable);
  const allSucceeded      =
    (group.group_members ?? []).some((m) => m.is_active && m.accounts?.is_active) &&
    retryableFailures.length === 0 && permanentFailures.length === 0 &&
    (sentCount + skippedCount) > 0;
  const nowISO = new Date().toISOString();

  if (allSucceeded) {
    let nextRun: string;
    try { nextRun = nextWeeklyOccurrence(schedule.cron_expression); }
    catch (err) { await supabase.from("schedules").update({ is_active: false }).eq("id", scheduleId); return; }
    await supabase.from("schedules").update({
      next_run_at: nextRun, last_run_at: nowISO, retry_until: null, retry_count: 0,
      last_attempt_at: nowISO, last_attempt_status: "sent", last_attempt_error: null,
    }).eq("id", scheduleId);
    scheduleTimer(scheduleId, nextRun);
  } else {
    const newRetryCount = schedule.retry_count + 1;
    const retryUntil    = new Date(Date.now() + schedule.retry_window_seconds * 1000).toISOString();
    await supabase.from("schedules").update({
      retry_until: retryUntil, retry_count: newRetryCount, last_attempt_at: nowISO,
      last_attempt_status: "retrying",
      last_attempt_error: results.filter((r) => r.status === "failed" && r.error).map((r) => `[${r.account_id}] ${r.error}`).join("; ") || null,
    }).eq("id", scheduleId);
    const intervalNext = calcRetryInterval(newRetryCount, schedule.retry_interval_seconds, schedule.retry_interval_max_seconds);
    const retryAt = new Date(Date.now() + intervalNext * 1000);
    if (retryAt < new Date(retryUntil)) scheduleTimer(scheduleId, retryAt.toISOString());
  }
}

/* ══════════════════════════════════════════════════════════════════════════
   LISTENER PUSH — o coração do v12
   
   Estratégia:
   1. Conecta cada conta via listenerPool (update loop ativo)
   2. Registra addEventHandler filtrando pelo chatId do grupo
   3. O Telegram envia o update em <5ms quando a mensagem chega
   4. O handler dispara o envio imediatamente, sem nenhum poll
   
   Fallback automático:
   Se o addEventHandler falhar (sessão sem permissão de updates, etc.),
   cai automaticamente no polling agressivo de 5ms como backup.
   ══════════════════════════════════════════════════════════════════════════ */
function startScheduledGroupListener(schedule: Schedule, group: Group): void {
  const groupId    = group.id;
  const chatId     = group.telegram_chat_id!;
  const scheduleId = schedule.id;
  const listenMap: Map<string, AbortController> = (globalThis as any).__listenMap ??= new Map();

  const existing = listenMap.get(groupId);
  if (existing) existing.abort();

  const ctrl = new AbortController();
  listenMap.set(groupId, ctrl);

  const activeMembers = (group.group_members ?? [])
    .filter((m) => m.is_active && m.accounts?.is_active && m.accounts?.session_string)
    .sort((a, b) => a.position - b.position);

  if (activeMembers.length === 0) {
    console.warn(`[listener] Nenhuma conta ativa para schedule ${scheduleId}`);
    listenMap.delete(groupId);
    return;
  }

  console.log(`[listener] 👂 PUSH mode — ${activeMembers.length} conta(s) em ${chatId} (schedule ${scheduleId})`);

  (async () => {
    try {
      const allAccounts = activeMembers.map((m) => accountCache.get(m.accounts!.id) ?? m.accounts!);

      // Pré-aquece via clientPool (sem update loop) para o envio — e listenerPool para escuta
      await Promise.allSettled([
        clientPool.prewarm(allAccounts),
        prewarmPeersForAccounts(allAccounts, chatId),
        ...allAccounts.map((a) => listenerPool.get(a)),
      ]);

      if (ctrl.signal.aborted) return;

      const deadline = Date.now() + OPEN_GROUP_LISTEN_TIMEOUT_MS;
      let fired = false;

      // Handlers push — um por conta, todos em paralelo
      const cleanupFns: Array<() => void> = [];
      const pushPromises = activeMembers.map((member) => {
        const account = accountCache.get(member.accounts!.id) ?? member.accounts!;

        return (async () => {
          // ── Tenta modo push primeiro ──────────────────────────────────────
          let pushWorking = false;
          try {
            const lClient = await listenerPool.get(account);

            // Resolve o peer via listenerPool também (necessário pro filtro de chat)
            let chatEntity: any;
            try { chatEntity = await lClient.getInputEntity(parseInt(chatId, 10)); } catch {}

            const handler = async (event: NewMessageEvent) => {
              if (fired || ctrl.signal.aborted) return;
              const msg  = event.message;
              const text = typeof msg.text === "string" ? msg.text.trim().toLowerCase() : "";
              const isOk    = text === "ok";
              const isMedia = msg.media != null && (msg.media as any).className !== "MessageMediaEmpty";
              if (!isOk && !isMedia) return;

              fired = true;
              ctrl.abort();
              listenMap.delete(groupId);
              console.log(`[listener] ✓ PUSH — sinal por ${account.phone_number} (schedule ${scheduleId})`);
              await dispatchAfterSignal(schedule, group, chatId, scheduleId, new Date());
            };

            // Filtra apenas pelo chat específico para não processar msgs de outros chats
            const filter = chatEntity
              ? new NewMessage({ chats: [chatEntity] })
              : new NewMessage({});

            lClient.addEventHandler(handler, filter);
            pushWorking = true;

            cleanupFns.push(() => {
              try { lClient.removeEventHandler(handler, filter); } catch {}
            });

            console.log(`[listener] ✓ Push ativo: ${account.phone_number}`);

            // Mantém a promise viva até abort ou deadline
            await new Promise<void>((resolve) => {
              const check = setInterval(() => {
                if (ctrl.signal.aborted || Date.now() >= deadline) {
                  clearInterval(check);
                  resolve();
                }
              }, 1_000);
            });

          } catch (pushErr: any) {
            console.warn(`[listener] Push falhou para ${account.phone_number}: ${pushErr.message} — usando fallback poll 5ms`);
          }

          // ── Fallback: polling 5ms se push não funcionou ───────────────────
          if (!pushWorking && !ctrl.signal.aborted) {
            console.log(`[listener] 🔄 Fallback poll 5ms: ${account.phone_number}`);
            let client = await clientPool.get(account).catch(() => null);
            if (!client) return;

            const startUnix = Math.floor((Date.now() - 10_000) / 1000);
            let lastSeenMsgId = 0;

            while (Date.now() < deadline && !ctrl.signal.aborted) {
              try {
                if (!client.connected) client = await clientPool.get(account);
                const peer   = await getOrResolvePeer(client, chatId, account.id);
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

                if (gotSignal && !ctrl.signal.aborted && !fired) {
                  fired = true;
                  ctrl.abort();
                  listenMap.delete(groupId);
                  console.log(`[listener] ✓ POLL — sinal por ${account.phone_number} (schedule ${scheduleId})`);
                  await dispatchAfterSignal(schedule, group, chatId, scheduleId, new Date());
                  return;
                }
              } catch (err: any) {
                if (!ctrl.signal.aborted) await new Promise((r) => setTimeout(r, 2_000));
              }
              if (!ctrl.signal.aborted) await new Promise((r) => setTimeout(r, LISTEN_POLL_MS));
            }
          }
        })();
      });

      await Promise.allSettled(pushPromises);

      // Limpa handlers de todos os clients
      for (const cleanup of cleanupFns) cleanup();
      listenMap.delete(groupId);

      if (fired || ctrl.signal.aborted) return;

      // Timeout 2h sem sinal
      console.warn(`[listener] ⏰ Timeout 2h — schedule ${scheduleId}`);
      const nowISO = new Date().toISOString();
      let nextRun: string;
      try { nextRun = nextWeeklyOccurrence(schedule.cron_expression); }
      catch { await supabase.from("schedules").update({ is_active: false }).eq("id", scheduleId); return; }
      await supabase.from("schedules").update({
        next_run_at: nextRun, retry_until: null, retry_count: 0,
        last_attempt_at: nowISO, last_attempt_status: "timeout",
        last_attempt_error: "Timeout aguardando sinal do admin",
      }).eq("id", scheduleId);
      scheduleTimer(scheduleId, nextRun);

    } catch (err: any) {
      console.error(`[listener] Erro inesperado para schedule ${scheduleId}:`, err.message);
      listenMap.delete(groupId);
    }
  })();
}

/* ─── fireSchedule ─── */
async function fireSchedule(scheduleId: string): Promise<void> {
  const now    = new Date();
  const nowISO = now.toISOString();

  let schedule = schedulePrefetchCache.get(scheduleId);
  schedulePrefetchCache.delete(scheduleId);

  if (schedule) {
    console.log(`[timer] ⚡ Schedule ${scheduleId} do pre-fetch cache`);
  } else {
    const { data: rows, error } = await supabase
      .from("schedules").select(SCHEDULE_SELECT)
      .eq("id", scheduleId).eq("is_active", true).single();
    if (error || !rows) { console.warn(`[timer] Schedule ${scheduleId} não encontrado.`); return; }
    schedule = rows as unknown as Schedule;
  }

  const group = schedule.groups;
  if (!group?.telegram_chat_id) { console.warn(`[timer] Schedule ${scheduleId}: sem telegram_chat_id.`); return; }

  if (group.group_members) {
    group.group_members = group.group_members.map((m) => ({
      ...m, accounts: m.accounts ? (accountCache.get(m.accounts.id) ?? m.accounts) : null,
    }));
  }

  console.log(`[timer] ⚡ Disparando schedule ${scheduleId} às ${nowISO}`);

  if (group.group_type === "open") {
    const listenMap: Map<string, AbortController> = (globalThis as any).__listenMap ??= new Map();
    if (listenMap.has(group.id)) { console.log(`[timer] Listener já ativo para grupo ${group.id}`); return; }
    startScheduledGroupListener(schedule, group);
    supabase.from("schedules").update({
      retry_until: new Date(now.getTime() + OPEN_GROUP_LISTEN_TIMEOUT_MS).toISOString(),
      last_attempt_at: nowISO, last_attempt_status: "waiting_admin", last_attempt_error: null,
    }).eq("id", scheduleId).then(() => {});
    return;
  }

  // Grupo fechado — dispatch imediato, tudo já está aquecido
  const alreadySent = schedule.retry_until ? await getAlreadySentAccountIds(schedule) : new Set<string>();
  const allGroupAccounts = (group.group_members ?? [])
    .filter((m) => m.is_active && m.accounts?.is_active)
    .map((m) => accountCache.get(m.accounts!.id) ?? m.accounts!);

  const results = await processMembersOf(schedule, alreadySent);

  const sentForMonitor = results
    .filter((r) => r.status === "sent")
    .map((r) => {
      const member = (group.group_members ?? []).find((m) => m.accounts?.id === r.account_id);
      return { account_id: r.account_id, message_text: member?.message_text ?? "" };
    }).filter((r) => r.message_text);

  if (sentForMonitor.length > 0) {
    monitorPositions(group.telegram_chat_id, sentForMonitor, scheduleId, now, "closed", allGroupAccounts)
      .catch((err) => console.error("[monitor] Erro:", err.message));
  }

  const sentCount         = results.filter((r) => r.status === "sent").length;
  const skippedCount      = results.filter((r) => r.status === "skipped").length;
  const retryableFailures = results.filter((r) => r.status === "failed" && r.retryable);
  const permanentFailures = results.filter((r) => r.status === "failed" && !r.retryable);
  const hasActiveMembers  = (group.group_members ?? []).some((m) => m.is_active && m.accounts?.is_active);
  const allSucceeded      = hasActiveMembers && retryableFailures.length === 0 && permanentFailures.length === 0 && (sentCount + skippedCount) > 0;

  if (allSucceeded) {
    let nextRun: string;
    try { nextRun = nextWeeklyOccurrence(schedule.cron_expression); }
    catch (err) { await supabase.from("schedules").update({ is_active: false }).eq("id", scheduleId); return; }
    supabase.from("schedules").update({
      next_run_at: nextRun, last_run_at: nowISO, retry_until: null, retry_count: 0,
      last_attempt_at: nowISO, last_attempt_status: "sent", last_attempt_error: null,
    }).eq("id", scheduleId).then(({ error: e }) => { if (e) console.error(`[timer] Falha ao atualizar schedule:`, e.message); });
    console.log(`[timer] ✓ Schedule ${scheduleId} OK. Próxima: ${nextRun}`);
    scheduleTimer(scheduleId, nextRun);
  } else {
    const newRetryCount  = schedule.retry_count + 1;
    const retryUntil     = !schedule.retry_until
      ? new Date(now.getTime() + schedule.retry_window_seconds * 1000).toISOString()
      : schedule.retry_until!;
    await supabase.from("schedules").update({
      retry_until: retryUntil, retry_count: newRetryCount, last_attempt_at: nowISO,
      last_attempt_status: "retrying",
      last_attempt_error: results.filter((r) => r.status === "failed" && r.error).map((r) => `[${r.account_id}] ${r.error}`).join("; ") || null,
    }).eq("id", scheduleId);
    const intervalNext = calcRetryInterval(newRetryCount, schedule.retry_interval_seconds, schedule.retry_interval_max_seconds);
    const retryAt = new Date(now.getTime() + intervalNext * 1000);
    if (retryAt < new Date(retryUntil)) scheduleTimer(scheduleId, retryAt.toISOString());
  }
}

/* ─── Precision timers com pre-fetch 15s ─── */
function scheduleTimer(scheduleId: string, nextRunAt: string): void {
  const delay = new Date(nextRunAt).getTime() - Date.now();
  if (delay < -5_000) { console.warn(`[timer] Schedule ${scheduleId} ignorado — passado demais`); return; }

  const existingTimer = scheduledTimers.get(scheduleId);
  if (existingTimer) clearTimeout(existingTimer);
  const existingPrefetch = prefetchTimers.get(scheduleId);
  if (existingPrefetch) { clearTimeout(existingPrefetch); prefetchTimers.delete(scheduleId); }

  const effectiveDelay = Math.max(0, delay);

  if (effectiveDelay > PREFETCH_BEFORE_MS) {
    const pt = setTimeout(async () => {
      prefetchTimers.delete(scheduleId);
      try {
        const { data, error } = await supabase
          .from("schedules").select(SCHEDULE_SELECT).eq("id", scheduleId).eq("is_active", true).single();
        if (error || !data) return;
        const s = data as unknown as Schedule;
        if (s.groups?.group_members) {
          s.groups.group_members = s.groups.group_members.map((m) => ({
            ...m, accounts: m.accounts ? (accountCache.get(m.accounts.id) ?? m.accounts) : null,
          }));
        }
        schedulePrefetchCache.set(scheduleId, s);

        const chatId  = s.groups?.telegram_chat_id;
        const members = (s.groups?.group_members ?? [])
          .filter((m) => m.is_active && m.accounts?.is_active && m.accounts?.session_string)
          .map((m) => accountCache.get(m.accounts!.id) ?? m.accounts!);

        if (chatId && members.length > 0) {
          await Promise.allSettled([
            clientPool.prewarm(members),
            prewarmPeersForAccounts(members, chatId),
          ]);
          console.log(`[prefetch] ✅ Schedule ${scheduleId} pré-carregado + ${members.length} peer(s) aquecido(s)`);
        }
      } catch (err: any) {
        console.warn(`[prefetch] Falha: ${err.message}`);
      }
    }, effectiveDelay - PREFETCH_BEFORE_MS);
    prefetchTimers.set(scheduleId, pt);
  }

  const timer = setTimeout(async () => {
    scheduledTimers.delete(scheduleId);
    try { await fireSchedule(scheduleId); }
    catch (err) { console.error(`[timer] Erro ao disparar schedule ${scheduleId}:`, err); }
  }, effectiveDelay);

  scheduledTimers.set(scheduleId, timer);
  console.log(`[timer] ⏰ Schedule ${scheduleId} — dispara em ${Math.round(effectiveDelay / 1000)}s`);
}

/* ─── Reload periódico ─── */
async function reloadSchedules(): Promise<void> {
  const now          = new Date();
  const nowISO       = now.toISOString();
  const lookaheadISO = new Date(now.getTime() + LOOKAHEAD_MS).toISOString();

  const [{ data: futureSchedules }, { data: retrySchedules }, { data: expiredRetries }] = await Promise.all([
    supabase.from("schedules").select("id, next_run_at").eq("is_active", true).is("retry_until", null).lte("next_run_at", lookaheadISO),
    supabase.from("schedules").select(SCHEDULE_SELECT).eq("is_active", true).not("retry_until", "is", null).gt("retry_until", nowISO),
    supabase.from("schedules").select("id, cron_expression, group_id").eq("is_active", true).not("retry_until", "is", null).lte("retry_until", nowISO),
  ]);

  await Promise.all((expiredRetries ?? []).map(async (expired) => {
    const listenMap: Map<string, AbortController> = (globalThis as any).__listenMap ??= new Map();
    const expGroupId = (expired as any).group_id as string | undefined;
    if (expGroupId) { const ctrl = listenMap.get(expGroupId); if (ctrl) { ctrl.abort(); listenMap.delete(expGroupId); } }
    let nextRun: string;
    try { nextRun = nextWeeklyOccurrence(expired.cron_expression); }
    catch { await supabase.from("schedules").update({ is_active: false }).eq("id", expired.id); return; }
    await supabase.from("schedules").update({
      next_run_at: nextRun, last_run_at: nowISO, retry_until: null, retry_count: 0,
      last_attempt_at: nowISO, last_attempt_status: "failed", last_attempt_error: "Retry expirou",
    }).eq("id", expired.id);
    scheduleTimer(expired.id, nextRun);
  }));

  for (const s of futureSchedules ?? []) {
    if (!scheduledTimers.has(s.id)) scheduleTimer(s.id, s.next_run_at);
  }

  for (const s of retrySchedules ?? []) {
    const schedule = s as unknown as Schedule;
    const listenMap: Map<string, AbortController> = (globalThis as any).__listenMap ??= new Map();
    if (listenMap.has(schedule.group_id)) continue;
    if (isRetryDue(schedule, now) && !scheduledTimers.has(schedule.id)) {
      fireSchedule(schedule.id).catch((err) => console.error(`[reload] Erro retry:`, err));
    }
  }
}

/* ─── Pre-warm de contas ─── */
let prewarmRunning = false;
async function prewarmAccounts(): Promise<void> {
  if (prewarmRunning) return;
  prewarmRunning = true;
  try {
    const { data, error } = await supabase
      .from("accounts").select("id, name, phone_number, api_id, api_hash, session_string, is_active").eq("is_active", true);
    if (error) { console.warn("[prewarm] Falha:", error.message); return; }
    const accounts = (data ?? []) as Account[];
    for (const account of accounts) accountCache.set(account.id, account);
    await Promise.allSettled(
      accounts.map(async (account) => {
        try { await clientPool.get(account); }
        catch (err: any) {
          const authDead = err.message?.includes("AUTH_KEY_UNREGISTERED") || err.message?.includes("USER_DEACTIVATED") || err.message?.includes("SESSION_REVOKED");
          if (authDead) await supabase.from("accounts").update({ is_active: false }).eq("id", account.id);
        }
      })
    );
  } finally { prewarmRunning = false; }
}

/* ─── HTTP server ─── */
const WORKER_PORT   = parseInt(process.env.PORT ?? "3001", 10);
const WORKER_SECRET = process.env.WORKER_SECRET ?? "";

function jsonResponse(res: http.ServerResponse, status: number, body: unknown) {
  res.writeHead(status, { "Content-Type": "application/json", "Connection": "keep-alive" });
  res.end(JSON.stringify(body));
}

const httpServer = http.createServer(async (req, res) => {
  if (WORKER_SECRET && req.headers["x-worker-secret"] !== WORKER_SECRET) {
    return jsonResponse(res, 401, { error: "Unauthorized" });
  }
  const url = new URL(req.url ?? "/", `http://localhost:${WORKER_PORT}`);

  // GET /accounts/:id/chats
  const chatsMatch = url.pathname.match(/^\/accounts\/([^/]+)\/chats$/);
  if (req.method === "GET" && chatsMatch) {
    const account = accountCache.get(chatsMatch[1]);
    if (!account) return jsonResponse(res, 404, { error: "Conta não encontrada" });
    try {
      const client  = await clientPool.get(account);
      const dialogs = await client.getDialogs({ limit: 200 });
      return jsonResponse(res, 200, dialogs
        .filter((d) => d.isGroup || d.isChannel)
        .map((d) => ({ id: String(d.id), name: d.title ?? d.name ?? "Sem nome", type: d.isChannel ? "channel" : "group", accessHash: null }))
        .sort((a, b) => a.name.localeCompare(b.name)));
    } catch (err: any) { return jsonResponse(res, 500, { error: err.message }); }
  }

  // GET /accounts/:id/chat-count?chat_id=XXXX
  const chatCountMatch = url.pathname.match(/^\/accounts\/([^/]+)\/chat-count$/);
  if (req.method === "GET" && chatCountMatch) {
    const chatId  = url.searchParams.get("chat_id");
    const account = accountCache.get(chatCountMatch[1]);
    if (!chatId)  return jsonResponse(res, 400, { error: "chat_id é obrigatório" });
    if (!account) return jsonResponse(res, 404, { error: "Conta não encontrada" });
    try {
      const client = await clientPool.get(account);
      const rawId  = chatId.replace(/^-100/, "").replace(/^-/, "");
      let count: number | null = null;
      try {
        const r = await client.invoke(new Api.channels.GetFullChannel({ channel: new Api.InputChannel({ channelId: bigInt(rawId), accessHash: bigInt(0) }) })) as any;
        if (typeof r?.fullChat?.participantsCount === "number") count = r.fullChat.participantsCount;
      } catch {}
      if (count === null) {
        try {
          const dialogs = await client.getDialogs({ limit: 500 });
          const absRaw  = rawId.replace(/^100/, "");
          const dialog  = dialogs.find((d) => { const s = String(d.id).replace(/^-/, ""); return s === rawId || s === absRaw || String(d.id) === chatId || `-100${s}` === chatId || `-${s}` === chatId; });
          const ent = dialog?.entity as any;
          if (typeof ent?.participantsCount === "number") count = ent.participantsCount;
          else if (typeof (dialog as any)?.participantsCount === "number") count = (dialog as any).participantsCount;
        } catch {}
      }
      if (count === null) {
        try {
          const full = await client.invoke(new Api.messages.GetFullChat({ chatId: bigInt(rawId.replace(/^100/, "")) })) as any;
          if (typeof full?.fullChat?.participantsCount === "number") count = full.fullChat.participantsCount;
          else if (full?.fullChat?.participants?.participants) count = full.fullChat.participants.participants.length;
        } catch {}
      }
      return jsonResponse(res, 200, { count });
    } catch (err: any) { return jsonResponse(res, 500, { error: err.message }); }
  }

  // GET /accounts/:id/chat-members?chat_id=XXXX
  const membersMatch = url.pathname.match(/^\/accounts\/([^/]+)\/chat-members$/);
  if (req.method === "GET" && membersMatch) {
    const chatId  = url.searchParams.get("chat_id");
    const account = accountCache.get(membersMatch[1]);
    if (!chatId)  return jsonResponse(res, 400, { error: "chat_id é obrigatório" });
    if (!account) return jsonResponse(res, 404, { error: "Conta não encontrada" });
    type MemberOut = { id: string; name: string | null; username: string | null; phone: string | null };
    try {
      const client       = await clientPool.get(account);
      const rawId        = chatId.replace(/^-/, "");
      const isSupergroup = chatId.startsWith("-100");
      let members: MemberOut[] = [];
      if (isSupergroup) {
        try {
          const dialogs = await client.getDialogs({ limit: 500 });
          const dialog  = dialogs.find((d) => { const id = String(d.id); return id === rawId || id === chatId || id === rawId.replace(/^100/, ""); });
          const entity  = dialog?.entity;
          if (entity && (entity.className === "Channel" || entity.className === "Chat")) {
            const result = await client.invoke(new Api.channels.GetParticipants({ channel: entity as Api.Channel, filter: new Api.ChannelParticipantsRecent(), offset: 0, limit: 200, hash: bigInt(0) }));
            if (result.className === "channels.ChannelParticipants") {
              members = result.users.filter((u): u is Api.User => u.className === "User" && !u.bot)
                .map((u) => ({ id: String(u.id), name: [u.firstName, u.lastName].filter(Boolean).join(" ") || null, username: u.username ? `@${u.username}` : null, phone: u.phone ? `+${u.phone}` : null }));
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
            for (const u of full.users) { if (u.className === "User") userMap.set(String(u.id), u as Api.User); }
            members = parts.participants
              .map((p) => { const u = userMap.get(String((p as any).userId)); if (!u || u.bot) return null; return { id: String(u.id), name: [u.firstName, u.lastName].filter(Boolean).join(" ") || null, username: u.username ? `@${u.username}` : null, phone: u.phone ? `+${u.phone}` : null }; })
              .filter((m): m is MemberOut => m !== null);
          }
        } catch {}
      }
      members.sort((a, b) => (a.name ?? "").localeCompare(b.name ?? ""));
      return jsonResponse(res, 200, members);
    } catch (err: any) { return jsonResponse(res, 500, { error: err.message }); }
  }

  // POST /accounts/:id/reload
  const reloadMatch = url.pathname.match(/^\/accounts\/([^/]+)\/reload$/);
  if (req.method === "POST" && reloadMatch) {
    const { data: row, error } = await supabase.from("accounts")
      .select("id, name, phone_number, api_id, api_hash, session_string, is_active").eq("id", reloadMatch[1]).single();
    if (error || !row) return jsonResponse(res, 404, { error: "Conta não encontrada" });
    const account = row as Account;
    accountCache.set(reloadMatch[1], account);
    if (!account.is_active || !account.session_string) return jsonResponse(res, 200, { ok: true, skipped: true });
    try { await clientPool.reload(account); return jsonResponse(res, 200, { ok: true }); }
    catch (err: any) { return jsonResponse(res, 500, { error: err.message }); }
  }

  // POST/DELETE /groups/:id/listen
  const listenMap: Map<string, AbortController> = (globalThis as any).__listenMap ??= new Map();
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
          const { data: grpRow } = await supabase.from("groups")
            .select(`id, telegram_chat_id, telegram_chat_name, group_type, name, user_id,
              group_members(id, message_text, position, is_active,
                accounts(id, name, phone_number, api_id, api_hash, session_string, is_active))`)
            .eq("id", groupId).single();
          if (!grpRow) { listenMap.delete(groupId); return; }

          const chatId = String(grpRow.telegram_chat_id);
          const members: GroupMember[] = (grpRow.group_members ?? []).map((m: any) => ({
            ...m, accounts: Array.isArray(m.accounts) ? (m.accounts[0] ?? null) : (m.accounts ?? null),
          }));
          const activeMembers = members.filter((m) => m.is_active && m.accounts?.is_active);
          if (activeMembers.length === 0) { listenMap.delete(groupId); return; }

          const allAccounts = activeMembers.map((m) => accountCache.get(m.accounts!.id) ?? m.accounts!);
          await Promise.allSettled([
            clientPool.prewarm(allAccounts),
            prewarmPeersForAccounts(allAccounts, chatId),
            ...allAccounts.map((a) => listenerPool.get(a)),
          ]);
          if (ctrl.signal.aborted) { listenMap.delete(groupId); return; }

          const deadline = Date.now() + 2 * 60 * 60_000;
          let fired = false;
          const cleanupFns: Array<() => void> = [];
          console.log(`[listen] 👂 PUSH mode — ${activeMembers.length} conta(s) em ${chatId} (manual grupo ${groupId})`);

          const pollerPromises = activeMembers.map((member) => {
            const account = accountCache.get(member.accounts!.id) ?? member.accounts!;
            return (async () => {
              let pushWorking = false;
              try {
                const lClient = await listenerPool.get(account);
                let chatEntity: any;
                try { chatEntity = await lClient.getInputEntity(parseInt(chatId, 10)); } catch {}

                const handler = async (event: NewMessageEvent) => {
                  if (fired || ctrl.signal.aborted) return;
                  const msg  = event.message;
                  const text = typeof msg.text === "string" ? msg.text.trim().toLowerCase() : "";
                  if (text !== "ok" && !(msg.media != null && (msg.media as any).className !== "MessageMediaEmpty")) return;
                  fired = true;
                  ctrl.abort();
                  listenMap.delete(groupId);
                  supabase.from("groups").update({ listener_session_id: null }).eq("id", groupId).then(() => {});

                  const scheduleStub: Schedule = {
                    id: `manual-${groupId}-${Date.now()}`, user_id: (grpRow as any).user_id ?? "",
                    group_id: groupId, cron_expression: "0 0 * * 0", next_run_at: new Date().toISOString(),
                    retry_window_seconds: 60, retry_interval_seconds: 5, retry_interval_max_seconds: 30,
                    retry_count: 0, retry_until: null, last_attempt_at: null,
                    groups: { id: groupId, name: (grpRow as any).name ?? groupId, telegram_chat_id: chatId, telegram_chat_name: (grpRow as any).telegram_chat_name ?? null, group_type: "open", group_members: members },
                  };
                  const results = await processMembersOf(scheduleStub, new Set<string>());
                  const sentForMonitor = results.filter((r) => r.status === "sent")
                    .map((r) => { const m = members.find((mb) => mb.accounts?.id === r.account_id); return { account_id: r.account_id, message_text: m?.message_text ?? "" }; })
                    .filter((r) => r.message_text);
                  if (sentForMonitor.length > 0) {
                    monitorPositions(chatId, sentForMonitor, scheduleStub.id, new Date(), "open", allAccounts)
                      .catch((err) => console.error("[listen] Monitoramento:", err.message));
                  }
                  console.log(`[listen] ✓ PUSH manual: ${results.filter((r) => r.status === "sent").length} enviada(s)`);
                };

                const filter = chatEntity ? new NewMessage({ chats: [chatEntity] }) : new NewMessage({});
                lClient.addEventHandler(handler, filter);
                pushWorking = true;
                cleanupFns.push(() => { try { lClient.removeEventHandler(handler, filter); } catch {} });

                await new Promise<void>((resolve) => {
                  const check = setInterval(() => { if (ctrl.signal.aborted || Date.now() >= deadline) { clearInterval(check); resolve(); } }, 1_000);
                });
              } catch {}

              if (!pushWorking && !ctrl.signal.aborted) {
                let client = await clientPool.get(account).catch(() => null);
                if (!client) return;
                const startUnix = Math.floor((Date.now() - 10_000) / 1000);
                let lastSeenMsgId = 0;
                while (Date.now() < deadline && !ctrl.signal.aborted) {
                  try {
                    if (!client.connected) client = await clientPool.get(account);
                    const peer   = await getOrResolvePeer(client, chatId, account.id);
                    const result = await client.invoke(new Api.messages.GetHistory({ peer: peer as any, limit: 10, offsetDate: 0, offsetId: 0, maxId: 0, minId: 0, hash: bigInt(0), addOffset: 0 })) as any;
                    const recentMsgs = (result.messages ?? []).filter((m: any) => (m.className === "Message" || m._ === "message") && m.date >= startUnix && m.id > lastSeenMsgId);
                    if (recentMsgs.length > 0) lastSeenMsgId = Math.max(lastSeenMsgId, ...recentMsgs.map((m: any) => m.id as number));
                    const gotSignal = recentMsgs.some((m: any) => { const text = typeof m.message === "string" ? m.message.trim().toLowerCase() : ""; return text === "ok" || (m.media != null && m.media.className !== "MessageMediaEmpty"); });
                    if (gotSignal && !ctrl.signal.aborted && !fired) {
                      fired = true; ctrl.abort(); listenMap.delete(groupId);
                      supabase.from("groups").update({ listener_session_id: null }).eq("id", groupId).then(() => {});
                      const scheduleStub: Schedule = {
                        id: `manual-${groupId}-${Date.now()}`, user_id: (grpRow as any).user_id ?? "",
                        group_id: groupId, cron_expression: "0 0 * * 0", next_run_at: new Date().toISOString(),
                        retry_window_seconds: 60, retry_interval_seconds: 5, retry_interval_max_seconds: 30,
                        retry_count: 0, retry_until: null, last_attempt_at: null,
                        groups: { id: groupId, name: (grpRow as any).name ?? groupId, telegram_chat_id: chatId, telegram_chat_name: (grpRow as any).telegram_chat_name ?? null, group_type: "open", group_members: members },
                      };
                      const results = await processMembersOf(scheduleStub, new Set<string>());
                      console.log(`[listen] ✓ POLL manual: ${results.filter((r) => r.status === "sent").length} enviada(s)`);
                      return;
                    }
                  } catch (err: any) { if (!ctrl.signal.aborted) await new Promise((r) => setTimeout(r, 2_000)); }
                  if (!ctrl.signal.aborted) await new Promise((r) => setTimeout(r, LISTEN_POLL_MS));
                }
              }
            })();
          });

          await Promise.allSettled(pollerPromises);
          for (const cleanup of cleanupFns) cleanup();
          listenMap.delete(groupId);
          if (!fired && !ctrl.signal.aborted) {
            await supabase.from("groups").update({ listener_session_id: null }).eq("id", groupId);
          }
        } catch (err: any) {
          console.error(`[listen] Erro inesperado grupo ${groupId}:`, err.message);
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
    let body: { user_id?: string } = {};
    try {
      const raw = await new Promise<string>((resolve) => { let d = ""; req.on("data", (c) => { d += c; }); req.on("end", () => resolve(d)); });
      body = JSON.parse(raw || "{}");
    } catch {}
    try {
      const { data: grpRow } = await supabase.from("groups")
        .select(`id, name, telegram_chat_id, telegram_chat_name, group_type, user_id,
          group_members(id, message_text, position, is_active,
            accounts(id, name, phone_number, api_id, api_hash, session_string, is_active))`)
        .eq("id", groupId).single();
      if (!grpRow || !grpRow.telegram_chat_id) return jsonResponse(res, 404, { error: "Grupo não encontrado" });
      const members: GroupMember[] = (grpRow.group_members ?? []).map((m: any) => ({ ...m, accounts: Array.isArray(m.accounts) ? (m.accounts[0] ?? null) : (m.accounts ?? null) }));
      const scheduleStub = { id: `manual-dispatch-${groupId}-${Date.now()}`, user_id: body.user_id ?? grpRow.user_id ?? "", group_id: groupId, cron_expression: "0 0 * * 0", next_run_at: new Date().toISOString(), retry_window_seconds: 60, retry_interval_seconds: 5, retry_interval_max_seconds: 30, retry_count: 0, retry_until: null, last_attempt_at: null, groups: { id: groupId, name: grpRow.name, telegram_chat_id: String(grpRow.telegram_chat_id), telegram_chat_name: grpRow.telegram_chat_name ?? null, group_type: (grpRow.group_type ?? "closed") as "open" | "closed", group_members: members } };
      const dispatchedAt     = new Date();
      const results          = await processMembersOf(scheduleStub as any, new Set<string>());
      const allGroupAccounts = members.filter((m) => m.is_active && m.accounts?.is_active).map((m) => m.accounts!);
      const sentForMonitor   = results.filter((r) => r.status === "sent").map((r) => { const member = members.find((m) => m.accounts?.id === r.account_id); return { account_id: r.account_id, message_text: member?.message_text ?? "" }; }).filter((r) => r.message_text);
      if (sentForMonitor.length > 0) monitorPositions(String(grpRow.telegram_chat_id), sentForMonitor, scheduleStub.id, dispatchedAt, scheduleStub.groups.group_type, allGroupAccounts).catch(() => {});
      return jsonResponse(res, 200, { ok: true, sent: results.filter((r) => r.status === "sent").length, failed: results.filter((r) => r.status === "failed").length, results });
    } catch (err: any) { return jsonResponse(res, 500, { error: err.message }); }
  }

  jsonResponse(res, 404, { error: "Not found" });
});

// Keep-alive agressivo
httpServer.keepAliveTimeout = 65_000;
httpServer.headersTimeout   = 70_000;

httpServer.listen(WORKER_PORT, () => {
  console.log(`[worker] HTTP na porta ${WORKER_PORT} (instância ${INSTANCE_ID})`);
});

/* ─── Inicialização ─── */
async function init(): Promise<void> {
  console.log(`[worker] Iniciando instância ${INSTANCE_ID}...`);
  const locked = await acquireInstanceLock();
  if (!locked) { console.error("[worker] Outra instância ativa — encerrando."); process.exit(1); }
  console.log(`[worker] Lock adquirido.`);
  await prewarmAccounts();
  await reloadSchedules();
  setInterval(renewInstanceLock, 10_000);
  setInterval(async () => { if (!(await checkInstanceLock())) { console.error("[worker] Lock perdido — encerrando."); process.exit(1); } }, 15_000);
  setInterval(async () => {
    try { await Promise.allSettled([reloadSchedules(), prewarmAccounts()]); }
    catch (err) { console.error("[reload] Erro:", err); }
  }, RELOAD_INTERVAL_MS);
  console.log("[worker] ✅ Pronto. Push ativo, poll fallback 5ms.");
}

init().catch((err) => { console.error("[worker] Falha na inicialização:", err); process.exit(1); });

// worker.ts — high-precision Telegram dispatch worker
// v13 — retry agressivo: 10ms entre tentativas, teto de 25s (era 50s sem delay)
import { createClient } from "@supabase/supabase-js";
import { TelegramClient, Api } from "telegram";
import { StringSession } from "telegram/sessions";
import bigInt from "big-integer";
import http from "http";

/* ─── Supabase ─── */
const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

/* ─── Constantes ─── */
const SEND_TIMEOUT_MS        = 15_000;
const RETRY_BUDGET_MS        = 25_000;   // teto de 25s para grupos fechados abrirem
const RETRY_DELAY_MS         = 10;       // delay entre tentativas (evita CPU spin, maximiza velocidade)
const RELOAD_INTERVAL_MS     = 30_000;
const LOOKAHEAD_MS           = 2 * 60 * 1000;
const KEEPALIVE_INTERVAL_MS  = 45_000;
const PREFETCH_BEFORE_MS     = 800;   // pre-carrega schedule N ms antes do fire

// Monitoramento de posição
const MONITOR_DELAY_CLOSED_MS     = 6_000;
const MONITOR_MAX_OPEN_MS         = 5 * 60_000;
const MONITOR_POLL_MS             = 5_000;
const LISTEN_POLL_MS              = 400;
const MONITOR_HISTORY_LIMIT       = 150;
const OPEN_GROUP_LISTEN_TIMEOUT_MS = 2 * 60 * 60_000;

/* ─── Tipos ─── */
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
interface MemberResult {
  member_id: string;
  account_id: string;
  position_rank: number;
  status: "sent" | "failed" | "skipped";
  retryable: boolean;
  error?: string;
}

/* ─── Peer cache ─── */
const peerCache    = new Map<string, unknown>();
const accountCache = new Map<string, Account>();

/* ─── Pre-fetch cache (0-latency dispatch) ─── */
const schedulePrefetchCache = new Map<string, Schedule>();
const prefetchTimers        = new Map<string, ReturnType<typeof setTimeout>>();

/* Query reutilizada no pre-fetch e no fallback de fireSchedule */
const SCHEDULE_SELECT = `
  id, cron_expression, user_id, group_id, next_run_at,
  retry_window_seconds, retry_interval_seconds, retry_interval_max_seconds,
  retry_count, retry_until, last_attempt_at,
  groups(id, name, telegram_chat_id, telegram_chat_name, group_type,
    group_members(id, message_text, position, is_active,
      accounts(id, name, phone_number, api_id, api_hash, session_string, is_active)))
`.trim();

/* ─── Resolve peer com múltiplos fallbacks ─── */
async function getOrResolvePeer(
  client: TelegramClient,
  telegramChatId: string,
  accountId: string
): Promise<unknown> {
  const cacheKey = `${accountId}:${telegramChatId}`;
  if (peerCache.has(cacheKey)) return peerCache.get(cacheKey)!;

  const chatIdNum = parseInt(telegramChatId, 10);
  if (isNaN(chatIdNum)) throw new Error(`telegram_chat_id inválido: "${telegramChatId}"`);

  try {
    const peer = await client.getInputEntity(chatIdNum);
    peerCache.set(cacheKey, peer);
    console.log(`[peer] ✓ getInputEntity: ${telegramChatId}`);
    return peer;
  } catch (e1: any) {
    console.warn(`[peer] getInputEntity falhou para ${telegramChatId}: ${e1.message}`);
  }

  const absId     = Math.abs(chatIdNum);
  const channelId = absId > 1_000_000_000_000 ? absId - 1_000_000_000_000 : absId;

  try {
    const result = await client.invoke(
      new Api.channels.GetChannels({
        id: [
          new Api.InputChannel({
            channelId: bigInt(channelId),
            accessHash: bigInt(0),
          }),
        ],
      })
    ) as any;

    const chat = result?.chats?.[0];
    if (chat?.accessHash != null) {
      const peer = new Api.InputPeerChannel({
        channelId: chat.id,
        accessHash: chat.accessHash,
      });
      peerCache.set(cacheKey, peer);
      console.log(`[peer] ✓ GetChannels MTProto: ${telegramChatId}`);
      return peer;
    }
  } catch (e2: any) {
    console.warn(`[peer] GetChannels falhou para ${telegramChatId}: ${e2.message}`);
  }

  try {
    console.warn(`[peer] Sincronizando dialogs para resolver ${telegramChatId}...`);
    await client.getDialogs({ limit: 200 });
    const peer = await client.getInputEntity(chatIdNum);
    peerCache.set(cacheKey, peer);
    console.log(`[peer] ✓ Resolvido após GetDialogs: ${telegramChatId}`);
    return peer;
  } catch (e3: any) {
    throw new Error(
      `PEER_UNRESOLVABLE ${telegramChatId}: conta não é membro ou sessão inválida. ` +
      `Último erro: ${e3.message}`
    );
  }
}

/* ─── Pool de conexões Telegram persistente ─── */
class TelegramClientPool {
  private clients            = new Map<string, TelegramClient>();
  private sessions           = new Map<string, string>();
  private keepaliveTimers    = new Map<string, ReturnType<typeof setInterval>>();
  private connectingPromises = new Map<string, Promise<TelegramClient>>();
  // Rastreia quando getDialogs do warm-up terminou por conta.
  // Permite que o prefetch aguarde o entity cache estar populado antes do fire.
  private dialogsPromises    = new Map<string, Promise<void>>();

  private startKeepalive(accountId: string, client: TelegramClient): void {
    const existing = this.keepaliveTimers.get(accountId);
    if (existing) clearInterval(existing);

    const interval = setInterval(async () => {
      if (!client.connected) {
        console.warn(`[keepalive] ${accountId} desconectado — removendo do pool`);
        this._evict(accountId, interval);
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
        console.warn(`[keepalive] Ping falhou para ${accountId}: ${err.message} — removendo do pool`);
        try { await client.disconnect(); } catch {}
        this._evict(accountId, interval);

        const authDead =
          err.message?.includes("AUTH_KEY_UNREGISTERED") ||
          err.message?.includes("USER_DEACTIVATED") ||
          err.message?.includes("SESSION_REVOKED");
        if (authDead) {
          console.warn(`[keepalive] Sessão morta para ${accountId} — desativando no banco.`);
          supabase.from("accounts").update({ is_active: false }).eq("id", accountId)
            .then(({ error }) => {
              if (error) console.error(`[keepalive] Falha ao desativar conta ${accountId}:`, error.message);
              else console.log(`[keepalive] Conta ${accountId} desativada.`);
            });
        }
      }
    }, KEEPALIVE_INTERVAL_MS);

    this.keepaliveTimers.set(accountId, interval);
  }

  private _evict(accountId: string, interval?: ReturnType<typeof setInterval>): void {
    if (interval) clearInterval(interval);
    this.keepaliveTimers.delete(accountId);
    this.clients.delete(accountId);
    this.dialogsPromises.delete(accountId);
  }

  async get(account: Account): Promise<TelegramClient> {
    const existing       = this.clients.get(account.id);
    const sessionInUse   = this.sessions.get(account.id);
    const sessionChanged = sessionInUse !== account.session_string;

    if (existing?.connected && !sessionChanged) return existing;

    const inflight = this.connectingPromises.get(account.id);
    if (inflight) {
      console.log(`[pool] Aguardando conexão em andamento para ${account.phone_number}...`);
      return inflight;
    }

    const connectPromise = (async () => {
      if (existing) {
        try { await existing.disconnect(); } catch {}
        this._evict(account.id);
      }

      if (sessionChanged && sessionInUse) {
        console.log(`[pool] Session mudou para ${account.phone_number} — reconectando...`);
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

      (client as any)._updateLoop = () => Promise.resolve();

      await client.connect();

      // Armazena a promise de dialogs para que awaitDialogsReady() possa sinalizá-la.
      // O entity cache fica populado quando ela resolve, eliminando getDialogs no caminho crítico.
      const dp = client.getDialogs({ limit: 100 })
        .then(() => { console.log(`[pool] ✓ Dialogs sincronizados: ${account.phone_number}`); })
        .catch((err: any) => { console.warn(`[pool] getDialogs falhou no warm-up de ${account.phone_number}: ${err.message}`); });
      this.dialogsPromises.set(account.id, dp as Promise<void>);

      this.clients.set(account.id, client);
      this.sessions.set(account.id, account.session_string);
      this.startKeepalive(account.id, client);
      console.log(`[pool] Conectado: ${account.phone_number}`);
      return client;
    })();

    this.connectingPromises.set(account.id, connectPromise);
    try {
      return await connectPromise;
    } finally {
      this.connectingPromises.delete(account.id);
    }
  }

  async reload(account: Account): Promise<TelegramClient> {
    const existing = this.clients.get(account.id);
    if (existing) {
      try { await existing.disconnect(); } catch {}
      this._evict(account.id);
    }
    this.connectingPromises.delete(account.id);
    return this.get(account);
  }

  async prewarm(accounts: Account[]): Promise<void> {
    console.log(`[pool] Pre-warming ${accounts.length} conta(s)...`);
    await Promise.allSettled(accounts.map((a) => this.get(a)));
    console.log(`[pool] Pre-warm concluído.`);
  }

  /**
   * Aguarda o getDialogs de warm-up terminar para a conta informada.
   * Limita a espera a timeoutMs (padrão 600ms) para não atrasar o prefetch.
   * Quando resolve, o entity cache do client já está populado →
   * getInputEntity funciona sem round-trip adicional.
   */
  async awaitDialogsReady(accountId: string, timeoutMs = 600): Promise<void> {
    const p = this.dialogsPromises.get(accountId);
    if (!p) return;
    await Promise.race([
      p,
      new Promise<void>((r) => setTimeout(r, timeoutMs)),
    ]).catch(() => {});
  }

  async disconnectAll(): Promise<void> {
    for (const timer of this.keepaliveTimers.values()) clearInterval(timer);
    this.keepaliveTimers.clear();
    await Promise.all(
      [...this.clients.entries()].map(async ([id, client]) => {
        try { await client.disconnect(); } catch {}
        console.log(`[pool] Desconectado: ${id}`);
      })
    );
    this.clients.clear();
  }
}

const clientPool = new TelegramClientPool();

/* ─── Graceful shutdown ─── */
async function shutdown() {
  console.log("[worker] Encerrando...");
  for (const t of prefetchTimers.values()) clearTimeout(t);
  prefetchTimers.clear();
  for (const t of scheduledTimers.values()) clearTimeout(t);
  scheduledTimers.clear();
  httpServer.close();
  await clientPool.disconnectAll();
  process.exit(0);
}
process.on("SIGTERM", shutdown);
process.on("SIGINT",  shutdown);

/* ─── Helpers ─── */
function isRetryableError(msg: string): boolean {
  const upper = msg.toUpperCase();
  if (upper.includes("AUTH_KEY_UNREGISTERED")) return false;
  if (upper.includes("USER_DEACTIVATED"))      return false;
  if (upper.includes("SESSION_REVOKED"))       return false;
  return true;
}

function nextWeeklyOccurrence(cronExpression: string): string {
  const parts = cronExpression.trim().split(/\s+/);
  const mi    = parseInt(parts[0], 10);
  const h     = parseInt(parts[1], 10);
  const dow   = parseInt(parts[4], 10);

  if (
    parts.length < 5 ||
    isNaN(mi) || isNaN(h) || isNaN(dow) ||
    mi < 0 || mi > 59 || h < 0 || h > 23 || dow < 0 || dow > 6
  ) throw new Error(`cron_expression inválida: "${cronExpression}"`);

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

/* ─── Deduplicação ─── */
async function getAlreadySentAccountIds(schedule: Schedule): Promise<Set<string>> {
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

  return new Set((data ?? []).map((r) => r.account_id as string));
}

/* ─── Envio agressivo com retry interno ─────────────────────────────────────
 * Estratégia para grupos fechados:
 *   - Tentativa imediata (sem delay inicial)
 *   - Qualquer erro → aguarda RETRY_DELAY_MS (10ms) e tenta de novo
 *   - Continua até RETRY_BUDGET_MS (25s) esgotar
 *   - FloodWait ainda é respeitado (não adianta lutar com rate-limit)
 * ──────────────────────────────────────────────────────────────────────────── */
async function sendAggressively(
  client: TelegramClient,
  account: Account,
  telegramChatId: string,
  messageText: string
): Promise<void> {
  const budgetEnd = Date.now() + RETRY_BUDGET_MS;
  let attempt     = 0;

  while (Date.now() < budgetEnd) {
    attempt++;
    const timeLeft = budgetEnd - Date.now();
    if (timeLeft < 100) break;

    const attemptTimeout = Math.min(SEND_TIMEOUT_MS, timeLeft - 50);

    try {
      await Promise.race([
        (async () => {
          const peer = await getOrResolvePeer(client, telegramChatId, account.id);
          try {
            await client.invoke(
              new Api.messages.SendMessage({
                peer:      peer as any,
                message:   messageText,
                randomId:  bigInt(Math.floor(Math.random() * 0x7fffffff)),
                noWebpage: true,
              })
            );
          } catch (err: any) {
            const errMsg = String(err?.message ?? "");

            if (
              errMsg.includes("PEER_ID_INVALID") ||
              errMsg.includes("CHANNEL_INVALID") ||
              errMsg.includes("CHANNEL_PRIVATE")
            ) {
              console.warn(`[peer] Cache inválido para ${telegramChatId} — limpando`);
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
              console.warn(`[retry] FloodWait ${waitSecs}s — ${account.phone_number}`);

              if (waitMs < budgetEnd - Date.now() - 500) {
                await new Promise((r) => setTimeout(r, waitMs));
                peerCache.delete(`${account.id}:${telegramChatId}`);
                const freshPeer = await getOrResolvePeer(client, telegramChatId, account.id);
                await client.invoke(
                  new Api.messages.SendMessage({
                    peer:      freshPeer as any,
                    message:   messageText,
                    randomId:  bigInt(Math.floor(Math.random() * 0x7fffffff)),
                    noWebpage: true,
                  })
                );
                return;
              }
              throw new Error(`FLOOD_WAIT_${waitSecs}_EXCEEDS_BUDGET`);
            }

            throw err;
          }
        })(),
        new Promise<never>((_, r) =>
          setTimeout(() => r(new Error(`TIMEOUT tentativa ${attempt}`)), attemptTimeout)
        ),
      ]);

      if (attempt > 1) {
        console.log(`[retry] ✓ ${account.phone_number} — enviou na tentativa ${attempt} (${Date.now() - (budgetEnd - RETRY_BUDGET_MS)}ms desde início)`);
      }
      return;

    } catch (err: unknown) {
      const errMsg    = (err as any)?.message ?? String(err);
      const remaining = budgetEnd - Date.now();
      if (remaining > 500) {
        console.warn(`[retry] tentativa ${attempt} falhou (${Math.round(remaining / 1000)}s restantes): ${errMsg}`);
      }
      // Aguarda 10ms antes de tentar de novo — evita CPU spin e
      // dá tempo mínimo para o grupo "abrir" entre tentativas.
      if (remaining > RETRY_DELAY_MS + 50) {
        await new Promise((r) => setTimeout(r, RETRY_DELAY_MS));
      }
    }
  }

  throw new Error(`BUDGET_EXCEEDED após ${attempt} tentativa(s) em ${RETRY_BUDGET_MS / 1000}s`);
}

/* ─── Tenta enviar um membro ─── */
async function trySendMember(
  member: GroupMember,
  account: Account,
  group: Group,
  schedule: Schedule,
  alreadySent: Set<string>,
  positionRank: number
): Promise<MemberResult> {
  if (alreadySent.has(account.id)) {
    console.log(`[worker] ↷ ${member.id} (${account.phone_number}) — já enviou neste ciclo [mem]`);
    return { member_id: member.id, account_id: account.id, position_rank: positionRank, status: "skipped", retryable: false };
  }

  let logStatus: "sent" | "failed" = "failed";
  let errorMsg: string | undefined;
  let retryable = false;

  try {
    const client = await clientPool.get(account);
    await sendAggressively(client, account, group.telegram_chat_id!, member.message_text ?? "");
    logStatus = "sent";
    alreadySent.add(account.id);
    console.log(`[worker] ✓ ${member.id} (${account.phone_number})`);
  } catch (err) {
    errorMsg  = err instanceof Error ? err.message : String(err);
    retryable = isRetryableError(errorMsg);
    console.error(
      `[worker] ✗ ${member.id} [${retryable ? "retryável" : "permanente"}] ` +
      `(${account.phone_number}): ${errorMsg}`
    );
  }

  // Insert em background — não bloqueia o caminho crítico de envio
  supabase.from("dispatch_logs").insert({
    user_id:             schedule.user_id,
    group_id:            group.id,
    account_id:          account.id,
    schedule_id:         schedule.id,
    status:              logStatus,
    message_text:        member.message_text,
    position_rank:       positionRank,
    group_name_snapshot: group.name,
    chat_name_snapshot:  group.telegram_chat_name,
    sent_at:             logStatus === "sent" ? new Date().toISOString() : null,
    error_message:       errorMsg ?? null,
  }).then(({ error: e }) => {
    if (e) console.error(`[log] Falha ao inserir dispatch_log para ${account.id}:`, e.message);
  });

  return { member_id: member.id, account_id: account.id, position_rank: positionRank, status: logStatus, retryable, error: errorMsg };
}

/* ─── Processa membros em paralelo ─── */
async function processMembersOf(
  schedule: Schedule,
  alreadySent: Set<string>
): Promise<MemberResult[]> {
  const group = schedule.groups;

  const members = (group.group_members ?? [])
    .filter((m) => m.is_active && m.accounts?.is_active && m.accounts?.session_string)
    .sort((a, b) => a.position - b.position);

  const parallel = await Promise.all(
    members.map((member, i) => trySendMember(member, member.accounts!, group, schedule, alreadySent, i + 1))
  );

  return parallel;
}

async function waitForAdminSignal(
  client: TelegramClient,
  telegramChatId: string,
  timeoutMs: number = 30 * 60_000
): Promise<boolean> {
  const deadline    = Date.now() + timeoutMs;
  const startUnix   = Math.floor((Date.now() - 10_000) / 1000);
  let lastSeenMsgId = 0;

  try { await getOrResolvePeer(client, telegramChatId, client.session.toString()); } catch {}

  console.log(`[admin-signal] Aguardando OK do admin em ${telegramChatId}...`);

  while (Date.now() < deadline) {
    try {
      const peer = await getOrResolvePeer(client, telegramChatId, client.session.toString());

      const result = await client.invoke(
        new Api.messages.GetHistory({
          peer:       peer as any,
          limit:      10,
          offsetDate: 0,
          offsetId:   0,
          maxId:      0,
          minId:      0,
          hash:       bigInt(0),
          addOffset:  0,
        })
      ) as any;

      const recentMsgs: any[] = (result.messages ?? [])
        .filter((m: any) => (m.className === "Message" || m._ === "message") && m.date >= startUnix && m.id > lastSeenMsgId);

      if (recentMsgs.length > 0) {
        lastSeenMsgId = Math.max(lastSeenMsgId, ...recentMsgs.map((m: any) => m.id as number));
      }

      const signal = recentMsgs.some((m: any) => {
        const isOk    = typeof m.message === "string" && m.message.trim().toLowerCase() === "ok";
        const isMedia = m.media != null && m.media.className !== "MessageMediaEmpty";
        return isOk || isMedia;
      });

      if (signal) {
        console.log(`[admin-signal] ✓ Sinal do admin detectado em ${telegramChatId}`);
        return true;
      }
    } catch (err: any) {
      console.warn(`[admin-signal] Erro ao buscar histórico: ${err.message}`);
    }

    await new Promise((r) => setTimeout(r, LISTEN_POLL_MS));
  }

  console.warn(`[admin-signal] Timeout — nenhum sinal do admin em ${telegramChatId}`);
  return false;
}

async function monitorPositions(
  telegramChatId: string,
  sentMembers: Array<{ account_id: string; message_text: string }>,
  scheduleId: string,
  dispatchedAt: Date,
  groupType: "open" | "closed"
): Promise<void> {
  if (sentMembers.length === 0) return;

  const account = accountCache.get(sentMembers[0].account_id);
  if (!account) {
    console.warn("[monitor] Conta não encontrada no cache — monitoramento de posição ignorado");
    return;
  }

  const client = await clientPool.get(account).catch(() => null);
  if (!client) {
    console.warn("[monitor] Não foi possível obter client — monitoramento de posição ignorado");
    return;
  }

  const windowStartUnix = Math.floor((dispatchedAt.getTime() - 15_000) / 1000);
  const deadline        = Date.now() + (groupType === "closed" ? MONITOR_DELAY_CLOSED_MS + 10_000 : MONITOR_MAX_OPEN_MS);
  const ourTexts        = new Set(sentMembers.map((m) => m.message_text).filter(Boolean));

  if (groupType === "closed") {
    await new Promise((r) => setTimeout(r, MONITOR_DELAY_CLOSED_MS));
  }

  console.log(`[monitor] Iniciando para schedule ${scheduleId} (${groupType})`);

  while (Date.now() < deadline) {
    try {
      const peer = await getOrResolvePeer(client, telegramChatId, account.id);

      const result = await client.invoke(
        new Api.messages.GetHistory({
          peer:       peer as any,
          limit:      MONITOR_HISTORY_LIMIT,
          offsetDate: 0,
          offsetId:   0,
          maxId:      0,
          minId:      0,
          hash:       bigInt(0),
          addOffset:  0,
        })
      ) as any;

      const allMsgs: any[] = result.messages ?? [];

      const windowMsgs = allMsgs
        .filter((m: any) => m._ === "message" && m.date >= windowStartUnix)
        .reverse();

      if (windowMsgs.length === 0) {
        if (groupType === "closed") {
          console.warn(`[monitor] Nenhuma mensagem na janela (grupo fechado) — abortando`);
          return;
        }
        await new Promise((r) => setTimeout(r, MONITOR_POLL_MS));
        continue;
      }

      const ourMessagesVisible = windowMsgs.some((m: any) => ourTexts.has(m.message));

      if (groupType === "open" && !ourMessagesVisible) {
        await new Promise((r) => setTimeout(r, MONITOR_POLL_MS));
        continue;
      }

      const updates: Promise<unknown>[] = [];
      const cutoff = new Date(dispatchedAt.getTime() - 60_000).toISOString();

      for (const sm of sentMembers) {
        if (!sm.message_text) continue;
        const idx = windowMsgs.findIndex((m: any) => m.message === sm.message_text);
        if (idx < 0) continue;

        const rank = idx + 1;
        console.log(`[monitor] ${sm.account_id}: #${rank} em ${telegramChatId}`);

        updates.push(
          Promise.resolve(
            supabase.from("dispatch_logs")
              .update({ position_rank: rank })
              .eq("schedule_id", scheduleId)
              .eq("account_id",  sm.account_id)
              .eq("status",      "sent")
              .gte("sent_at",    cutoff)
          )
        );
      }

      await Promise.allSettled(updates);
      console.log(`[monitor] ✓ Posições salvas para schedule ${scheduleId}`);
      return;

    } catch (err: any) {
      console.warn(`[monitor] Erro ao buscar histórico: ${err.message}`);
      if (groupType === "closed") return;
      await new Promise((r) => setTimeout(r, MONITOR_POLL_MS));
    }
  }

  console.warn(`[monitor] Timeout — posições não registradas para schedule ${scheduleId}`);
}

/* ─── Listener não-bloqueante para grupos abertos (schedule-driven) ─── */
function startScheduledGroupListener(
  schedule: Schedule,
  group: Group,
  account: Account
): void {
  const groupId    = group.id;
  const chatId     = group.telegram_chat_id!;
  const scheduleId = schedule.id;
  const listenMap: Map<string, AbortController> = (globalThis as any).__listenMap ??= new Map();

  const existing = listenMap.get(groupId);
  if (existing) existing.abort();

  const ctrl      = new AbortController();
  listenMap.set(groupId, ctrl);

  const deadline    = Date.now() + OPEN_GROUP_LISTEN_TIMEOUT_MS;
  const startUnix   = Math.floor((Date.now() - 10_000) / 1000);
  let lastSeenMsgId = 0;

  console.log(`[schedule-listen] 👂 Aguardando sinal do admin em ${chatId} para schedule ${scheduleId}`);

  (async () => {
    try {
      let client = await clientPool.get(account).catch(() => null);
      if (!client) {
        console.warn(`[schedule-listen] Sem client para ${scheduleId} — abortando listener`);
        listenMap.delete(groupId);
        return;
      }

      try { await getOrResolvePeer(client, chatId, account.id); } catch {}

      while (Date.now() < deadline && !ctrl.signal.aborted) {
        try {
          if (!client.connected) {
            console.warn(`[schedule-listen] Client desconectado — reconectando para ${scheduleId}`);
            client = await clientPool.get(account);
            try { await getOrResolvePeer(client, chatId, account.id); } catch {}
          }

          const peer   = await getOrResolvePeer(client, chatId, account.id);
          const result = await client.invoke(
            new Api.messages.GetHistory({
              peer: peer as any, limit: 10,
              offsetDate: 0, offsetId: 0, maxId: 0, minId: 0,
              hash: bigInt(0), addOffset: 0,
            })
          ) as any;

          const recentMsgs: any[] = (result.messages ?? []).filter(
            (m: any) => (m.className === "Message" || m._ === "message") && m.date >= startUnix && m.id > lastSeenMsgId
          );
          if (recentMsgs.length > 0) {
            lastSeenMsgId = Math.max(lastSeenMsgId, ...recentMsgs.map((m: any) => m.id as number));
          }

          const gotSignal = recentMsgs.some((m: any) => {
            const text    = typeof m.message === "string" ? m.message.trim().toLowerCase() : "";
            const isOk    = text === "ok";
            const isMedia = m.media != null && m.media.className !== "MessageMediaEmpty";
            return isOk || isMedia;
          });

          if (gotSignal && !ctrl.signal.aborted) {
            console.log(`[schedule-listen] ✓ Sinal detectado — disparando schedule ${scheduleId}`);
            listenMap.delete(groupId);

            const dispatchedAt = new Date();
            const alreadySent  = await getAlreadySentAccountIds(schedule);
            const results      = await processMembersOf(schedule, alreadySent);

            const sentForMonitor = results
              .filter((r) => r.status === "sent")
              .map((r) => {
                const member = (group.group_members ?? []).find((m) => m.accounts?.id === r.account_id);
                return { account_id: r.account_id, message_text: member?.message_text ?? "" };
              })
              .filter((r) => r.message_text);

            if (sentForMonitor.length > 0) {
              monitorPositions(chatId, sentForMonitor, scheduleId, dispatchedAt, "open")
                .catch((err) => console.error("[schedule-listen] Erro no monitoramento:", err.message));
            }

            const sentCount         = results.filter((r) => r.status === "sent").length;
            const skippedCount      = results.filter((r) => r.status === "skipped").length;
            const retryableFailures = results.filter((r) => r.status === "failed" && r.retryable);
            const permanentFailures = results.filter((r) => r.status === "failed" && !r.retryable);
            const totalDelivered    = sentCount + skippedCount;
            const hasActiveMembers  = (group.group_members ?? []).some((m) => m.is_active && m.accounts?.is_active);
            const allSucceeded      = hasActiveMembers && retryableFailures.length === 0 && permanentFailures.length === 0 && totalDelivered > 0;
            const nowISO            = new Date().toISOString();

            if (allSucceeded) {
              let nextRun: string;
              try { nextRun = nextWeeklyOccurrence(schedule.cron_expression); }
              catch (err) {
                console.error(`[schedule-listen] cron inválido no schedule ${scheduleId}:`, err);
                await supabase.from("schedules").update({ is_active: false }).eq("id", scheduleId);
                return;
              }
              await supabase.from("schedules").update({
                next_run_at:         nextRun,
                last_run_at:         nowISO,
                retry_until:         null,
                retry_count:         0,
                last_attempt_at:     nowISO,
                last_attempt_status: "sent",
                last_attempt_error:  null,
              }).eq("id", scheduleId);
              console.log(`[schedule-listen] ✓ Schedule ${scheduleId} OK. Próxima: ${nextRun}`);
              scheduleTimer(scheduleId, nextRun);
            } else {
              const newRetryCount  = schedule.retry_count + 1;
              const retryUntil     = new Date(Date.now() + schedule.retry_window_seconds * 1000).toISOString();
              const failedErrors   = results
                .filter((r) => r.status === "failed" && r.error)
                .map((r) => `[${r.account_id}] ${r.error}`).join("; ");
              await supabase.from("schedules").update({
                retry_until:         retryUntil,
                retry_count:         newRetryCount,
                last_attempt_at:     nowISO,
                last_attempt_status: "retrying",
                last_attempt_error:  failedErrors || null,
              }).eq("id", scheduleId);
              const intervalNext = calcRetryInterval(newRetryCount, schedule.retry_interval_seconds, schedule.retry_interval_max_seconds);
              const retryAt = new Date(Date.now() + intervalNext * 1000);
              if (retryAt < new Date(retryUntil)) scheduleTimer(scheduleId, retryAt.toISOString());
            }
            return;
          }

        } catch (err: any) {
          if (!ctrl.signal.aborted) {
            console.warn(`[schedule-listen] Erro ao buscar histórico (${scheduleId}): ${err.message}`);
            await new Promise((r) => setTimeout(r, 2_000));
          }
        }

        if (!ctrl.signal.aborted) {
          await new Promise((r) => setTimeout(r, LISTEN_POLL_MS));
        }
      }

      listenMap.delete(groupId);

      if (ctrl.signal.aborted) {
        console.log(`[schedule-listen] ⏹ Listener abortado para schedule ${scheduleId}`);
        return;
      }

      console.warn(`[schedule-listen] ⏰ Timeout 2h — nenhum sinal do admin para schedule ${scheduleId}`);
      const nowISO = new Date().toISOString();
      let nextRun: string;
      try { nextRun = nextWeeklyOccurrence(schedule.cron_expression); }
      catch { await supabase.from("schedules").update({ is_active: false }).eq("id", scheduleId); return; }
      await supabase.from("schedules").update({
        next_run_at:         nextRun,
        retry_until:         null,
        retry_count:         0,
        last_attempt_at:     nowISO,
        last_attempt_status: "timeout",
        last_attempt_error:  "Timeout aguardando sinal do admin",
      }).eq("id", scheduleId);
      scheduleTimer(scheduleId, nextRun);

    } catch (err: any) {
      console.error(`[schedule-listen] Erro inesperado para schedule ${scheduleId}:`, err.message);
      listenMap.delete(groupId);
    }
  })();
}

/* ─── Dispara um schedule ─── */
async function fireSchedule(scheduleId: string): Promise<void> {
  const now    = new Date();
  const nowISO = now.toISOString();

  // Usa dados pré-carregados (caminho 0-latência) — fallback para busca ao vivo
  let prefetched = schedulePrefetchCache.get(scheduleId);
  schedulePrefetchCache.delete(scheduleId);

  const schedule = await (async () => {
    if (prefetched) {
      console.log(`[timer] ⚡ Schedule ${scheduleId} servido do pre-fetch cache`);
      return prefetched;
    }
    const { data: rows, error } = await supabase
      .from("schedules")
      .select(SCHEDULE_SELECT)
      .eq("id", scheduleId)
      .eq("is_active", true)
      .single();
    if (error || !rows) return null;
    return rows as unknown as Schedule;
  })();

  if (!schedule) {
    console.warn(`[timer] Schedule ${scheduleId} não encontrado ou inativo.`);
    return;
  }

  const group = schedule.groups;

  if (!group?.telegram_chat_id) {
    console.warn(`[timer] Schedule ${scheduleId}: sem telegram_chat_id, pulando.`);
    return;
  }

  if (group.group_members) {
    group.group_members = group.group_members.map((m) => ({
      ...m,
      accounts: m.accounts ? (accountCache.get(m.accounts.id) ?? m.accounts) : null,
    }));
  }

  console.log(`[timer] ⚡ Disparando schedule ${scheduleId} às ${nowISO}`);

  if (group.group_type === "open") {
    const listenMap: Map<string, AbortController> = (globalThis as any).__listenMap ??= new Map();

    if (listenMap.has(group.id)) {
      console.log(`[timer] Schedule ${scheduleId}: listener já ativo para grupo ${group.id} — ignorando`);
      return;
    }

    const firstAccount = (group.group_members ?? [])
      .filter((m) => m.is_active && m.accounts?.is_active)
      .sort((a, b) => a.position - b.position)[0]?.accounts ?? null;

    if (!firstAccount) {
      console.warn(`[timer] Schedule ${scheduleId}: nenhuma conta ativa no grupo — abortando.`);
      return;
    }

    startScheduledGroupListener(schedule, group, firstAccount);

    await supabase.from("schedules").update({
      retry_until:         new Date(now.getTime() + OPEN_GROUP_LISTEN_TIMEOUT_MS).toISOString(),
      last_attempt_at:     nowISO,
      last_attempt_status: "waiting_admin",
      last_attempt_error:  null,
    }).eq("id", scheduleId);

    console.log(`[timer] 👂 Schedule ${scheduleId}: listener não-bloqueante iniciado (grupo aberto ${group.id})`);
    return;
  }

  // Em ciclos frescos (sem retry_until) não há nada enviado ainda —
  // skip da query de dedup elimina ~100ms do caminho crítico.
  const alreadySent = schedule.retry_until
    ? await getAlreadySentAccountIds(schedule)
    : new Set<string>();

  if (alreadySent.size > 0) {
    console.log(`[dedup] ${alreadySent.size} account(s) já enviaram neste ciclo — serão pulados.`);
  }

  const results = await processMembersOf(schedule, alreadySent);

  const sentForMonitor = results
    .filter((r) => r.status === "sent")
    .map((r) => {
      const member = (group.group_members ?? []).find((m) => m.accounts?.id === r.account_id);
      return { account_id: r.account_id, message_text: member?.message_text ?? "" };
    })
    .filter((r) => r.message_text);

  if (sentForMonitor.length > 0) {
    monitorPositions(
      group.telegram_chat_id,
      sentForMonitor,
      scheduleId,
      now,
      group.group_type ?? "closed"
    ).catch((err) => console.error("[monitor] Erro não capturado:", err.message));
  }

  const sentCount         = results.filter((r) => r.status === "sent").length;
  const skippedCount      = results.filter((r) => r.status === "skipped").length;
  const retryableFailures = results.filter((r) => r.status === "failed" && r.retryable);
  const permanentFailures = results.filter((r) => r.status === "failed" && !r.retryable);
  const totalDelivered    = sentCount + skippedCount;

  const hasActiveMembers = (group.group_members ?? []).some(
    (m) => m.is_active && m.accounts?.is_active
  );
  const allSucceeded =
    hasActiveMembers &&
    retryableFailures.length === 0 &&
    permanentFailures.length === 0 &&
    totalDelivered > 0;

  if (allSucceeded) {
    let nextRun: string;
    try {
      nextRun = nextWeeklyOccurrence(schedule.cron_expression);
    } catch (err) {
      console.error(`[timer] cron inválido no schedule ${scheduleId}, desativando:`, err);
      await supabase.from("schedules").update({ is_active: false }).eq("id", scheduleId);
      return;
    }

    // Update do schedule em background — mensagem já foi enviada, isso não é crítico
    supabase.from("schedules").update({
      next_run_at:         nextRun,
      last_run_at:         nowISO,
      retry_until:         null,
      retry_count:         0,
      last_attempt_at:     nowISO,
      last_attempt_status: "sent",
      last_attempt_error:  null,
    }).eq("id", scheduleId).then(({ error: e }) => {
      if (e) console.error(`[timer] Falha ao atualizar schedule ${scheduleId}:`, e.message);
    });

    console.log(`[timer] ✓ Schedule ${scheduleId} OK. Próxima: ${nextRun}`);
    scheduleTimer(scheduleId, nextRun);

  } else {
    const newRetryCount  = schedule.retry_count + 1;
    const isFirstFailure = !schedule.retry_until;
    const retryUntil     = isFirstFailure
      ? new Date(now.getTime() + schedule.retry_window_seconds * 1000).toISOString()
      : schedule.retry_until!;

    const failedErrors = results
      .filter((r) => r.status === "failed" && r.error)
      .map((r) => `[${r.account_id}] ${r.error}`)
      .join("; ");

    await supabase.from("schedules").update({
      retry_until:         retryUntil,
      retry_count:         newRetryCount,
      last_attempt_at:     nowISO,
      last_attempt_status: "retrying",
      last_attempt_error:  failedErrors || null,
    }).eq("id", scheduleId);

    const intervalNext = calcRetryInterval(
      newRetryCount,
      schedule.retry_interval_seconds,
      schedule.retry_interval_max_seconds
    );

    console.warn(
      `[timer] ⚠ Schedule ${scheduleId}: ${retryableFailures.length} falha(s) retryável(eis), ` +
      `${permanentFailures.length} permanente(s). ` +
      `Retry #${newRetryCount} em ~${intervalNext}s (até ${retryUntil})`
    );

    const retryAt = new Date(now.getTime() + intervalNext * 1000);
    if (retryAt < new Date(retryUntil)) {
      scheduleTimer(scheduleId, retryAt.toISOString());
    }
  }
}

/* ─── Precision timers ─── */
const scheduledTimers = new Map<string, ReturnType<typeof setTimeout>>();

function scheduleTimer(scheduleId: string, nextRunAt: string): void {
  const delay = new Date(nextRunAt).getTime() - Date.now();

  if (delay < -5_000) {
    console.warn(`[timer] Schedule ${scheduleId} ignorado — next_run_at muito no passado (${nextRunAt})`);
    return;
  }

  const existing = scheduledTimers.get(scheduleId);
  if (existing) clearTimeout(existing);

  // Limpa pre-fetch anterior se houver
  const existingPrefetch = prefetchTimers.get(scheduleId);
  if (existingPrefetch) { clearTimeout(existingPrefetch); prefetchTimers.delete(scheduleId); }

  const effectiveDelay = Math.max(0, delay);

  // Pre-fetch: carrega o schedule no cache PREFETCH_BEFORE_MS antes do fire.
  // Quando fireSchedule rodar, não há nenhuma query bloqueante no caminho crítico.
  if (effectiveDelay > PREFETCH_BEFORE_MS) {
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
        // Injeta contas do accountCache (mais frescos)
        if (s.groups?.group_members) {
          s.groups.group_members = s.groups.group_members.map((m) => ({
            ...m,
            accounts: m.accounts ? (accountCache.get(m.accounts.id) ?? m.accounts) : null,
          }));
        }
        schedulePrefetchCache.set(scheduleId, s);
        console.log(`[prefetch] ✅ Schedule ${scheduleId} pré-carregado — iniciando peer pre-warm`);

        // Pre-aquece conexão TCP + entity cache (dialogs) + peer cache para cada conta ativa.
        const chatId      = s.groups?.telegram_chat_id;
        const activeAccts = (s.groups?.group_members ?? []).filter(
          (m) => m.is_active && m.accounts?.is_active && m.accounts?.session_string
        );
        if (chatId && activeAccts.length > 0) {
          Promise.allSettled(
            activeAccts.map(async (m) => {
              const acct = m.accounts!;
              try {
                // 1. Garante client conectado
                const cli = await clientPool.get(acct);
                // 2. Aguarda getDialogs terminar (até 600ms) — popula entity cache interno do client
                await clientPool.awaitDialogsReady(acct.id, 600);
                // 3. Resolve e cacheia o peer — hit garantido no fire path
                await getOrResolvePeer(cli, chatId, acct.id);
                console.log(`[prefetch] 🔌 ${acct.phone_number} → peer ${chatId} pre-aquecido`);
              } catch (e: any) {
                console.warn(`[prefetch] Pre-warm falhou ${acct.phone_number}: ${e.message}`);
              }
            })
          ).then(() => {
            console.log(`[prefetch] 🔌 Peer pre-warm concluído para schedule ${scheduleId}`);

            // Ping MTProto nativo 500ms antes do fire.
            //
            // Motivação: servidores Telegram podem fechar conexões ociosas entre dois keepalives
            // (intervalo de 45s). O ping garante que a conexão está viva imediatamente antes do
            // disparo, eliminando a reconexão (~200ms) que aparecia nos logs como:
            //   [connection closed] / [Connection closed while receiving data]
            //
            // Usamos Api.Ping (MTProto layer) em vez de getMe() por ser um pacote mínimo
            // sem serialização de objeto — menor overhead, resposta mais rápida.
            // Aumentado de 200ms para 500ms para dar margem em links com maior RTT
            // (Railway Amsterdam → Telegram DC).
            const msUntilFire = new Date(s.next_run_at).getTime() - Date.now();
            const pingDelay   = Math.max(0, msUntilFire - 500);
            setTimeout(async () => {
              await Promise.allSettled(
                activeAccts.map(async (m) => {
                  try {
                    const cli = await clientPool.get(m.accounts!);
                    // Ping MTProto nativo — pacote mínimo, sem overhead de objeto
                    await cli.invoke(new Api.Ping({ pingId: bigInt(Date.now()) }));
                    console.log(`[prefetch] 🏓 Ping MTProto OK ${m.accounts!.phone_number}`);
                  } catch {
                    // Se o ping falhar, tenta reconectar proativamente para não pegar
                    // a reconexão no caminho crítico do fire
                    try {
                      await clientPool.reload(m.accounts!);
                      console.log(`[prefetch] 🔄 Reconectado proativamente ${m.accounts!.phone_number}`);
                    } catch {}
                  }
                })
              );
            }, pingDelay);
          });
        }

      } catch (err: any) {
        console.warn(`[prefetch] Falha ao pré-carregar schedule ${scheduleId}: ${err.message}`);
      }
    }, effectiveDelay - PREFETCH_BEFORE_MS);
    prefetchTimers.set(scheduleId, pt);
  }

  const timer = setTimeout(async () => {
    scheduledTimers.delete(scheduleId);
    try {
      await fireSchedule(scheduleId);
    } catch (err) {
      console.error(`[timer] Erro inesperado ao disparar schedule ${scheduleId}:`, err);
    }
  }, effectiveDelay);

  scheduledTimers.set(scheduleId, timer);

  const fireAt = new Date(Date.now() + effectiveDelay).toISOString();
  console.log(`[timer] ⏰ Schedule ${scheduleId} — dispara em ${Math.round(effectiveDelay / 1000)}s (${fireAt})`);
}

/* ─── Reload periódico ─── */
async function reloadSchedules(): Promise<void> {
  const now          = new Date();
  const nowISO       = now.toISOString();
  const lookaheadISO = new Date(now.getTime() + LOOKAHEAD_MS).toISOString();

  const [
    { data: futureSchedules },
    { data: retrySchedules  },
    { data: expiredRetries  },
  ] = await Promise.all([
    supabase
      .from("schedules")
      .select("id, next_run_at")
      .eq("is_active", true)
      .is("retry_until", null)
      .lte("next_run_at", lookaheadISO),

    supabase
      .from("schedules")
      .select(`
        id, cron_expression, user_id, group_id, next_run_at,
        retry_window_seconds, retry_interval_seconds, retry_interval_max_seconds,
        retry_count, retry_until, last_attempt_at,
        groups(id, name, telegram_chat_id, telegram_chat_name, group_type,
          group_members(id, message_text, position, is_active,
            accounts(id, name, phone_number, api_id, api_hash, session_string, is_active)))
      `)
      .eq("is_active", true)
      .not("retry_until", "is", null)
      .gt("retry_until", nowISO),

    supabase
      .from("schedules")
      .select("id, cron_expression, group_id")
      .eq("is_active", true)
      .not("retry_until", "is", null)
      .lte("retry_until", nowISO),
  ]);

  await Promise.all(
    (expiredRetries ?? []).map(async (expired) => {
      console.warn(`[reload] Schedule ${expired.id}: retry expirou. Avançando para próxima semana.`);

      const listenMap: Map<string, AbortController> = (globalThis as any).__listenMap ??= new Map();
      const expGroupId = (expired as any).group_id as string | undefined;
      if (expGroupId) {
        const ctrl = listenMap.get(expGroupId);
        if (ctrl) {
          ctrl.abort();
          listenMap.delete(expGroupId);
          console.log(`[reload] Listener abortado para grupo ${expGroupId} (schedule ${expired.id} expirou)`);
        }
      }
      let nextRun: string;
      try {
        nextRun = nextWeeklyOccurrence(expired.cron_expression);
      } catch {
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
      scheduleTimer(expired.id, nextRun);
    })
  );

  for (const s of futureSchedules ?? []) {
    if (!scheduledTimers.has(s.id)) {
      scheduleTimer(s.id, s.next_run_at);
    }
  }

  for (const s of retrySchedules ?? []) {
    const schedule = s as unknown as Schedule;

    const listenMap: Map<string, AbortController> = (globalThis as any).__listenMap ??= new Map();
    if (listenMap.has(schedule.group_id)) {
      continue;
    }

    if (isRetryDue(schedule, now) && !scheduledTimers.has(schedule.id)) {
      console.log(`[reload] Schedule ${schedule.id} em retry — disparando agora.`);
      fireSchedule(schedule.id).catch((err) =>
        console.error(`[reload] Erro no retry do schedule ${schedule.id}:`, err)
      );
    }
  }
}

/* ─── Pre-warm ─── */
let prewarmRunning = false;
async function prewarmAccounts(): Promise<void> {
  if (prewarmRunning) return;
  prewarmRunning = true;
  try {
    const { data, error } = await supabase
      .from("accounts")
      .select("id, name, phone_number, api_id, api_hash, session_string, is_active")
      .eq("is_active", true);

    if (error) {
      console.warn("[prewarm] Falha ao buscar contas:", error.message);
      return;
    }

    const accounts = (data ?? []) as Account[];
    for (const account of accounts) accountCache.set(account.id, account);

    await Promise.allSettled(
      accounts.map(async (account) => {
        try {
          await clientPool.get(account);
        } catch (err: any) {
          const authDead =
            err.message?.includes("AUTH_KEY_UNREGISTERED") ||
            err.message?.includes("USER_DEACTIVATED") ||
            err.message?.includes("SESSION_REVOKED");
          if (authDead) {
            console.warn(`[prewarm] Sessão morta para ${account.phone_number} — desativando no banco.`);
            await supabase.from("accounts").update({ is_active: false }).eq("id", account.id);
          }
        }
      })
    );
  } finally {
    prewarmRunning = false;
  }
}

/* ─── HTTP server interno ─── */
const WORKER_PORT   = parseInt(process.env.PORT ?? "3001", 10);
const WORKER_SECRET = process.env.WORKER_SECRET ?? "";

function jsonResponse(res: http.ServerResponse, status: number, body: unknown) {
  const payload = JSON.stringify(body);
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(payload);
}

const httpServer = http.createServer(async (req, res) => {
  if (WORKER_SECRET && req.headers["x-worker-secret"] !== WORKER_SECRET) {
    return jsonResponse(res, 401, { error: "Unauthorized" });
  }

  const url = new URL(req.url ?? "/", `http://localhost:${WORKER_PORT}`);

  // ── GET /accounts/:id/chats ──────────────────────────────────────────────
  const chatsMatch = url.pathname.match(/^\/accounts\/([^/]+)\/chats$/);
  if (req.method === "GET" && chatsMatch) {
    const accountId = chatsMatch[1];
    const account   = accountCache.get(accountId);

    if (!account) {
      return jsonResponse(res, 404, { error: "Conta não encontrada no cache do worker" });
    }

    try {
      const client  = await clientPool.get(account);
      const dialogs = await client.getDialogs({ limit: 200 });
      const chats   = dialogs
        .filter((d) => d.isGroup || d.isChannel)
        .map((d) => ({
          id:          String(d.id),
          name:        d.title ?? d.name ?? "Sem nome",
          type:        d.isChannel ? "channel" : "group",
          accessHash:  null,
        }))
        .sort((a, b) => a.name.localeCompare(b.name));

      return jsonResponse(res, 200, chats);
    } catch (err: any) {
      console.error("[http] /chats erro:", err.message);
      return jsonResponse(res, 500, { error: err.message });
    }
  }

  // ── GET /accounts/:id/chat-count?chat_id=XXXX ───────────────────────────
  const chatCountMatch = url.pathname.match(/^\/accounts\/([^/]+)\/chat-count$/);
  if (req.method === "GET" && chatCountMatch) {
    const accountId = chatCountMatch[1];
    const chatId    = url.searchParams.get("chat_id");
    const account   = accountCache.get(accountId);

    if (!chatId)  return jsonResponse(res, 400, { error: "chat_id é obrigatório" });
    if (!account) return jsonResponse(res, 404, { error: "Conta não encontrada no cache do worker" });

    try {
      const client = await clientPool.get(account);
      const rawId  = chatId.replace(/^-100/, "").replace(/^-/, "");
      let count: number | null = null;

      try {
        const channelId = bigInt(rawId);
        const result = await client.invoke(
          new Api.channels.GetFullChannel({
            channel: new Api.InputChannel({
              channelId,
              accessHash: bigInt(0),
            }),
          })
        ) as any;
        const participantsCount = result?.fullChat?.participantsCount;
        if (typeof participantsCount === "number") {
          count = participantsCount;
          console.log(`[chat-count] ✓ GetFullChannel: ${chatId} → ${count}`);
        }
      } catch (e1: any) {
        console.warn(`[chat-count] GetFullChannel falhou para ${chatId}: ${e1.message}`);
      }

      if (count === null) {
        try {
          const dialogs = await client.getDialogs({ limit: 500 });
          const absRaw  = rawId.replace(/^100/, "");
          const dialog  = dialogs.find((d) => {
            const dIdStr = String(d.id).replace(/^-/, "");
            return dIdStr === rawId ||
                   dIdStr === absRaw ||
                   String(d.id) === chatId ||
                   `-100${dIdStr}` === chatId ||
                   `-${dIdStr}` === chatId;
          });
          if (dialog?.entity) {
            const ent = dialog.entity as any;
            if (typeof ent.participantsCount === "number") {
              count = ent.participantsCount;
              console.log(`[chat-count] ✓ dialog.entity.participantsCount: ${chatId} → ${count}`);
            }
          }
          if (count === null && dialog) {
            const participants = (dialog as any).participantsCount;
            if (typeof participants === "number") {
              count = participants;
              console.log(`[chat-count] ✓ dialog.participantsCount: ${chatId} → ${count}`);
            }
          }
        } catch (e2: any) {
          console.warn(`[chat-count] dialog fallback falhou para ${chatId}: ${e2.message}`);
        }
      }

      if (count === null) {
        try {
          const absId     = rawId.replace(/^100/, "");
          const numericId = bigInt(absId);
          const full      = await client.invoke(new Api.messages.GetFullChat({ chatId: numericId })) as any;
          if (typeof full?.fullChat?.participantsCount === "number") {
            count = full.fullChat.participantsCount;
            console.log(`[chat-count] ✓ GetFullChat.participantsCount: ${chatId} → ${count}`);
          } else {
            const parts = full?.fullChat?.participants;
            if (parts?.participants) {
              count = parts.participants.length;
              console.log(`[chat-count] ✓ GetFullChat.participants.length: ${chatId} → ${count}`);
            }
          }
        } catch (e3: any) {
          console.warn(`[chat-count] GetFullChat falhou para ${chatId}: ${e3.message}`);
        }
      }

      return jsonResponse(res, 200, { count });
    } catch (err: any) {
      console.error("[http] /chat-count erro:", err.message);
      return jsonResponse(res, 500, { error: err.message });
    }
  }

  // ── GET /accounts/:id/chat-members?chat_id=XXXX ─────────────────────────
  const membersMatch = url.pathname.match(/^\/accounts\/([^/]+)\/chat-members$/);
  if (req.method === "GET" && membersMatch) {
    const accountId = membersMatch[1];
    const chatId    = url.searchParams.get("chat_id");
    const account   = accountCache.get(accountId);

    if (!chatId)    return jsonResponse(res, 400, { error: "chat_id é obrigatório" });
    if (!account)   return jsonResponse(res, 404, { error: "Conta não encontrada no cache do worker" });

    type MemberOut = { id: string; name: string | null; username: string | null; phone: string | null };

    try {
      const client       = await clientPool.get(account);
      const rawId        = chatId.replace(/^-/, "");
      const isSupergroup = chatId.startsWith("-100");
      let members: MemberOut[] = [];

      if (isSupergroup) {
        try {
          const dialogs = await client.getDialogs({ limit: 500 });
          const dialog  = dialogs.find((d) => {
            const dId = String(d.id);
            return dId === rawId || dId === chatId || dId === rawId.replace(/^100/, "");
          });
          const entity = dialog?.entity;
          if (entity && (entity.className === "Channel" || entity.className === "Chat")) {
            const result = await client.invoke(
              new Api.channels.GetParticipants({
                channel: entity as Api.Channel,
                filter:  new Api.ChannelParticipantsRecent(),
                offset:  0,
                limit:   200,
                hash:    bigInt(0),
              })
            );
            if (result.className === "channels.ChannelParticipants") {
              members = result.users
                .filter((u): u is Api.User => u.className === "User" && !u.bot)
                .map((u) => ({
                  id:       String(u.id),
                  name:     [u.firstName, u.lastName].filter(Boolean).join(" ") || null,
                  username: u.username ? `@${u.username}` : null,
                  phone:    u.phone ? `+${u.phone}` : null,
                }));
            }
          }
        } catch { /* tenta estratégia 2 */ }
      }

      if (members.length === 0) {
        try {
          const numericId = bigInt(rawId);
          const full      = await client.invoke(new Api.messages.GetFullChat({ chatId: numericId }));
          const chatFull  = full.fullChat as Api.ChatFull;
          const parts     = chatFull.participants;
          if (parts && parts.className === "ChatParticipants") {
            const userMap = new Map<string, Api.User>();
            for (const u of full.users) {
              if (u.className === "User") userMap.set(String(u.id), u as Api.User);
            }
            members = parts.participants
              .map((p) => {
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
        } catch { /* silencia */ }
      }

      members.sort((a, b) => (a.name ?? "").localeCompare(b.name ?? ""));
      return jsonResponse(res, 200, members);
    } catch (err: any) {
      console.error("[http] /chat-members erro:", err.message);
      return jsonResponse(res, 500, { error: err.message });
    }
  }

  // ── POST /accounts/:id/reload ────────────────────────────────────────────
  const reloadMatch = url.pathname.match(/^\/accounts\/([^/]+)\/reload$/);
  if (req.method === "POST" && reloadMatch) {
    const accountId = reloadMatch[1];

    const { data: row, error } = await supabase
      .from("accounts")
      .select("id, name, phone_number, api_id, api_hash, session_string, is_active")
      .eq("id", accountId)
      .single();

    if (error || !row) {
      return jsonResponse(res, 404, { error: "Conta não encontrada" });
    }

    const account = row as Account;
    accountCache.set(accountId, account);

    if (!account.is_active || !account.session_string) {
      return jsonResponse(res, 200, { ok: true, skipped: true, reason: "conta inativa ou sem sessão" });
    }

    try {
      await clientPool.reload(account);
      console.log(`[http] /reload ✓ conta ${account.phone_number} recarregada no pool`);
      return jsonResponse(res, 200, { ok: true });
    } catch (err: any) {
      console.error("[http] /reload erro:", err.message);
      return jsonResponse(res, 500, { error: err.message });
    }
  }

  // ── POST/DELETE /groups/:id/listen ──────────────────────────────────────
  const listenMap: Map<string, AbortController> = (globalThis as any).__listenMap ??= new Map();

  const listenMatch = url.pathname.match(/^\/groups\/([^/]+)\/listen$/);
  if (listenMatch) {
    const groupId = listenMatch[1];

    if (req.method === "DELETE") {
      const ctrl = listenMap.get(groupId);
      if (ctrl) {
        ctrl.abort();
        listenMap.delete(groupId);
        console.log(`[listen] ⏹ Escuta cancelada para grupo ${groupId}`);
      }
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

          if (!grpRow) {
            console.warn(`[listen] Grupo ${groupId} não encontrado`);
            return;
          }

          const chatId = String(grpRow.telegram_chat_id);
          const members: GroupMember[] = (grpRow.group_members ?? []).map((m: any) => ({
            ...m,
            accounts: Array.isArray(m.accounts) ? (m.accounts[0] ?? null) : (m.accounts ?? null),
          }));
          const firstMember = members.find((m) => m.is_active && m.accounts?.is_active);

          if (!firstMember?.accounts) {
            console.warn(`[listen] Nenhuma conta ativa no grupo ${groupId}`);
            return;
          }

          const account = accountCache.get(firstMember.accounts.id) ?? firstMember.accounts as unknown as Account;
          const client  = await clientPool.get(account);

          const MANUAL_LISTEN_TIMEOUT_MS = 2 * 60 * 60_000;
          const deadline    = Date.now() + MANUAL_LISTEN_TIMEOUT_MS;
          const startUnix   = Math.floor((Date.now() - 10_000) / 1000);
          let lastSeenMsgId = 0;

          try { await getOrResolvePeer(client, chatId, account.id); } catch {}

          console.log(`[listen] 👂 Aguardando OK da admin em ${chatId} (grupo ${groupId})`);

          while (Date.now() < deadline && !ctrl.signal.aborted) {
            try {
              const peer   = await getOrResolvePeer(client, chatId, account.id);
              const result = await client.invoke(
                new Api.messages.GetHistory({
                  peer: peer as any, limit: 10,
                  offsetDate: 0, offsetId: 0, maxId: 0, minId: 0,
                  hash: bigInt(0), addOffset: 0,
                })
              ) as any;

              const recentMsgs = (result.messages ?? []).filter(
                (m: any) => (m.className === "Message" || m._ === "message") && m.date >= startUnix && m.id > lastSeenMsgId
              );

              if (recentMsgs.length > 0) {
                lastSeenMsgId = Math.max(lastSeenMsgId, ...recentMsgs.map((m: any) => m.id as number));
              }

              const gotSignal = recentMsgs.some((m: any) => {
                const text    = typeof m.message === "string" ? m.message.trim().toLowerCase() : "";
                const isOk    = text === "ok";
                const isMedia = m.media != null && m.media.className !== "MessageMediaEmpty";
                return isOk || isMedia;
              });

              if (gotSignal && !ctrl.signal.aborted) {
                console.log(`[listen] ✓ Sinal da admin detectado para grupo ${groupId} — disparando`);
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
                const alreadySent  = new Set<string>();
                const results      = await processMembersOf(scheduleStub as any, alreadySent);

                const sentForMonitor = results
                  .filter((r) => r.status === "sent")
                  .map((r) => {
                    const member = members.find((m) => m.accounts?.id === r.account_id);
                    return { account_id: r.account_id, message_text: member?.message_text ?? "" };
                  })
                  .filter((r) => r.message_text);

                if (sentForMonitor.length > 0) {
                  monitorPositions(chatId, sentForMonitor, scheduleStub.id, dispatchedAt, "open")
                    .catch((err) => console.error("[listen] Erro no monitoramento:", err.message));
                }

                const sent = results.filter((r) => r.status === "sent").length;
                console.log(`[listen] ✓ Disparo manual concluído para grupo ${groupId}: ${sent} enviadas`);
                return;
              }
            } catch (err: any) {
              if (!ctrl.signal.aborted) {
                console.warn(`[listen] Erro ao buscar histórico para ${groupId}: ${err.message}`);
              }
            }

            if (!ctrl.signal.aborted) {
              await new Promise((r) => setTimeout(r, LISTEN_POLL_MS));
            }
          }

          if (ctrl.signal.aborted) {
            console.log(`[listen] ⏹ Escuta abortada para grupo ${groupId}`);
          } else {
            console.warn(`[listen] ⏰ Timeout — nenhum OK recebido para grupo ${groupId}`);
            await supabase.from("groups").update({ listener_session_id: null }).eq("id", groupId);
          }
          listenMap.delete(groupId);
        } catch (err: any) {
          console.error(`[listen] Erro inesperado para grupo ${groupId}:`, err.message);
          listenMap.delete(groupId);
        }
      })();

      return jsonResponse(res, 200, { ok: true });
    }
  }

  jsonResponse(res, 404, { error: "Not found" });
});

httpServer.listen(WORKER_PORT, () => {
  console.log(`[worker] HTTP interno escutando na porta ${WORKER_PORT}`);
});

/* ─── Inicialização ─── */
async function init(): Promise<void> {
  console.log("[worker] Iniciando...");
  await prewarmAccounts();
  await reloadSchedules();

  setInterval(async () => {
    try {
      await Promise.allSettled([reloadSchedules(), prewarmAccounts()]);
    } catch (err) {
      console.error("[reload] Erro no reload periódico:", err);
    }
  }, RELOAD_INTERVAL_MS);

  console.log("[worker] Pronto.");
}

init().catch((err) => {
  console.error("[worker] Falha na inicialização:", err);
  process.exit(1);
});

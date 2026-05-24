// worker-flat.ts — dispatch worker Telegram, sem camadas de abstração
//
// Fix v1: firingNow Set previne duplo disparo quando reloadSchedules
//         roda enquanto fireSchedule ainda está executando (race condition
//         entre RETRY_BUDGET_MS=50s e RELOAD_INTERVAL_MS=30s)
//
// Fix v2 (2025-05):
//   BUG #1 — reconnect loop infinito:
//     _updateLoop substituído por receiveUpdates:false (flag nativa do GramJS).
//     A supressão manual quebrava o fluxo de sync de estado pós-reconexão,
//     causando "Not connected" em loop no _recvLoop.
//
//   BUG #2 — peerCache stale após reconexão:
//     Ao desconectar/reconectar um account, todos os peers desse account
//     são removidos do cache. accessHash gerado com sessão anterior é inválido.
//
//   BUG #3 — sem delay entre tentativas no sendMessage:
//     Loop de retry sem espera causava dezenas de tentativas por segundo
//     quando o client estava instável. Adicionado backoff exponencial
//     (1s → 2s → 4s → 8s) entre tentativas.
//
//   BUG #4 — getDialogs warm-up não era awaited:
//     getDialogs() disparado com .then() dentro de getClient() corria
//     em paralelo com o primeiro resolvePeer(), que encontrava cache vazio
//     e ia para a estratégia lenta (getDialogs de 200 itens) no caminho
//     crítico. Warm-up movido para prewarmAccounts() com await.

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

// Quanto tempo cada tentativa de envio pode durar antes de fazer timeout
const SEND_TIMEOUT_MS = 15_000;

// Budget total de retentativas para enviar a mensagem de um membro.
// Se não enviou nesse tempo, desiste e marca como "failed".
// IMPORTANTE: esse valor (50s) é maior que RELOAD_INTERVAL_MS (30s),
// o que causava o bug de duplo disparo. O firingNow resolve isso.
const RETRY_BUDGET_MS = 50_000;

// Com que frequência o worker relê o banco para encontrar schedules novos
const RELOAD_INTERVAL_MS = 30_000;

// Janela futura que o reload considera ao buscar schedules próximos
const LOOKAHEAD_MS = 2 * 60 * 1000;

// Frequência do ping de keepalive para manter conexões Telegram vivas
const KEEPALIVE_INTERVAL_MS = 45_000;

// Quanto aguardar após o envio antes de ler o histórico (grupos fechados).
// Dá tempo para o Telegram processar e ordenar as mensagens.
const MONITOR_DELAY_CLOSED_MS = 6_000;

// Quanto tempo no máximo monitorar posição em grupos abertos
const MONITOR_MAX_OPEN_MS = 5 * 60_000;

// Intervalo de polling ao monitorar posição
const MONITOR_POLL_MS = 5_000;

// Intervalo de polling ao aguardar sinal do admin ("ok" ou mídia)
const LISTEN_POLL_MS = 400;

// Quantas mensagens buscar do histórico ao monitorar posições
const MONITOR_HISTORY_LIMIT = 150;

// Quanto tempo o listener de grupo aberto espera o sinal do admin antes de desistir
const OPEN_GROUP_LISTEN_TIMEOUT_MS = 2 * 60 * 60_000; // 2 horas

// FIX #3: Backoff máximo entre tentativas de envio (ms)
const SEND_RETRY_BACKOFF_MAX_MS = 8_000;

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
  position: number;          // ordem de disparo dentro do grupo
  is_active: boolean;
  accounts: Account | null;
}

interface Group {
  id: string;
  name: string;
  telegram_chat_id: string | null;   // ID numérico do chat no Telegram
  telegram_chat_name: string | null;
  group_type: "open" | "closed";
  // open:   aguarda sinal do admin ("ok" ou mídia) antes de enviar
  // closed: dispara imediatamente no horário agendado
  group_members: GroupMember[];
}

interface Schedule {
  id: string;
  cron_expression: string;           // ex: "30 14 * * 2" = terça 14:30 UTC
  user_id: string;
  group_id: string;
  next_run_at: string;               // próximo disparo planejado (ISO)
  retry_window_seconds: number;      // janela total de retentativas após falha
  retry_interval_seconds: number;    // intervalo base entre retentativas (cresce exponencialmente)
  retry_interval_max_seconds: number;// teto do intervalo de retry
  retry_count: number;               // quantas retentativas já ocorreram no ciclo atual
  retry_until: string | null;        // se não-nulo: estamos em modo retry até essa data
  last_attempt_at: string | null;    // quando foi a última tentativa (usado pelo isRetryDue)
  groups: Group;
}

interface DispatchResult {
  account_id: string;
  message_text: string | null;
  status: "sent" | "failed" | "skipped";
  retryable: boolean;
  error?: string;
}

/* ─────────────────────────────────────────────────────────────────────────────
   ESTADO GLOBAL
   Tudo que precisa viver entre chamadas fica em Maps no topo.
   ───────────────────────────────────────────────────────────────────────────── */

// Conexões Telegram ativas: accountId → TelegramClient conectado
const clients = new Map<string, TelegramClient>();

// Qual session_string foi usada para abrir cada client.
// Permite detectar se a sessão mudou e reconectar.
const sessions = new Map<string, string>();

// Timers de keepalive por conta: accountId → setInterval handle
const keepaliveTimers = new Map<string, ReturnType<typeof setInterval>>();

// Cache de peers Telegram resolvidos: "accountId:chatId" → InputPeer
// Evita re-resolver o mesmo chat em todo envio (operação potencialmente cara)
const peerCache = new Map<string, unknown>();

// Cache de contas do banco: accountId → Account
const accountCache = new Map<string, Account>();

// Timers de disparo agendado: scheduleId → setTimeout handle
const scheduledTimers = new Map<string, ReturnType<typeof setTimeout>>();

// Listeners de grupos abertos: groupId → AbortController
const listenMap = new Map<string, AbortController>();

// FIX v1: schedules atualmente em execução — previne duplo disparo
const firingNow = new Set<string>();

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

/* ─────────────────────────────────────────────────────────────────────────────
   HELPERS INTERNOS DE PEER CACHE
   ───────────────────────────────────────────────────────────────────────────── */

// FIX #2: Limpa todos os peers de um account do cache.
// Chamado sempre que o client é desconectado/reconectado, porque
// o accessHash do peer é vinculado à sessão anterior e fica inválido.
function evictPeersForAccount(accountId: string): void {
  for (const key of peerCache.keys()) {
    if (key.startsWith(`${accountId}:`)) {
      peerCache.delete(key);
    }
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   GERENCIAMENTO DE CONEXÕES TELEGRAM
   ───────────────────────────────────────────────────────────────────────────── */

async function getClient(account: Account): Promise<TelegramClient> {
  const existing      = clients.get(account.id);
  const sessionInUse  = sessions.get(account.id);
  const sessionChanged = sessionInUse !== account.session_string;

  if (existing?.connected && !sessionChanged) return existing;

  if (existing) {
    try { await existing.disconnect(); } catch {}
    clients.delete(account.id);
    // FIX #2: limpa peers do account ao desconectar
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

  // FIX #1: Suprimir o update loop sem quebrar o fluxo de reconexão.
  //
  // Abordagem descartada — (client as any)._updateLoop = () => Promise.resolve():
  //   Substituía a função inteira, mas o GramJS chama _updateLoop(client) passando
  //   o client como argumento (não como método). O monkey-patch no objeto não
  //   interceptava a chamada no módulo updates.js, que mantinha sua própria
  //   referência. Resultado: o loop subia mesmo com o patch, e quando o sender
  //   ficava instável o _recvLoop lançava "Not connected" sem ter o loop para
  //   processar o estado → reconnect → "Not connected" infinito.
  //
  // Abordagem descartada — receiveUpdates: false no construtor:
  //   Não existe em TelegramClientParams. Causa TS2353 com strict mode.
  //
  // Solução correta — _loopStarted = true antes de connect():
  //   O GramJS declara _loopStarted em telegramBaseClient (linha 176 do .d.ts)
  //   e inicializa como false. Em TelegramClient.connect(), ambos os pontos
  //   que chamam _updateLoop são guardados por `if (!this._loopStarted)`.
  //   Setar true antes do connect() faz os dois guards falharem → loop nunca
  //   sobe. O flag já é typed como `protected boolean`, então o cast para `any`
  //   é necessário apenas para acesso externo — não há risco de TS2353.
  //   O fluxo de reconexão (autoReconnect, _handleReconnect) não depende desse
  //   flag, então continua funcionando normalmente.
  (client as any)._loopStarted = true;

  await client.connect();

  clients.set(account.id, client);
  sessions.set(account.id, account.session_string);

  // FIX #4: getDialogs NÃO é chamado aqui.
  // Antes ficava aqui como .then() (fire-and-forget), correndo em paralelo
  // com o primeiro resolvePeer(). Isso causava cache miss na estratégia 1
  // (getInputEntity) e forçava a estratégia 3 (getDialogs 200 itens) no
  // caminho crítico de disparo.
  // O warm-up agora é feito em prewarmAccounts() com await, antes de qualquer disparo.

  // Keepalive: pinga a conta a cada 45s para manter a conexão viva.
  const interval = setInterval(async () => {
    if (!client.connected) {
      console.warn(`[keepalive] ${account.phone_number} desconectou — removendo do pool`);
      clients.delete(account.id);
      // FIX #2: limpa peers ao detectar desconexão no keepalive
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
      // FIX #2: limpa peers ao detectar falha no keepalive
      evictPeersForAccount(account.id);
      keepaliveTimers.delete(account.id);
      clearInterval(interval);

      const authDead =
        err.message?.includes("AUTH_KEY_UNREGISTERED") ||
        err.message?.includes("USER_DEACTIVATED") ||
        err.message?.includes("SESSION_REVOKED");
      if (authDead) {
        console.warn(`[keepalive] Sessão morta: ${account.phone_number} — desativando no banco`);
        supabase.from("accounts").update({ is_active: false }).eq("id", account.id).then(({ error: e }) => {
          if (e) console.error(`[keepalive] Falha ao desativar ${account.id}:`, e.message);
        });
      }
    }
  }, KEEPALIVE_INTERVAL_MS);

  keepaliveTimers.set(account.id, interval);
  console.log(`[client] ✓ Conectado: ${account.phone_number}`);
  return client;
}

async function reloadClient(account: Account): Promise<TelegramClient> {
  const existing = clients.get(account.id);
  if (existing) {
    try { await existing.disconnect(); } catch {}
    clients.delete(account.id);
    // FIX #2: limpa peers ao fazer reload manual
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

  // Estratégia 1: getInputEntity — usa o cache interno do GramJS.
  // Funciona se o cliente já sincronizou os dialogs (warm-up em prewarmAccounts).
  try {
    const peer = await client.getInputEntity(chatIdNum);
    peerCache.set(key, peer);
    return peer;
  } catch {}

  // Estratégia 2: GetChannels via MTProto direto.
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

  // Estratégia 3: sincroniza todos os dialogs e tenta de novo.
  // Mais lento, mas resolve casos onde a conta é membro mas o grupo
  // não estava nos dialogs em cache.
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
  const budgetEnd = Date.now() + RETRY_BUDGET_MS;
  let attempt = 0;

  while (Date.now() < budgetEnd) {
    attempt++;
    const timeLeft = budgetEnd - Date.now();
    if (timeLeft < 500) break;

    try {
      await Promise.race([
        (async () => {
          const peer = await resolvePeer(client, telegramChatId, account.id);
          try {
            await client.sendMessage(peer as any, { message: messageText });
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
                await client.sendMessage(freshPeer as any, { message: messageText });
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
        // FIX #3: backoff exponencial entre tentativas.
        // Antes: tentava de novo imediatamente → dezenas de req/s com client instável.
        // Agora: 1s → 2s → 4s → 8s (teto), sempre respeitando o budget restante.
        const backoffMs = Math.min(1_000 * Math.pow(2, attempt - 1), SEND_RETRY_BACKOFF_MAX_MS);
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

            await updateScheduleAfterDispatch(schedule, results, dispatchedAt);
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
      scheduleTimer(schedule.id, nextRun);

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
        account_id:  account.id,
        message_text: member.message_text,
        status:      "skipped" as const,
        retryable:   false,
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
  now: Date
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
    scheduleTimer(schedule.id, nextRun);

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
      scheduleTimer(schedule.id, retryAt.toISOString());
    }
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   DISPARO DE SCHEDULE
   ───────────────────────────────────────────────────────────────────────────── */
async function fireSchedule(scheduleId: string): Promise<void> {
  if (firingNow.has(scheduleId)) {
    console.warn(`[fire] Schedule ${scheduleId} já em execução — ignorando disparo duplicado`);
    return;
  }
  firingNow.add(scheduleId);

  try {
    const now = new Date();

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

    const schedule = data as unknown as Schedule;
    const group    = schedule.groups;

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

    await updateScheduleAfterDispatch(schedule, results, now);

  } finally {
    firingNow.delete(scheduleId);
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   TIMER DE PRECISÃO
   ───────────────────────────────────────────────────────────────────────────── */
function scheduleTimer(scheduleId: string, nextRunAt: string): void {
  const delay = new Date(nextRunAt).getTime() - Date.now();

  if (delay < -5_000) {
    console.warn(`[timer] Schedule ${scheduleId} ignorado — muito no passado (${nextRunAt})`);
    return;
  }

  const prev = scheduledTimers.get(scheduleId);
  if (prev) clearTimeout(prev);

  const effectiveDelay = Math.max(0, delay);

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
      .select("id, next_run_at")
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
    scheduleTimer(expired.id, nextRun);
  }));

  for (const s of futureSchedules ?? []) {
    if (!scheduledTimers.has(s.id)) {
      scheduleTimer(s.id, s.next_run_at);
    }
  }

  for (const s of retrySchedules ?? []) {
    const schedule = s as unknown as Schedule;

    if (listenMap.has(schedule.group_id)) continue;

    if (
      isRetryDue(schedule, now) &&
      !scheduledTimers.has(schedule.id) &&
      !firingNow.has(schedule.id)
    ) {
      console.log(`[reload] Schedule ${schedule.id} em retry — disparando agora.`);
      fireSchedule(schedule.id).catch(err =>
        console.error(`[reload] Erro no retry do schedule ${schedule.id}:`, err)
      );
    }
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   PRE-WARM DE CONTAS
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

        // FIX #4: getDialogs é awaited aqui, no prewarm, antes de qualquer disparo.
        // Antes ficava dentro de getClient() como .then() (fire-and-forget),
        // correndo em paralelo com o primeiro resolvePeer() e causando cache miss.
        // Agora o warm-up completa antes de reloadSchedules() criar os timers.
        await client.getDialogs({ limit: 100 });
        console.log(`[prewarm] ✓ Dialogs prontos: ${account.phone_number}`);

      } catch (err: any) {
        const authDead =
          err.message?.includes("AUTH_KEY_UNREGISTERED") ||
          err.message?.includes("USER_DEACTIVATED") ||
          err.message?.includes("SESSION_REVOKED");
        if (authDead) {
          console.warn(`[prewarm] Sessão morta: ${account.phone_number} — desativando.`);
          await supabase.from("accounts").update({ is_active: false }).eq("id", account.id);
        } else {
          console.warn(`[prewarm] Falha ao conectar ${account.phone_number}: ${err.message}`);
        }
      }
    }));
  } finally {
    prewarmRunning = false;
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   HTTP SERVER — 5 ROTAS DE GERENCIAMENTO
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

  // ── GET /accounts/:id/chats ──────────────────────────────────────────────
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

  // ── GET /accounts/:id/chat-count?chat_id=XXXX ───────────────────────────
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

  // ── GET /accounts/:id/chat-members?chat_id=XXXX ─────────────────────────
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

  // ── POST /accounts/:id/reload ────────────────────────────────────────────
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
      // FIX #4: warm-up dos dialogs após reload manual também
      await client.getDialogs({ limit: 100 });
      console.log(`[http] /reload ✓ ${account.phone_number} recarregada`);
      return jsonResponse(res, 200, { ok: true });
    } catch (err: any) {
      console.error("[http] /reload erro:", err.message);
      return jsonResponse(res, 500, { error: err.message });
    }
  }

  // ── POST/DELETE /groups/:id/listen ──────────────────────────────────────
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
   Sequência de boot:
   1. prewarmAccounts: conecta todas as contas, awaita getDialogs (FIX #4),
      popula accountCache
   2. reloadSchedules: lê o banco e cria os timers iniciais
   3. setInterval: mantém o ciclo de reload a cada 30s
   ───────────────────────────────────────────────────────────────────────────── */
async function init(): Promise<void> {
  console.log("[worker] Iniciando...");
  await prewarmAccounts();  // FIX #4: getDialogs awaited aqui antes de criar timers
  await reloadSchedules();
  setInterval(async () => {
    try { await Promise.allSettled([reloadSchedules(), prewarmAccounts()]); }
    catch (err) { console.error("[reload] Erro no reload periódico:", err); }
  }, RELOAD_INTERVAL_MS);
  console.log("[worker] Pronto.");
}

init().catch(err => {
  console.error("[worker] Falha na inicialização:", err);
  process.exit(1);
});

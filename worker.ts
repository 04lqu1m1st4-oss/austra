// worker-flat.ts — dispatch worker Telegram, sem camadas de abstração
//
// Fix v1: firingNow Set previne duplo disparo quando reloadSchedules
//         roda enquanto fireSchedule ainda está executando (race condition
//         entre RETRY_BUDGET_MS=50s e RELOAD_INTERVAL_MS=30s)
//
// Fix v2 (2025-05):
//   BUG #1 — reconnect loop infinito:
//     _loopStarted=true antes de connect() suprime o updateLoop sem quebrar
//     o fluxo de reconexão (autoReconnect, _handleReconnect não dependem do flag).
//
//   BUG #2 — peerCache stale após reconexão:
//     Ao desconectar/reconectar um account, todos os peers desse account
//     são removidos do cache. accessHash gerado com sessão anterior é inválido.
//
//   BUG #3 — sem delay entre tentativas no sendMessage:
//     Adicionado backoff exponencial (1s → 2s → 4s → 8s) entre tentativas.
//
//   BUG #4 — getDialogs warm-up não era awaited:
//     Warm-up movido para prewarmAccounts() com await + pre-resolve de peers.
//
// Fix v3 (2025-05):
//   BUG #5 — AUTH_KEY_DUPLICATED (406) via race condition em getClient():
//     connectingPromises Map<accountId, Promise<TelegramClient>> serializa
//     conexões concorrentes para o mesmo account.
//
// Otimizações v4 (2025-05) — latência mínima no caminho crítico:
//
//   OPT #1 — invoke direto em vez de client.sendMessage():
//     client.sendMessage() de alto nível chama getInputEntity internamente
//     mesmo quando recebe um InputPeer já resolvido — adiciona ~5-20ms por
//     envio. client.invoke(new Api.messages.SendMessage(...)) com peer já
//     resolvido bypassa toda essa lógica e vai direto ao MTProto.
//
//   OPT #2 — pre-fetch do schedule antes do fire:
//     scheduleTimer agenda um segundo timeout PREFETCH_BEFORE_MS antes do
//     disparo real. Esse timeout faz a query Supabase e guarda o resultado
//     em schedulePrefetchCache. Quando fireSchedule executa, consome o cache
//     instantaneamente — a query deixa de estar no caminho crítico (~50-150ms).
//
//   OPT #3 — pre-resolve de peers no prewarm:
//     Após getDialogs(), prewarmAccounts() chama resolvePeer() para todos os
//     telegram_chat_id de grupos ativos. O peerCache fica populado antes do
//     primeiro disparo — elimina a Estratégia 3 (getDialogs 200 itens) do
//     caminho crítico.
//
//   OPT #4 — noWebpage: true + randomId único por tentativa:
//     noWebpage evita que o Telegram tente gerar preview (RTT extra).
//     randomId diferente em cada tentativa evita que o Telegram dedup uma
//     mensagem que falhou e está sendo reenviada.

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
// IMPORTANTE: maior que RELOAD_INTERVAL_MS (30s) — o firingNow resolve o duplo disparo.
const RETRY_BUDGET_MS = 50_000;

// Com que frequência o worker relê o banco para encontrar schedules novos
const RELOAD_INTERVAL_MS = 30_000;

// Janela futura que o reload considera ao buscar schedules próximos
const LOOKAHEAD_MS = 2 * 60 * 1000;

// Frequência do ping de keepalive para manter conexões Telegram vivas
const KEEPALIVE_INTERVAL_MS = 45_000;

// OPT #2: quanto tempo antes do fire fazer o pre-fetch do schedule no banco
const PREFETCH_BEFORE_MS = 800;

// Quanto aguardar após o envio antes de ler o histórico (grupos fechados)
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

// Backoff máximo entre tentativas de envio (ms)
const SEND_RETRY_BACKOFF_MAX_MS = 8_000;

// Intervalo de polling para detectar abertura de grupo fechado (ms)
// Ajuste conservador: se vier FLOOD_WAIT cai para CLOSED_GROUP_POLL_FLOOD_FALLBACK_MS
const CLOSED_GROUP_POLL_MS = 10;
const CLOSED_GROUP_POLL_FLOOD_FALLBACK_MS = 50;

// Janela de espera ao redor do horário programado para grupo fechado
// Após esse tempo sem abertura, desiste e avança para próxima semana
const CLOSED_GROUP_WATCH_WINDOW_MS = 30_000;

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

/* ─────────────────────────────────────────────────────────────────────────────
   ESTADO GLOBAL
   ───────────────────────────────────────────────────────────────────────────── */

// Conexões Telegram ativas: accountId → TelegramClient
const clients = new Map<string, TelegramClient>();

// session_string usada em cada client — detecta mudança de sessão
const sessions = new Map<string, string>();

// Timers de keepalive: accountId → setInterval handle
const keepaliveTimers = new Map<string, ReturnType<typeof setInterval>>();

// Cache de peers resolvidos: "accountId:chatId" → InputPeer
// OPT #3: populado no prewarm antes do primeiro disparo
const peerCache = new Map<string, unknown>();

// Cache de contas: accountId → Account
const accountCache = new Map<string, Account>();

// Timers de disparo: scheduleId → setTimeout handle
const scheduledTimers = new Map<string, ReturnType<typeof setTimeout>>();

// OPT #2: timers de pre-fetch: scheduleId → setTimeout handle
const prefetchTimers = new Map<string, ReturnType<typeof setTimeout>>();

// OPT #2: schedules pre-carregados prontos para uso instantâneo no fire
const schedulePrefetchCache = new Map<string, Schedule>();

// Listeners de grupos abertos: groupId → AbortController
const listenMap = new Map<string, AbortController>();

// FIX v1: schedules atualmente em execução — previne duplo disparo
const firingNow = new Set<string>();

// FIX BUG #5: mutex de conexão por account — previne AUTH_KEY_DUPLICATED
const connectingPromises = new Map<string, Promise<TelegramClient>>();

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

// Gera um randomId único para cada invoke de SendMessage.
// OPT #4: randomId diferente por tentativa evita que o Telegram dedup
// uma mensagem que falhou e está sendo reenviada com o mesmo ID.
function makeRandomId(): bigInt.BigInteger {
  // 52 bits de entropia (safe para JS number, único na prática)
  return bigInt(Date.now()).multiply(bigInt(1000)).add(bigInt(Math.floor(Math.random() * 1000)));
}

/* ─────────────────────────────────────────────────────────────────────────────
   HELPERS DE PEER CACHE
   ───────────────────────────────────────────────────────────────────────────── */

// FIX #2: limpa peers de um account ao desconectar/reconectar
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

  // Fast path: client conectado e sessão não mudou
  if (existing?.connected && !sessionChanged) return existing;

  // FIX BUG #5: se já há uma conexão em andamento para este account,
  // reutiliza a Promise existente em vez de criar uma segunda conexão.
  // Sem isso, chamadas concorrentes criariam dois TelegramClient com a
  // mesma session_string → AUTH_KEY_DUPLICATED.
  const inflight = connectingPromises.get(account.id);
  if (inflight) return inflight;

  const connectPromise = (async () => {
    // Desconecta client anterior se existir
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

    // FIX BUG #1: suprimir o update loop sem quebrar o fluxo de reconexão.
    // _loopStarted=true antes de connect() faz os guards de `if (!this._loopStarted)`
    // no GramJS falharem → updateLoop nunca sobe.
    // autoReconnect/_handleReconnect não dependem desse flag, continuam funcionando.
    (client as any)._loopStarted = true;

    await client.connect();

    clients.set(account.id, client);
    sessions.set(account.id, account.session_string);

    // Keepalive a cada 45s para manter a conexão viva
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
  })();

  connectingPromises.set(account.id, connectPromise);
  try {
    return await connectPromise;
  } finally {
    // Remove sempre ao terminar (sucesso ou falha) para permitir reconexões futuras
    connectingPromises.delete(account.id);
  }
}

async function reloadClient(account: Account): Promise<TelegramClient> {
  const existing = clients.get(account.id);
  if (existing) {
    try { await existing.disconnect(); } catch {}
    clients.delete(account.id);
    evictPeersForAccount(account.id);
    const t = keepaliveTimers.get(account.id);
    if (t) { clearInterval(t); keepaliveTimers.delete(account.id); }
  }
  // Garante que não há promise em andamento para este account
  connectingPromises.delete(account.id);
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

  // Estratégia 1: getInputEntity — cache interno do GramJS.
  // Funciona quando os dialogs já foram sincronizados (prewarm com OPT #3).
  try {
    const peer = await client.getInputEntity(chatIdNum);
    peerCache.set(key, peer);
    return peer;
  } catch {}

  // Estratégia 2: GetChannels via MTProto direto com accessHash=0.
  // O Telegram retorna o accessHash real para canais/grupos que a conta é membro.
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
  // Caminho lento — só chega aqui se o prewarm falhou ou a conta foi adicionada
  // ao grupo após o boot.
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
            // OPT #1: invoke direto bypassa a resolução interna de entidade
            // do client.sendMessage() de alto nível (~5-20ms por envio).
            // OPT #4: noWebpage=true evita RTT extra de preview.
            //         randomId único por tentativa evita dedup indevido em retries.
            await client.invoke(new Api.messages.SendMessage({
              peer:       peer as any,
              message:    messageText,
              randomId:   makeRandomId(),
              noWebpage:  true,
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
                  randomId:  makeRandomId(),
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
   Posição real = quantas mensagens chegaram no grupo ANTES da mensagem do robô,
   contando desde o momento do disparo (abertura do grupo).
   Inclui mensagens de qualquer pessoa, não só das contas do robô.
   ───────────────────────────────────────────────────────────────────────────── */
async function monitorPositions(
  telegramChatId: string,
  sentMembers: Array<{ account_id: string; message_text: string }>,
  scheduleId: string,
  dispatchedAt: Date,   // momento exato do disparo (abertura detectada)
  groupType: "open" | "closed"
): Promise<void> {
  if (sentMembers.length === 0) return;

  const account = accountCache.get(sentMembers[0].account_id);
  if (!account) { console.warn("[monitor] Conta não encontrada no cache — ignorando"); return; }

  const client = await getClient(account).catch(() => null);
  if (!client) { console.warn("[monitor] Sem client — ignorando monitoramento"); return; }

  // windowStartUnix: janela começa no momento exato do disparo (sem margem negativa)
  // Mensagens anteriores ao disparo não entram na contagem de posição
  const windowStartUnix = Math.floor(dispatchedAt.getTime() / 1000);

  const ourTexts = new Set(sentMembers.map(m => m.message_text).filter(Boolean));
  const deadline = Date.now() + (groupType === "closed"
    ? MONITOR_DELAY_CLOSED_MS + 10_000
    : MONITOR_MAX_OPEN_MS);

  // Grupo fechado: aguarda um tempo para as mensagens chegarem antes de ler
  if (groupType === "closed") await new Promise(r => setTimeout(r, MONITOR_DELAY_CLOSED_MS));

  console.log(`[monitor] Iniciando para schedule ${scheduleId} (${groupType}) — janela desde ${dispatchedAt.toISOString()}`);

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

      // Todas as mensagens desde o disparo, em ordem cronológica (mais antiga primeiro)
      // Inclui mensagens de qualquer remetente — é isso que forma a fila real do grupo
      const windowMsgs: any[] = (result.messages ?? [])
        .filter((m: any) =>
          (m._ === "message" || m.className === "Message") &&
          m.date >= windowStartUnix
        )
        .reverse(); // reverse: GetHistory vem do mais novo; queremos cronológico

      if (windowMsgs.length === 0) {
        if (groupType === "closed") {
          console.warn("[monitor] Sem mensagens na janela (grupo fechado) — aguardando mais");
          await new Promise(r => setTimeout(r, MONITOR_POLL_MS));
          continue;
        }
        await new Promise(r => setTimeout(r, MONITOR_POLL_MS));
        continue;
      }

      // Para grupo aberto: aguarda até nossas mensagens aparecerem
      if (groupType === "open" && !windowMsgs.some((m: any) => ourTexts.has(m.message))) {
        await new Promise(r => setTimeout(r, MONITOR_POLL_MS));
        continue;
      }

      // Para grupo fechado: se nossas mensagens ainda não apareceram, aguarda
      if (groupType === "closed" && !windowMsgs.some((m: any) => ourTexts.has(m.message))) {
        await new Promise(r => setTimeout(r, MONITOR_POLL_MS));
        continue;
      }

      // Calcula posição real de cada conta:
      // posição = índice da mensagem na lista cronológica completa do grupo (desde o disparo)
      // Ex: se chegaram 3 mensagens de outros antes da nossa → posição = 4
      const cutoff = new Date(dispatchedAt.getTime() - 60_000).toISOString();
      await Promise.allSettled(sentMembers.map(sm => {
        if (!sm.message_text) return;

        // Encontra o índice da mensagem desta conta na lista cronológica completa
        const idx = windowMsgs.findIndex((m: any) => m.message === sm.message_text);
        if (idx < 0) {
          console.warn(`[monitor] Mensagem não encontrada na janela para account ${sm.account_id}`);
          return;
        }

        // posição real = quantas mensagens chegaram antes + 1 (1-based)
        const realPosition = idx + 1;
        const totalInWindow = windowMsgs.length;

        console.log(
          `[monitor] ${sm.account_id}: posição #${realPosition} de ${totalInWindow} ` +
          `mensagens na janela — ${telegramChatId}`
        );

        return supabase.from("dispatch_logs")
          .update({ position_rank: realPosition })
          .eq("schedule_id", scheduleId)
          .eq("account_id", sm.account_id)
          .eq("status", "sent")
          .gte("sent_at", cutoff);
      }));

      console.log(`[monitor] ✓ Posições salvas para schedule ${scheduleId} (${windowMsgs.length} msgs na janela)`);
      return;

    } catch (err: any) {
      console.warn(`[monitor] Erro ao buscar histórico: ${err.message}`);
      await new Promise(r => setTimeout(r, MONITOR_POLL_MS));
      if (groupType === "closed" && Date.now() >= deadline) return;
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

    // Log em background — não bloqueia o caminho crítico de envio
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

    // Update em background — mensagens já enviadas, isso não é crítico
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
   LISTENER DE GRUPO FECHADO
   Estratégia: polling leve de GetFullChannel a cada CLOSED_GROUP_POLL_MS (10ms)
   para detectar o momento exato em que o admin remove a restrição de envio.
   Quando defaultBannedRights.sendMessages passar a false → dispara imediatamente.
   Nunca tenta enviar antes da detecção — zero tentativas desperdiçadas.
   ───────────────────────────────────────────────────────────────────────────── */
function startClosedGroupListener(schedule: Schedule, group: Group, account: Account): void {
  const existing = listenMap.get(group.id);
  if (existing) existing.abort();

  const ctrl = new AbortController();
  listenMap.set(group.id, ctrl);

  console.log(
    `[closed-listen] 🔒 Aguardando abertura de ${group.telegram_chat_id} ` +
    `(schedule ${schedule.id}) — polling ${CLOSED_GROUP_POLL_MS}ms`
  );

  (async () => {
    try {
      const client = await getClient(account).catch(() => null);
      if (!client) {
        console.warn(`[closed-listen] Sem client — abortando para ${schedule.id}`);
        listenMap.delete(group.id);
        return;
      }

      // Pre-resolve peer antes de entrar no loop crítico
      try { await resolvePeer(client, group.telegram_chat_id!, account.id); } catch {}

      const deadline  = Date.now() + CLOSED_GROUP_WATCH_WINDOW_MS;
      let pollMs      = CLOSED_GROUP_POLL_MS;
      let lastRestricted: boolean | null = null;

      while (Date.now() < deadline && !ctrl.signal.aborted) {
        const loopStart = Date.now();

        try {
          const rawId     = String(group.telegram_chat_id!).replace(/^-/, "");
          const channelId = rawId.startsWith("100") ? rawId.slice(3) : rawId;

          // GetFullChannel: query leve de metadata, não conta como envio
          // Retorna defaultBannedRights com os bits de restrição atuais do grupo
          const result = await client.invoke(
            new Api.channels.GetFullChannel({
              channel: new Api.InputChannel({
                channelId: bigInt(channelId),
                accessHash: bigInt(0),
              }),
            })
          ) as any;

          const banned   = result?.fullChat?.chat?.defaultBannedRights;
          // sendMessages=true significa PROIBIDO; false ou ausente significa LIBERADO
          const restricted = banned?.sendMessages === true;

          if (lastRestricted === null) {
            // Primeira leitura — só loga estado inicial
            console.log(
              `[closed-listen] Estado inicial: ${restricted ? "🔒 fechado" : "🔓 já aberto"} ` +
              `— ${group.telegram_chat_id}`
            );
          }

          if (!restricted && !ctrl.signal.aborted) {
            // Grupo abriu — dispara imediatamente
            const detectedAt   = new Date();
            const detectedMs   = detectedAt.getTime();
            const scheduleMs   = new Date(schedule.next_run_at).getTime();
            const openedAfterMs = detectedMs - scheduleMs; // ms após o horário programado

            console.log(
              `[closed-listen] 🔓 ABERTURA DETECTADA\n` +
              `  schedule_id  : ${schedule.id}\n` +
              `  horário prog : ${schedule.next_run_at}\n` +
              `  detectado em : ${detectedAt.toISOString()}\n` +
              `  Δ abertura   : +${openedAfterMs}ms após horário programado`
            );

            listenMap.delete(group.id);

            const alreadySent  = schedule.retry_until
              ? await getAlreadySentIds(schedule)
              : new Set<string>();

            const dispatchStart = Date.now();
            const results       = await dispatchToGroup(schedule, group, alreadySent);
            const dispatchEndMs = Date.now();

            // O que importa: detectedAt → fim do envio
            // Esse é o tempo real que separa você do primeiro lugar
            const totalMs   = dispatchEndMs - detectedMs;
            const waitMs    = dispatchStart - detectedMs;  // detecção → início do dispatch
            const sendMs    = dispatchEndMs - dispatchStart; // dispatch em si

            const sentCount = results.filter(r => r.status === "sent").length;
            const failCount = results.filter(r => r.status === "failed").length;

            console.log(
              `[closed-listen] ✅ DISPARO CONCLUÍDO\n` +
              `  schedule_id      : ${schedule.id}\n` +
              `  detectado em     : ${detectedAt.toISOString()}\n` +
              `  detecção→dispatch: ${waitMs}ms\n` +
              `  dispatch (envios): ${sendMs}ms\n` +
              `  ──────────────────────────────\n` +
              `  TOTAL abertura→fim: ${totalMs}ms  ← esse é o número\n` +
              `  enviadas: ${sentCount} | falhas: ${failCount}`
            );

            const sentForMonitor = results
              .filter(r => r.status === "sent")
              .map(r => ({ account_id: r.account_id, message_text: r.message_text ?? "" }))
              .filter(r => r.message_text);
            if (sentForMonitor.length > 0) {
              monitorPositions(
                group.telegram_chat_id!,
                sentForMonitor,
                schedule.id,
                detectedAt,  // janela começa no momento da abertura
                "closed"
              ).catch(err => console.error("[closed-listen] Erro no monitor:", err.message));
            }

            await updateScheduleAfterDispatch(schedule, results, detectedAt);
            return;
          }

          lastRestricted = restricted;
          // Restaura poll normal após eventual fallback
          if (pollMs !== CLOSED_GROUP_POLL_MS) pollMs = CLOSED_GROUP_POLL_MS;

        } catch (err: any) {
          if (ctrl.signal.aborted) return;

          const isFlood =
            err?.seconds != null ||
            err?.constructor?.name === "FloodWaitError" ||
            /flood/i.test(String(err?.message ?? ""));

          if (isFlood) {
            // Flood no polling de verificação — cai para fallback e continua
            pollMs = CLOSED_GROUP_POLL_FLOOD_FALLBACK_MS;
            const waitSecs: number = typeof err.seconds === "number"
              ? err.seconds
              : parseInt(String(err?.message ?? "").match(/(\d+)/)?.[1] ?? "1", 10);
            console.warn(
              `[closed-listen] FloodWait ${waitSecs}s no polling — ` +
              `fallback para ${CLOSED_GROUP_POLL_FLOOD_FALLBACK_MS}ms`
            );
            await new Promise(r => setTimeout(r, Math.min(waitSecs * 1000, deadline - Date.now())));
            continue;
          }

          // Erro transitório (network, timeout) — tenta reconectar e continua
          console.warn(`[closed-listen] Erro no poll (${schedule.id}): ${err.message}`);
          try {
            if (!client.connected) {
              await getClient(account);
              await resolvePeer(client, group.telegram_chat_id!, account.id);
            }
          } catch {}
        }

        // Espera o restante do intervalo descontando o tempo gasto na chamada
        const elapsed = Date.now() - loopStart;
        const wait    = Math.max(0, pollMs - elapsed);
        if (wait > 0 && !ctrl.signal.aborted) {
          await new Promise(r => setTimeout(r, wait));
        }
      }

      listenMap.delete(group.id);

      if (ctrl.signal.aborted) {
        console.log(`[closed-listen] ⏹ Listener abortado para schedule ${schedule.id}`);
        return;
      }

      // Janela de 30s esgotada sem abertura — avança para próxima semana
      console.warn(
        `[closed-listen] ⏰ Janela de ${CLOSED_GROUP_WATCH_WINDOW_MS / 1000}s esgotada ` +
        `sem abertura detectada — schedule ${schedule.id}`
      );
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
        last_attempt_error:  `Grupo fechado: janela de ${CLOSED_GROUP_WATCH_WINDOW_MS / 1000}s sem abertura`,
      }).eq("id", schedule.id);
      scheduleTimer(schedule.id, nextRun);

    } catch (err: any) {
      console.error(`[closed-listen] Erro inesperado (${schedule.id}):`, err.message);
      listenMap.delete(group.id);
    }
  })();
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

    // OPT #2: tenta consumir o pre-fetch cache primeiro.
    // Se o prefetch chegou a tempo (800ms antes), a query Supabase não está
    // no caminho crítico — o schedule já está em memória.
    let schedule = schedulePrefetchCache.get(scheduleId);
    if (schedule) {
      schedulePrefetchCache.delete(scheduleId);
      console.log(`[fire] ⚡ Schedule ${scheduleId} servido do pre-fetch cache`);
    } else {
      // Fallback: busca ao vivo (primeiro boot, retry imediato, etc.)
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

    // Injeta contas frescas do accountCache (podem ter sido atualizadas desde o pre-fetch)
    if (group.group_members) {
      group.group_members = group.group_members.map(m => ({
        ...m,
        accounts: m.accounts ? (accountCache.get(m.accounts.id) ?? m.accounts) : null,
      }));
    }

    console.log(`[fire] ⚡ Disparando schedule ${scheduleId} às ${now.toISOString()}`);

    // Listener já ativo para este grupo (open ou closed) — não duplica
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

    if (group.group_type === "open") {
      startGroupListener(schedule, group, firstAccount as Account);

      await supabase.from("schedules").update({
        retry_until:         new Date(now.getTime() + OPEN_GROUP_LISTEN_TIMEOUT_MS).toISOString(),
        last_attempt_at:     now.toISOString(),
        last_attempt_status: "waiting_admin",
        last_attempt_error:  null,
      }).eq("id", scheduleId);
      return;
    }

    if (group.group_type === "closed") {
      // Listener roda em background — libera firingNow para não bloquear reloads
      firingNow.delete(scheduleId);

      startClosedGroupListener(schedule, group, firstAccount as Account);

      await supabase.from("schedules").update({
        retry_until:         new Date(now.getTime() + CLOSED_GROUP_WATCH_WINDOW_MS).toISOString(),
        last_attempt_at:     now.toISOString(),
        last_attempt_status: "waiting_open",
        last_attempt_error:  null,
      }).eq("id", scheduleId);
      return;
    }

    // Em ciclos frescos (sem retry_until) não há nada enviado ainda —
    // skip da query de dedup elimina ~50-100ms do caminho crítico.
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
   TIMER DE PRECISÃO + PRE-FETCH
   ───────────────────────────────────────────────────────────────────────────── */
function scheduleTimer(scheduleId: string, nextRunAt: string): void {
  const delay = new Date(nextRunAt).getTime() - Date.now();

  if (delay < -5_000) {
    console.warn(`[timer] Schedule ${scheduleId} ignorado — muito no passado (${nextRunAt})`);
    return;
  }

  // Cancela timers anteriores (fire + prefetch)
  const prev = scheduledTimers.get(scheduleId);
  if (prev) clearTimeout(prev);
  const prevPrefetch = prefetchTimers.get(scheduleId);
  if (prevPrefetch) { clearTimeout(prevPrefetch); prefetchTimers.delete(scheduleId); }

  const effectiveDelay = Math.max(0, delay);

  // OPT #2: pre-fetch do schedule PREFETCH_BEFORE_MS antes do fire.
  // Carrega o schedule do banco enquanto ainda há tempo, e guarda em
  // schedulePrefetchCache. Quando fireSchedule rodar, consome instantaneamente.
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
        // Injeta contas frescas do accountCache no pre-fetch
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

        // FIX #4: getDialogs awaited antes de qualquer disparo
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

    // OPT #3: pre-resolve de peers para todos os grupos ativos.
    // Após getDialogs(), popula peerCache com os telegram_chat_id conhecidos.
    // Garante que resolvePeer() vai pelo caminho rápido (Estratégia 1: cache local)
    // no primeiro disparo, sem cair na Estratégia 3 (getDialogs 200 itens).
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
              .catch(() => {}) // silencia — só otimização, não crítico
          );
        }
      }

      await Promise.allSettled(resolvePromises);
      console.log(`[prewarm] ✓ Pre-resolve de peers concluído (${resolvePromises.length} entradas)`);
    } catch (err: any) {
      console.warn(`[prewarm] Falha no pre-resolve de peers: ${err.message}`);
    }

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

  for (const t of prefetchTimers.values()) clearTimeout(t);
  prefetchTimers.clear();

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
      pre-resolve peers de todos os grupos ativos (OPT #3),
      popula accountCache e peerCache
   2. reloadSchedules: lê o banco e cria os timers iniciais
      (cada timer agenda também um pre-fetch — OPT #2)
   3. setInterval: mantém o ciclo de reload a cada 30s
   ───────────────────────────────────────────────────────────────────────────── */
async function init(): Promise<void> {
  console.log("[worker] Iniciando...");
  await prewarmAccounts();
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

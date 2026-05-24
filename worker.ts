// worker-flat.ts — dispatch worker Telegram, sem camadas de abstração
// Fix v1: firingNow Set previne duplo disparo quando reloadSchedules
//         roda enquanto fireSchedule ainda está executando (race condition
//         entre RETRY_BUDGET_MS=50s e RELOAD_INTERVAL_MS=30s)

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
   Antes estava escondido dentro de TelegramClientPool (classe).
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
// Preenchido no prewarm e atualizado via /reload. Evita joins repetidos.
const accountCache = new Map<string, Account>();

// Timers de disparo agendado: scheduleId → setTimeout handle
// Usado para detectar se um schedule já tem timer ativo e evitar duplicatas
const scheduledTimers = new Map<string, ReturnType<typeof setTimeout>>();

// Listeners de grupos abertos: groupId → AbortController
// Permite cancelar um listener em andamento (ex: quando o retry expira)
const listenMap = new Map<string, AbortController>();

// FIX: schedules atualmente em execução.
// Resolve o bug de duplo disparo: quando fireSchedule demora mais de
// RELOAD_INTERVAL_MS (30s), o reloadSchedules tentava disparar de novo
// porque scheduledTimers não tinha o ID (foi deletado ao iniciar) e
// last_attempt_at no banco ainda era antigo (update só ocorre no final).
const firingNow = new Set<string>();

/* ─────────────────────────────────────────────────────────────────────────────
   QUERY REUTILIZADA
   Seleciona o schedule com todos os dados relacionados em um único join.
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
   HELPERS PUROS (sem side effects)
   ───────────────────────────────────────────────────────────────────────────── */

// Erros de autenticação morta não valem retry — a sessão precisa ser
// renovada manualmente. Qualquer outro erro (flood, timeout, rede) é retryável.
function isRetryableError(msg: string): boolean {
  const u = msg.toUpperCase();
  return !u.includes("AUTH_KEY_UNREGISTERED") &&
         !u.includes("USER_DEACTIVATED") &&
         !u.includes("SESSION_REVOKED");
}

// Calcula o próximo horário semanal a partir de um cron simplificado.
// Só suporta o subconjunto "minuto hora * * dia_da_semana" (5 campos).
// Exemplos: "30 14 * * 2" = toda terça às 14:30 UTC
function nextWeeklyOccurrence(cron: string): string {
  const parts = cron.trim().split(/\s+/);
  const mi    = parseInt(parts[0], 10);
  const h     = parseInt(parts[1], 10);
  const dow   = parseInt(parts[4], 10); // 0=domingo, 6=sábado

  if (
    parts.length < 5 ||
    isNaN(mi) || isNaN(h) || isNaN(dow) ||
    mi < 0 || mi > 59 || h < 0 || h > 23 || dow < 0 || dow > 6
  ) {
    throw new Error(`cron_expression inválida: "${cron}"`);
  }

  const now = new Date();
  let daysUntil = (dow - now.getUTCDay() + 7) % 7;

  // Se é o mesmo dia da semana, verifica se o horário já passou.
  // Se passou, agenda para daqui 7 dias.
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

// Backoff exponencial com teto: base * 2^count, limitado a max.
// count=0 → base, count=1 → 2*base, count=2 → 4*base, ...
function calcRetryInterval(count: number, base: number, max: number): number {
  return Math.min(base * Math.pow(2, count), max);
}

// Verifica se já é hora de tentar de novo, comparando last_attempt_at
// com o intervalo calculado pelo backoff.
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
   GERENCIAMENTO DE CONEXÕES TELEGRAM
   Antes era a classe TelegramClientPool com métodos get/reload/prewarm/_evict.
   Agora são funções soltas operando nos Maps globais acima.
   ───────────────────────────────────────────────────────────────────────────── */

// Retorna um client Telegram conectado para a conta.
// Reutiliza o existente se já conectado e com a mesma sessão.
// Reconecta automaticamente se a sessão mudou ou o client caiu.
async function getClient(account: Account): Promise<TelegramClient> {
  const existing      = clients.get(account.id);
  const sessionInUse  = sessions.get(account.id);
  const sessionChanged = sessionInUse !== account.session_string;

  if (existing?.connected && !sessionChanged) return existing;

  // Se existe mas está morto ou a sessão mudou, derruba e reconecta
  if (existing) {
    try { await existing.disconnect(); } catch {}
    clients.delete(account.id);
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

  // Desativa o update loop padrão do GramJS.
  // Não precisamos receber updates em tempo real (usamos polling manual),
  // e o loop consome recursos e pode causar reconexões desnecessárias.
  (client as any)._updateLoop = () => Promise.resolve();

  await client.connect();

  // Warm-up em background: sincroniza os dialogs do Telegram.
  // Isso popula o cache interno do GramJS com os peers dos grupos,
  // acelerando resolvePeer nas próximas chamadas.
  client.getDialogs({ limit: 100 }).then(() => {
    console.log(`[client] ✓ Dialogs warm-up: ${account.phone_number}`);
  }).catch((err: any) => {
    console.warn(`[client] Dialogs warm-up falhou para ${account.phone_number}: ${err.message}`);
  });

  clients.set(account.id, client);
  sessions.set(account.id, account.session_string);

  // Keepalive: pinga a conta a cada 45s para manter a conexão viva.
  // O Telegram fecha conexões ociosas após alguns minutos.
  // Se o ping falhar, remove do pool para forçar reconexão no próximo uso.
  const interval = setInterval(async () => {
    if (!client.connected) {
      console.warn(`[keepalive] ${account.phone_number} desconectou — removendo do pool`);
      clients.delete(account.id);
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
      keepaliveTimers.delete(account.id);
      clearInterval(interval);

      // Sessões mortas (ban, logout remoto) nunca vão funcionar novamente.
      // Desativa no banco para não desperdiçar recursos tentando reconectar.
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

// Força reconexão descartando o client atual.
// Chamado pela rota HTTP /reload quando a sessão foi atualizada no banco.
async function reloadClient(account: Account): Promise<TelegramClient> {
  const existing = clients.get(account.id);
  if (existing) {
    try { await existing.disconnect(); } catch {}
    clients.delete(account.id);
    const t = keepaliveTimers.get(account.id);
    if (t) { clearInterval(t); keepaliveTimers.delete(account.id); }
  }
  return getClient(account);
}

/* ─────────────────────────────────────────────────────────────────────────────
   RESOLUÇÃO DE PEER TELEGRAM
   Um "peer" é o identificador que o MTProto usa internamente para um chat.
   O ID numérico que temos no banco não é diretamente aceito — precisa ser
   "resolvido" para um InputPeer com accessHash.
   Tentamos 3 estratégias em ordem de custo crescente.
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
  // Funciona se o cliente já sincronizou os dialogs (warm-up acima).
  try {
    const peer = await client.getInputEntity(chatIdNum);
    peerCache.set(key, peer);
    return peer;
  } catch {}

  // Estratégia 2: GetChannels via MTProto direto.
  // IDs de supergrupos/canais seguem o padrão -100XXXXXXXXXX.
  // Subtraímos 1_000_000_000_000 para obter o channelId real.
  // O accessHash=0 funciona para canais públicos.
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
  // Mais lento (pode levar alguns segundos), mas resolve casos onde
  // a conta é membro mas o grupo não estava nos dialogs em cache.
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
   Tenta enviar a mensagem repetidamente dentro de um budget de tempo (50s).
   Cada tentativa tem um timeout próprio (15s).
   Trata FloodWait (Telegram rate limit) esperando o tempo indicado.
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

            // Peer inválido: limpa do cache para forçar nova resolução na próxima tentativa
            if (
              errMsg.includes("PEER_ID_INVALID") ||
              errMsg.includes("CHANNEL_INVALID") ||
              errMsg.includes("CHANNEL_PRIVATE")
            ) {
              peerCache.delete(`${account.id}:${telegramChatId}`);
            }

            // FloodWait: o Telegram pediu para esperar N segundos antes de enviar de novo.
            // Se o tempo de espera cabe no budget restante, esperamos e tentamos de novo.
            // Caso contrário, jogamos o erro para cima.
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
                return; // sucesso após FloodWait
              }
              throw new Error(`FLOOD_WAIT_${waitSecs}_EXCEEDS_BUDGET`);
            }

            throw err;
          }
        })(),
        // Race com timeout por tentativa: se demorar mais de 15s, cancela e tenta de novo
        new Promise<never>((_, r) =>
          setTimeout(
            () => r(new Error(`TIMEOUT tentativa ${attempt}`)),
            Math.min(SEND_TIMEOUT_MS, timeLeft - 100)
          )
        ),
      ]);

      // Chegou aqui = enviou com sucesso
      if (attempt > 1) console.log(`[send] ✓ ${account.phone_number} — enviou na tentativa ${attempt}`);
      return;

    } catch (err: any) {
      const remaining = budgetEnd - Date.now();
      if (remaining > 500) {
        console.warn(`[send] tentativa ${attempt} falhou (${Math.round(remaining / 1000)}s restantes): ${err.message}`);
      }
    }
  }

  throw new Error(`BUDGET_EXCEEDED após ${attempt} tentativa(s) em ${RETRY_BUDGET_MS / 1000}s`);
}

/* ─────────────────────────────────────────────────────────────────────────────
   DEDUPLICAÇÃO
   Antes de enviar, verifica quais accounts já enviaram neste ciclo.
   Um "ciclo" começa em next_run_at e termina em retry_until (ou agora,
   se não houve falha). Evita reenviar em caso de retry parcial.
   ───────────────────────────────────────────────────────────────────────────── */
async function getAlreadySentIds(schedule: Schedule): Promise<Set<string>> {
  // O início do ciclo é: retry_until - retry_window_seconds.
  // Se não há retry_until, o ciclo começou em next_run_at.
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
    return new Set(); // em caso de falha, não bloqueia o envio
  }
  return new Set((data ?? []).map(r => r.account_id as string));
}

/* ─────────────────────────────────────────────────────────────────────────────
   MONITORAMENTO DE POSIÇÃO
   Após o envio, lê o histórico do chat para saber em qual posição cada
   mensagem apareceu (ranking por chegada). Salva position_rank no log.
   Isso é feito em background — não bloqueia o fluxo principal.
   ───────────────────────────────────────────────────────────────────────────── */
async function monitorPositions(
  telegramChatId: string,
  sentMembers: Array<{ account_id: string; message_text: string }>,
  scheduleId: string,
  dispatchedAt: Date,
  groupType: "open" | "closed"
): Promise<void> {
  if (sentMembers.length === 0) return;

  // Usa a primeira conta que enviou para ler o histórico
  const account = accountCache.get(sentMembers[0].account_id);
  if (!account) { console.warn("[monitor] Conta não encontrada no cache — ignorando"); return; }

  const client = await getClient(account).catch(() => null);
  if (!client) { console.warn("[monitor] Sem client — ignorando monitoramento"); return; }

  // Janela: 15s antes do disparo até agora (margem para mensagens pré-existentes)
  const windowStartUnix = Math.floor((dispatchedAt.getTime() - 15_000) / 1000);
  const deadline        = Date.now() + (groupType === "closed"
    ? MONITOR_DELAY_CLOSED_MS + 10_000
    : MONITOR_MAX_OPEN_MS);
  const ourTexts = new Set(sentMembers.map(m => m.message_text).filter(Boolean));

  // Grupos fechados: aguarda alguns segundos para o Telegram processar a ordem
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

      // Filtra mensagens dentro da janela de tempo, em ordem cronológica
      const windowMsgs = (result.messages ?? [])
        .filter((m: any) => m._ === "message" && m.date >= windowStartUnix)
        .reverse(); // GetHistory retorna do mais novo para o mais antigo

      if (windowMsgs.length === 0) {
        if (groupType === "closed") {
          console.warn("[monitor] Sem mensagens na janela (grupo fechado) — abortando");
          return;
        }
        await new Promise(r => setTimeout(r, MONITOR_POLL_MS));
        continue;
      }

      // Grupos abertos: espera até nossas mensagens aparecerem no histórico
      if (groupType === "open" && !windowMsgs.some((m: any) => ourTexts.has(m.message))) {
        await new Promise(r => setTimeout(r, MONITOR_POLL_MS));
        continue;
      }

      // Para cada membro que enviou, encontra a posição no histórico e salva
      const cutoff = new Date(dispatchedAt.getTime() - 60_000).toISOString();
      await Promise.allSettled(sentMembers.map(sm => {
        if (!sm.message_text) return;
        const idx = windowMsgs.findIndex((m: any) => m.message === sm.message_text);
        if (idx < 0) return;
        const rank = idx + 1; // posição 1-indexed no chat
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
   Grupos "open" não disparam no horário — ficam aguardando um sinal do admin
   no chat (mensagem "ok" ou qualquer mídia). Quando o sinal chega, envia
   as mensagens de todos os membros e atualiza o schedule.

   O listener roda em background (IIFE async sem await no chamador).
   É cancelável via AbortController (listenMap).
   ───────────────────────────────────────────────────────────────────────────── */
function startGroupListener(schedule: Schedule, group: Group, account: Account): void {
  // Cancela listener anterior do mesmo grupo, se existir
  const existing = listenMap.get(group.id);
  if (existing) existing.abort();

  const ctrl = new AbortController();
  listenMap.set(group.id, ctrl);

  const deadline    = Date.now() + OPEN_GROUP_LISTEN_TIMEOUT_MS;
  const startUnix   = Math.floor((Date.now() - 10_000) / 1000); // 10s de margem
  let lastSeenMsgId = 0; // evita reprocessar mensagens já vistas

  console.log(`[listen] 👂 Aguardando sinal do admin em ${group.telegram_chat_id} para schedule ${schedule.id}`);

  (async () => {
    try {
      let client = await getClient(account).catch(() => null);
      if (!client) {
        console.warn(`[listen] Sem client — abortando listener para ${schedule.id}`);
        listenMap.delete(group.id);
        return;
      }

      // Pré-resolve o peer para não atrasar o primeiro poll
      try { await resolvePeer(client, group.telegram_chat_id!, account.id); } catch {}

      while (Date.now() < deadline && !ctrl.signal.aborted) {
        try {
          // Reconecta se o client caiu durante a espera
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

          // Filtra mensagens novas (posteriores ao início do listener e não vistas antes)
          const recentMsgs = (result.messages ?? []).filter(
            (m: any) =>
              (m.className === "Message" || m._ === "message") &&
              m.date >= startUnix &&
              m.id > lastSeenMsgId
          );
          if (recentMsgs.length > 0) {
            lastSeenMsgId = Math.max(lastSeenMsgId, ...recentMsgs.map((m: any) => m.id as number));
          }

          // Sinal válido: mensagem de texto "ok" (case-insensitive) OU qualquer mídia
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

            // Monitoramento de posição em background
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

      // Timeout de 2h: nenhum sinal chegou. Avança para próxima semana.
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
   Envia a mensagem de cada membro ativo em paralelo.
   Retorna o resultado de cada envio para que fireSchedule decida
   se foi sucesso total ou se precisa agendar retry.

   Antes estava dividido em processMembersOf + trySendMember (dois níveis).
   Agora é uma única função.
   ───────────────────────────────────────────────────────────────────────────── */
async function dispatchToGroup(
  schedule: Schedule,
  group: Group,
  alreadySent: Set<string>
): Promise<DispatchResult[]> {
  // Filtra membros ativos com conta válida, ordenados por posição configurada
  const members = (group.group_members ?? [])
    .filter(m => m.is_active && m.accounts?.is_active && m.accounts?.session_string)
    .sort((a, b) => a.position - b.position);

  return Promise.all(members.map(async (member, i) => {
    const account      = member.accounts!;
    const positionRank = i + 1;

    // Deduplicação: se essa conta já enviou neste ciclo (mesmo que em execução anterior),
    // pula sem enviar de novo
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
      alreadySent.add(account.id); // marca para dedup dentro desta mesma execução
      console.log(`[dispatch] ✓ ${account.phone_number}`);
    } catch (err) {
      error     = err instanceof Error ? err.message : String(err);
      retryable = isRetryableError(error);
      console.error(
        `[dispatch] ✗ ${account.phone_number} [${retryable ? "retryável" : "permanente"}]: ${error}`
      );
    }

    // Insere log em background — não bloqueia o caminho crítico de envio
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
   Decide se o ciclo foi bem-sucedido (avança para próxima semana) ou
   se precisa de retry (agenda nova tentativa com backoff exponencial).

   Extraída de fireSchedule e startGroupListener para não duplicar lógica.
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

  // Considera sucesso se: há membros ativos, sem falhas retryáveis,
  // sem falhas permanentes, e pelo menos um enviou ou foi pulado (já enviou antes)
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

    // Update em background — as mensagens já foram enviadas, isso não é crítico
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
    // Se é a primeira falha do ciclo, define retry_until a partir de agora.
    // Se já está em retry, mantém o retry_until original (janela não se expande).
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

    // Aqui o update é awaited porque determina se/quando haverá próximo retry
    await supabase.from("schedules").update({
      retry_until:         retryUntil,
      retry_count:         newRetryCount,
      last_attempt_at:     nowISO,
      last_attempt_status: "retrying",
      last_attempt_error:  failErrors || null,
    }).eq("id", schedule.id);

    const retryAt = new Date(now.getTime() + interval * 1000);
    // Só agenda retry se ainda cabe dentro da janela
    if (retryAt < new Date(retryUntil)) {
      scheduleTimer(schedule.id, retryAt.toISOString());
    }
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   DISPARO DE SCHEDULE
   Função central: busca os dados, decide o tipo de grupo e executa.
   Protegida pelo firingNow Set contra execuções paralelas do mesmo ID.
   ───────────────────────────────────────────────────────────────────────────── */
async function fireSchedule(scheduleId: string): Promise<void> {
  // FIX: Proteção contra duplo disparo.
  // Sem isso: reloadSchedules rodava a cada 30s e, se fireSchedule ainda estivesse
  // executando (pode demorar até 50s por membro), disparava uma segunda vez porque
  // scheduledTimers não tinha mais o ID (deletado ao iniciar) e last_attempt_at
  // no banco ainda estava desatualizado (é gravado só no final).
  if (firingNow.has(scheduleId)) {
    console.warn(`[fire] Schedule ${scheduleId} já em execução — ignorando disparo duplicado`);
    return;
  }
  firingNow.add(scheduleId);

  try {
    const now = new Date();

    // Busca o schedule com todos os dados relacionados em um único join
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

    // Substitui os dados de conta pelo cache local, que é mais fresco que o join do banco
    // (o /reload atualiza o accountCache sem precisar recarregar todos os schedules)
    if (group.group_members) {
      group.group_members = group.group_members.map(m => ({
        ...m,
        accounts: m.accounts ? (accountCache.get(m.accounts.id) ?? m.accounts) : null,
      }));
    }

    console.log(`[fire] ⚡ Disparando schedule ${scheduleId} às ${now.toISOString()}`);

    // ── Grupos ABERTOS: inicia listener e retorna imediatamente ──────────────
    // O envio só ocorre quando o admin mandar o sinal no chat.
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

      // Marca no banco que estamos aguardando, com janela de 2h
      await supabase.from("schedules").update({
        retry_until:         new Date(now.getTime() + OPEN_GROUP_LISTEN_TIMEOUT_MS).toISOString(),
        last_attempt_at:     now.toISOString(),
        last_attempt_status: "waiting_admin",
        last_attempt_error:  null,
      }).eq("id", scheduleId);
      return;
    }

    // ── Grupos FECHADOS: envia imediatamente ─────────────────────────────────
    // Em ciclos frescos (retry_until é null) não há nada enviado ainda —
    // pula a query de dedup para economizar ~100ms no caminho crítico.
    const alreadySent = schedule.retry_until
      ? await getAlreadySentIds(schedule)
      : new Set<string>();

    if (alreadySent.size > 0) {
      console.log(`[dedup] ${alreadySent.size} account(s) já enviaram neste ciclo — pulando.`);
    }

    const results = await dispatchToGroup(schedule, group, alreadySent);

    // Monitora posições em background (não bloqueia atualização do schedule)
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
    // Sempre remove do Set, mesmo em caso de exceção
    firingNow.delete(scheduleId);
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   TIMER DE PRECISÃO
   Calcula o delay até next_run_at e cria um setTimeout.
   Registra em scheduledTimers para que reloadSchedules saiba que
   já existe um timer ativo e não crie um segundo.
   ───────────────────────────────────────────────────────────────────────────── */
function scheduleTimer(scheduleId: string, nextRunAt: string): void {
  const delay = new Date(nextRunAt).getTime() - Date.now();

  // Schedules muito no passado (>5s) são ignorados.
  // Isso acontece quando next_run_at já passou e o worker estava offline.
  // O reloadSchedules vai detectar e tratar esses casos separadamente.
  if (delay < -5_000) {
    console.warn(`[timer] Schedule ${scheduleId} ignorado — muito no passado (${nextRunAt})`);
    return;
  }

  // Cancela timer anterior se existir (evita timers duplicados ao reagendar)
  const existing = scheduledTimers.get(scheduledTimers.has(scheduleId) ? scheduleId : "");
  const prev = scheduledTimers.get(scheduleId);
  if (prev) clearTimeout(prev);

  const effectiveDelay = Math.max(0, delay);

  const timer = setTimeout(async () => {
    // Remove do Map antes de executar para que reloadSchedules possa
    // detectar que não há timer (só relevante se fireSchedule falhar
    // antes de criar o próximo timer)
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
   Roda a cada 30s. Responsável por três coisas:
   1. Detectar schedules futuros e criar timers para eles
   2. Detectar schedules em retry que já é hora de tentar de novo
   3. Detectar retries que expiraram e avançar para próxima semana
   ───────────────────────────────────────────────────────────────────────────── */
async function reloadSchedules(): Promise<void> {
  const now          = new Date();
  const nowISO       = now.toISOString();
  const lookaheadISO = new Date(now.getTime() + LOOKAHEAD_MS).toISOString();

  // Busca os três tipos de schedules em paralelo para minimizar latência
  const [
    { data: futureSchedules },   // schedules normais que disparam nos próximos 2 min
    { data: retrySchedules },    // schedules em retry (retry_until no futuro)
    { data: expiredRetries },    // schedules em retry cujo prazo expirou
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

  // ── 1. Retries expirados: desiste e avança para próxima semana ───────────
  await Promise.all((expiredRetries ?? []).map(async expired => {
    console.warn(`[reload] Schedule ${expired.id}: retry expirou sem sucesso. Avançando.`);

    // Cancela listener de grupo aberto se estiver ativo
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

  // ── 2. Schedules futuros: cria timers para os que ainda não têm ──────────
  for (const s of futureSchedules ?? []) {
    if (!scheduledTimers.has(s.id)) {
      scheduleTimer(s.id, s.next_run_at);
    }
  }

  // ── 3. Schedules em retry: dispara agora se for a hora ───────────────────
  for (const s of retrySchedules ?? []) {
    const schedule = s as unknown as Schedule;

    // Se há um listener ativo para o grupo (grupo aberto aguardando admin), não interfere
    if (listenMap.has(schedule.group_id)) continue;

    // FIX: verifica firingNow além de scheduledTimers
    if (
      isRetryDue(schedule, now) &&
      !scheduledTimers.has(schedule.id) &&
      !firingNow.has(schedule.id)  // ← proteção contra duplo disparo
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
   Ao iniciar (e periodicamente), conecta todas as contas ativas.
   Objetivos:
   - Ter os clients prontos antes do primeiro disparo (sem latência de conexão)
   - Detectar e desativar sessões mortas antes de tentar enviar
   - Popular o accountCache para que fireSchedule não precise ir ao banco
   ───────────────────────────────────────────────────────────────────────────── */
let prewarmRunning = false;
async function prewarmAccounts(): Promise<void> {
  if (prewarmRunning) return; // evita execuções paralelas
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
        await getClient(account);
      } catch (err: any) {
        const authDead =
          err.message?.includes("AUTH_KEY_UNREGISTERED") ||
          err.message?.includes("USER_DEACTIVATED") ||
          err.message?.includes("SESSION_REVOKED");
        if (authDead) {
          console.warn(`[prewarm] Sessão morta: ${account.phone_number} — desativando.`);
          await supabase.from("accounts").update({ is_active: false }).eq("id", account.id);
        }
      }
    }));
  } finally {
    prewarmRunning = false;
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   HTTP SERVER — 5 ROTAS DE GERENCIAMENTO
   Permite que o backend (Next.js/API) interaja com o worker em runtime
   sem reiniciá-lo. Autenticado via header x-worker-secret.
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
  // Lista todos os grupos e canais que a conta participa.
  // Usado no frontend para o usuário escolher o telegram_chat_id do grupo.
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
  // Retorna o número de participantes de um chat.
  // Tenta três estratégias em ordem de confiabilidade.
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

      // Estratégia 1: GetFullChannel (supergrupos e canais)
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

      // Estratégia 2: dialog.entity (mais lento, mas funciona para qualquer tipo)
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

      // Estratégia 3: GetFullChat (grupos legados)
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
  // Lista os participantes de um chat (sem bots).
  // Usado para associar membros do Telegram a contas no sistema.
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

      // Estratégia 1: GetParticipants (supergrupos/canais via MTProto)
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

      // Estratégia 2: GetFullChat (grupos legados)
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
  // Força reconexão de uma conta específica. Chamado quando a sessão
  // é atualizada no banco (ex: re-login via QR code).
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
    accountCache.set(accountId, account); // atualiza cache local

    if (!account.is_active || !account.session_string) {
      return jsonResponse(res, 200, { ok: true, skipped: true, reason: "conta inativa ou sem sessão" });
    }

    try {
      await reloadClient(account);
      console.log(`[http] /reload ✓ ${account.phone_number} recarregada`);
      return jsonResponse(res, 200, { ok: true });
    } catch (err: any) {
      console.error("[http] /reload erro:", err.message);
      return jsonResponse(res, 500, { error: err.message });
    }
  }

  // ── POST/DELETE /groups/:id/listen ──────────────────────────────────────
  // POST: inicia listener manual para um grupo aberto.
  //       Disparado pelo frontend quando o usuário ativa o modo de escuta.
  // DELETE: cancela o listener ativo para o grupo.
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

      // Listener manual roda em background, mesma lógica do scheduled listener
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

          const deadline    = Date.now() + 2 * 60 * 60_000; // 2h
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

                // Cria um stub de Schedule para reutilizar dispatchToGroup
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
   Garante que não ficam timers, intervals ou conexões penduradas
   quando o processo recebe SIGTERM (ex: deploy, restart do container).
   ───────────────────────────────────────────────────────────────────────────── */
async function shutdown() {
  console.log("[worker] Encerrando...");

  // Cancela todos os timers pendentes
  for (const t of scheduledTimers.values()) clearTimeout(t);
  scheduledTimers.clear();

  // Para todos os keepalives
  for (const t of keepaliveTimers.values()) clearInterval(t);
  keepaliveTimers.clear();

  // Para o servidor HTTP
  httpServer.close();

  // Desconecta todos os clients Telegram
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
   1. prewarmAccounts: conecta todas as contas e popula accountCache
   2. reloadSchedules: lê o banco e cria os timers iniciais
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

import { randomUUID } from "node:crypto";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const NAMESPACE = "pi-intercom-hook";
const REGISTER = "intercom:extension-register";
const READY = "intercom:extension-registry-ready";
const LIMIT = 64;
export const MAX_OUTPUT_BYTES = 256 * 1024;
const CHUNK_BYTES = 6 * 1024;
const MAX_PARTS = Math.ceil(MAX_OUTPUT_BYTES / CHUNK_BYTES);
type Channel = {
    snapshot(): { connected: boolean; supported: boolean };
    listSessions(): Promise<unknown>;
    publish(payload: unknown, options: { audience: "capable" }): void;
};
type Target = { id: string; endpointEpoch: string; name?: string };
type Watch = {
    watchId: string;
    sessionId: string;
    endpointEpoch: string;
    status: "subscribing" | "watching" | "receiving" | "failed";
    error?: string;
};
type Pending = { resolve(): void; reject(error: Error): void };
type Subscription = {
    to: string;
    watchId: string;
    endpointEpoch: string;
    status: "validating" | "watching" | "failed";
    error?: string;
};
type Assembly = { parts: string[]; total: number; aborted: boolean; stopReason: string; truncated: boolean };
function record(value: unknown): value is Record<string, any> {
    return value !== null && typeof value === "object" && !Array.isArray(value);
}
function errorText(error: unknown): string {
    return error instanceof Error ? error.message : String(error);
}

/** Bound extension-owned memory before encoding or joining arbitrary assistant text. */
export function captureText(parts: readonly { type: string; text?: string }[], stopReason: string) {
    let text = "";
    let used = 0;
    let blocks = 0;
    let truncated = false;
    for (const part of parts) {
        if (part.type !== "text" || typeof part.text !== "string") continue;
        for (const piece of blocks++ ? ["\n", part.text] : [part.text]) {
            const remaining = MAX_OUTPUT_BYTES - used;
            // At most cap+1 UTF-16 units are ever encoded, even for an enormous source block.
            const prefix = Buffer.from(piece.slice(0, remaining + 1), "utf8");
            // stream:true drops an incomplete trailing UTF-8 sequence, not a real U+FFFD.
            const accepted = new TextDecoder("utf-8").decode(prefix.subarray(0, remaining), { stream: true });
            text += accepted;
            used += Buffer.byteLength(accepted);
            if (accepted.length < piece.length) { truncated = true; break; }
        }
        if (truncated) break;
    }
    return { text, stopReason, truncated };
}
export function resolveTarget(value: unknown, to: string, self: string): Target {
    if (!Array.isArray(value)) throw new Error("Intercom returned an invalid roster");
    const sessions = value.filter(record);
    const exact = sessions.filter((s) => s.id === to);
    const named = sessions.filter((s) => typeof s.name === "string" && s.name.toLowerCase() === to.toLowerCase());
    const matches = exact.length ? exact : named.length ? named : sessions.filter((s) => typeof s.id === "string" && s.id.startsWith(to));
    if (matches.length !== 1) throw new Error(matches.length ? "Ambiguous session; use full ID" : "Session not found");
    const s = matches[0]!;
    if (s.id === self) throw new Error("Cannot subscribe to this session");
    if (typeof s.id !== "string" || typeof s.endpointEpoch !== "string" || !s.endpointEpoch || s.trustedLocal !== true) {
        throw new Error("Expected a trusted local intercom endpoint with endpointEpoch");
    }
    return { id: s.id, endpointEpoch: s.endpointEpoch, ...(typeof s.name === "string" ? { name: s.name } : {}) };
}

/** Public intercom extension bus only. The worker owns the one-shot subscription. */
export function createHook(
    host: Pick<ExtensionAPI, "events" | "on" | "sendMessage">,
    options: { selfId?: () => string | undefined; ackTimeoutMs?: number } = {},
) {
    const selfId = options.selfId ?? (() => process.env.PI_INTERCOM_SESSION_ID);
    let channel: Channel | undefined;
    let unsubscribe: (() => void) | undefined;
    let active = false;
    let generation = 0;
    let final = { text: "", stopReason: "unknown", truncated: false };
    let sawAssistant = false;
    const watches = new Map<string, Watch>();
    const pending = new Map<string, Pending>();
    const subscriptions = new Map<string, Subscription>();
    const assemblies = new Map<string, Assembly>();

    function available(): Channel {
        if (!active || !channel?.snapshot().connected || !channel.snapshot().supported) {
            throw new Error("Load pi-intercom with extension-bus support and wait for its connection");
        }
        if (!selfId()) throw new Error("Intercom session identity is unavailable");
        return channel;
    }
    function publish(payload: Record<string, unknown>) {
        available().publish({ version: 1, ...payload }, { audience: "capable" });
    }
    function cancelRemote(watch: Watch) {
        try { publish({ kind: "cancel", to: watch.sessionId, watchId: watch.watchId }); } catch { /* best effort */ }
    }
    function fail(watch: Watch, error: string) {
        watch.status = "failed";
        watch.error = error;
        assemblies.delete(watch.watchId);
        pending.get(watch.watchId)?.reject(new Error(error));
    }
    function reset(reason: string) {
        generation++;
        for (const p of pending.values()) p.reject(new Error(reason));
        pending.clear();
        watches.clear();
        subscriptions.clear();
        assemblies.clear();
        final = { text: "", stopReason: "unknown", truncated: false };
        sawAssistant = false;
    }
    async function subscribe(from: string, payload: Record<string, any>) {
        const key = `${from}/${payload.watchId}`;
        if (subscriptions.has(key)) return;
        if (subscriptions.size >= LIMIT) {
            publish({ kind: "rejected", to: from, watchId: payload.watchId, error: "Worker subscription limit reached (64)" });
            return;
        }
        const entry: Subscription = { to: from, watchId: payload.watchId, endpointEpoch: payload.endpointEpoch, status: "validating" };
        subscriptions.set(key, entry);
        const epoch = generation;
        const ownId = selfId();
        try {
            const roster = await available().listSessions();
            if (!active || epoch !== generation || subscriptions.get(key) !== entry) return;
            const own = Array.isArray(roster) ? roster.find((s) => record(s) && s.id === ownId) : undefined;
            if (!own || own.endpointEpoch !== entry.endpointEpoch) throw new Error("Worker endpoint changed; retry subscription");
            entry.status = "watching";
            publish({ kind: "ack", to: from, watchId: entry.watchId, endpointEpoch: entry.endpointEpoch });
        } catch (error) {
            if (subscriptions.get(key) !== entry) return;
            subscriptions.delete(key);
            try { publish({ kind: "rejected", to: from, watchId: entry.watchId, error: errorText(error) }); } catch { /* caller times out */ }
        }
    }
    function onEvent(event: unknown) {
        if (!active || !record(event)) return;
        if (event.type === "connection" && (!event.connected || !event.supported)) {
            for (const w of watches.values()) fail(w, "Intercom connection lost; settlement delivery is unknown. Cancel and re-arm.");
            subscriptions.clear();
            return;
        }
        const p = event.payload;
        if (event.type !== "message" || typeof event.fromSessionId !== "string" || !record(p) || p.version !== 1 || p.to !== selfId() || typeof p.watchId !== "string" || p.watchId.length > 100) return;
        const from = event.fromSessionId;
        if (from === selfId()) return;
        if (p.kind === "subscribe" && typeof p.endpointEpoch === "string") {
            void subscribe(from, p);
            return;
        }
        if (p.kind === "cancel") {
            subscriptions.delete(`${from}/${p.watchId}`);
            return;
        }
        const watch = watches.get(p.watchId);
        if (!watch || watch.sessionId !== from || watch.status === "failed") return;
        if (p.kind === "rejected") {
            fail(watch, typeof p.error === "string" ? p.error : "Worker rejected subscription");
        } else if (p.kind === "ack" && p.endpointEpoch === watch.endpointEpoch && watch.status === "subscribing") {
            watch.status = "watching";
            pending.get(watch.watchId)?.resolve();
        } else if (p.kind === "settled" && p.endpointEpoch === watch.endpointEpoch && watch.status !== "subscribing") {
            if (!Number.isInteger(p.total) || p.total < 1 || p.total > MAX_PARTS || !Number.isInteger(p.index) || p.index < 0 || p.index >= p.total || typeof p.data !== "string" || p.data.length > CHUNK_BYTES * 4 / 3 || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(p.data) || typeof p.aborted !== "boolean" || typeof p.truncated !== "boolean" || typeof p.stopReason !== "string" || p.stopReason.length > 100) {
                fail(watch, "Invalid settled-output payload");
                return;
            }
            let assembly = assemblies.get(watch.watchId);
            if (!assembly) {
                assembly = { parts: [], total: p.total, aborted: p.aborted, stopReason: p.stopReason, truncated: p.truncated };
                assemblies.set(watch.watchId, assembly);
            }
            if (assembly.total !== p.total || assembly.aborted !== p.aborted || assembly.stopReason !== p.stopReason || assembly.truncated !== p.truncated || (assembly.parts[p.index] !== undefined && assembly.parts[p.index] !== p.data)) {
                fail(watch, "Inconsistent settled-output payload");
                return;
            }
            assembly.parts[p.index] = p.data;
            watch.status = "receiving";
            if (Object.keys(assembly.parts).length !== assembly.total) return;
            const bytes = Buffer.concat(assembly.parts.map((part) => Buffer.from(part, "base64")));
            if (bytes.length > MAX_OUTPUT_BYTES) { fail(watch, "Settled output exceeds limit"); return; }
            const output = bytes.toString("utf8");
            try {
                host.sendMessage({
                    customType: NAMESPACE,
                    content: `[pi-intercom-hook] Worker run settled (not a task-success assertion).\n${JSON.stringify({ sessionId: from, watchId: watch.watchId, aborted: assembly.aborted, stopReason: assembly.stopReason, truncated: assembly.truncated })}\n${assembly.truncated ? "[Output truncated to 256 KiB]\n" : ""}${output || "[No final assistant text]"}`,
                    display: true,
                    details: { watchId: watch.watchId, sessionId: from, output, aborted: assembly.aborted, stopReason: assembly.stopReason, truncated: assembly.truncated },
                }, { triggerTurn: true, deliverAs: "followUp" });
                watches.delete(watch.watchId);
                assemblies.delete(watch.watchId);
            } catch (error) { fail(watch, `Wake request failed: ${errorText(error)}`); }
        }
    }
    function register() {
        if (channel) return;
        host.events.emit(REGISTER, {
            namespace: NAMESPACE,
            ownerEligible: false,
            onEvent,
            onReady(candidate: unknown) {
                if (record(candidate) && typeof candidate.snapshot === "function" && typeof candidate.listSessions === "function" && typeof candidate.publish === "function") channel = candidate as Channel;
            },
        });
    }
    function connect() {
        unsubscribe ??= host.events.on(READY, register);
        register();
    }
    connect();
    host.on("session_start", () => { reset("Session changed"); active = true; connect(); });
    host.on("session_shutdown", () => {
        for (const w of watches.values()) cancelRemote(w);
        active = false;
        reset("Session shut down");
        unsubscribe?.();
        unsubscribe = undefined;
    });
    host.on("agent_start", () => {
        final = { text: "", stopReason: "unknown", truncated: false };
        sawAssistant = false;
    });
    host.on("message_end", (event) => {
        if (event.message.role !== "assistant") return;
        sawAssistant = true;
        final = captureText(event.message.content, event.message.stopReason);
    });
    host.on("agent_settled", (event, ctx) => {
        if (!active) return;
        // Later message_end handlers can replace/redact the message. Read the finalized
        // persisted assistant response at settlement, not this hook's earlier observation.
        if (sawAssistant) {
            const branch = ctx.sessionManager?.getBranch?.();
            for (let i = (branch?.length ?? 0) - 1; i >= 0; i--) {
                const entry = branch![i]!;
                if (entry.type === "message" && entry.message.role === "assistant") {
                    final = captureText(entry.message.content, entry.message.stopReason);
                    break;
                }
            }
        }
        const bytes = Buffer.from(final.text, "utf8");
        const total = Math.max(1, Math.ceil(bytes.length / CHUNK_BYTES));
        for (const [key, s] of subscriptions) {
            if (s.status !== "watching") continue;
            try {
                for (let index = 0; index < total; index++) publish({ kind: "settled", to: s.to, watchId: s.watchId, endpointEpoch: s.endpointEpoch, index, total, data: bytes.subarray(index * CHUNK_BYTES, (index + 1) * CHUNK_BYTES).toString("base64"), aborted: event.aborted, stopReason: final.stopReason, truncated: final.truncated });
                subscriptions.delete(key);
            } catch (error) { s.status = "failed"; s.error = `Output transport failed: ${errorText(error)}`; }
        }
        final = { text: "", stopReason: "unknown", truncated: false };
        sawAssistant = false;
    });

    return {
        async execute(input: unknown, mode: string, signal?: AbortSignal) {
            if (!record(input)) throw new Error("Expected intercom_hook arguments");
            const action = input.action ?? "watch";
            if (action === "list") return { watches: [...watches.values()].map((w) => ({ ...w })), subscriptions: [...subscriptions.values()].map((s) => ({ ...s })) };
            if (action === "cancel") {
                if (typeof input.watchId !== "string") throw new Error("cancel requires watchId");
                const w = watches.get(input.watchId);
                if (w) { watches.delete(w.watchId); assemblies.delete(w.watchId); pending.get(w.watchId)?.reject(new Error("Subscription cancelled")); cancelRemote(w); }
                return { watchId: input.watchId, cancelled: Boolean(w) };
            }
            if (action !== "watch") throw new Error("Unknown action; use watch, list or cancel");
            if (mode !== "tui" && mode !== "rpc") throw new Error("Subscription needs a reachable long-lived TUI/RPC coordinator; print/json dispose their receiver");
            if (typeof input.to !== "string" || !input.to.trim()) throw new Error("watch requires a worker intercom ID or unique name");
            signal?.throwIfAborted();
            const current = available();
            const epoch = generation;
            const target = resolveTarget(await current.listSessions(), input.to.trim(), selfId()!);
            signal?.throwIfAborted();
            if (!active || epoch !== generation || current !== available()) throw new Error("Coordinator session changed; retry");
            if (watches.size >= LIMIT) throw new Error("Watch limit reached (64); cancel records first");
            const watch: Watch = { watchId: randomUUID(), sessionId: target.id, endpointEpoch: target.endpointEpoch, status: "subscribing" };
            watches.set(watch.watchId, watch);
            let timer: ReturnType<typeof setTimeout> | undefined;
            const abort = () => pending.get(watch.watchId)?.reject(new Error("Subscription aborted"));
            try {
                await new Promise<void>((resolve, reject) => {
                    pending.set(watch.watchId, { resolve, reject });
                    timer = setTimeout(() => reject(new Error("Worker did not acknowledge subscription; load pi-intercom-hook in the worker and retry")), options.ackTimeoutMs ?? 5_000);
                    signal?.addEventListener("abort", abort, { once: true });
                    publish({ kind: "subscribe", to: target.id, endpointEpoch: target.endpointEpoch, watchId: watch.watchId });
                });
                return { watch: { ...watch }, note: "Worker acknowledged: one-shot next agent_settled output subscription. No process-exit monitoring or task-success claim. Coordinator must remain reachable." };
            } catch (error) {
                watches.delete(watch.watchId);
                assemblies.delete(watch.watchId);
                cancelRemote(watch);
                throw error;
            } finally {
                clearTimeout(timer);
                signal?.removeEventListener("abort", abort);
                pending.delete(watch.watchId);
            }
        },
    };
}

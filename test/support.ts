import { createHook } from "../src/hook.ts";

/** In-memory equivalent of the public bus: sender ID is transport-owned, capable audience includes sender. */
export function network() {
    const peers = new Map<string, any>();
    const frames: any[] = [];
    const roster = () => [...peers].map(([id, p]) => ({ id, name: p.name, endpointEpoch: p.epoch, trustedLocal: true }));
    function peer(id: string, options: { intercomFirst?: boolean; timeout?: number } = {}) {
        const handlers = new Map<string, any[]>();
        const listeners = new Map<string, Set<any>>();
        const messages: any[] = [];
        let registration: any;
        let registryReady = options.intercomFirst !== false;
        const p: any = { id, name: id, epoch: `epoch-${id}`, connected: true, messages, throwSend: false };
        peers.set(id, p);
        const emit = (name: string, payload: any) => {
            if (name === "intercom:extension-register" && registryReady) {
                registration = payload;
                payload.onReady(p.channel);
            }
            for (const listener of listeners.get(name) ?? []) listener(payload);
        };
        p.channel = {
            snapshot: () => ({ connected: p.connected, supported: true }),
            listSessions: async () => roster(),
            publish(payload: any, options: any) {
                if (!p.connected) throw new Error("disconnected");
                if (p.throwPublish) throw new Error("publish failure");
                if (options.audience !== "capable") throw new Error("expected capable audience");
                if (Buffer.byteLength(JSON.stringify(payload)) > 16 * 1024) throw new Error("broker payload limit exceeded");
                frames.push({ from: id, payload });
                for (const recipient of peers.values()) {
                    if (recipient.connected) queueMicrotask(() => recipient.deliver(id, payload));
                }
            },
        };
        p.host = {
            events: {
                emit,
                on(name: string, handler: any) {
                    if (!listeners.has(name)) listeners.set(name, new Set());
                    listeners.get(name)!.add(handler);
                    return () => listeners.get(name)?.delete(handler);
                },
            },
            on(name: string, handler: any) {
                if (!handlers.has(name)) handlers.set(name, []);
                handlers.get(name)!.push(handler);
            },
            sendMessage(message: any, options: any) {
                if (p.throwSend) throw new Error("send failure");
                messages.push({ message, options });
            },
        };
        p.deliver = (fromSessionId: string, payload: any) => registration?.onEvent({ type: "message", fromSessionId, payload });
        p.branch = [];
        p.fire = async (name: string, payload: any = {}) => {
            if (name === "session_start") p.branch = [];
            if (name === "message_end") p.branch.push({ type: "message", message: payload.message });
            for (const handler of handlers.get(name) ?? []) await handler({ type: name, ...payload }, { mode: "rpc", sessionManager: { getBranch: () => p.branch } });
        };
        p.ready = () => { registryReady = true; emit("intercom:extension-registry-ready", {}); };
        p.disconnect = () => { p.connected = false; registration?.onEvent({ type: "connection", connected: false, supported: false }); };
        p.hook = createHook(p.host, { selfId: () => id, ackTimeoutMs: options.timeout ?? 100 });
        p.text = (text: string, stopReason = "stop") => p.fire("message_end", { message: { role: "assistant", content: [{ type: "text", text }], stopReason } });
        p.list = () => p.hook.execute({ action: "list" }, "rpc");
        p.watch = (to: string, signal?: AbortSignal) => p.hook.execute({ to }, "rpc", signal);
        return p;
    }
    return { peer, peers, frames, roster };
}
export async function flush() { await new Promise((resolve) => setImmediate(resolve)); }
export async function pair(options: { intercomFirst?: boolean; timeout?: number } = {}) {
    const net = network();
    const coordinator = net.peer("coordinator", options);
    const worker = net.peer("worker", options);
    await coordinator.fire("session_start");
    await worker.fire("session_start");
    if (options.intercomFirst === false) { coordinator.ready(); worker.ready(); }
    return { ...net, coordinator, worker };
}

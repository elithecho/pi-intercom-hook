// Real installed Pi loader/schema and lifecycle handlers, paired simulated public intercom bus.
// No provider/model calls, settings changes, process-exit monitoring or shared broker mutation.
import assert from "node:assert/strict";
import { resolve, dirname } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

if (!process.argv[2]) throw new Error("Usage: node scripts/smoke.mjs /path/to/@earendil-works/pi-coding-agent");
const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const sdk = resolve(process.argv[2]);
const { createEventBus } = await import(pathToFileURL(resolve(sdk, "dist/core/event-bus.js")).href);
const { loadExtensions, createExtensionRuntime } = await import(pathToFileURL(resolve(sdk, "dist/core/extensions/loader.js")).href);
const peers = new Map();
const originalId = process.env.PI_INTERCOM_SESSION_ID;
function asPeer(id, action) {
    const previous = process.env.PI_INTERCOM_SESSION_ID;
    process.env.PI_INTERCOM_SESSION_ID = id;
    try { return action(); } finally {
        if (previous === undefined) delete process.env.PI_INTERCOM_SESSION_ID;
        else process.env.PI_INTERCOM_SESSION_ID = previous;
    }
}
async function peer(id) {
    const runtime = createExtensionRuntime();
    const events = createEventBus();
    const p = { id, runtime, messages: [] };
    peers.set(id, p);
    events.on("intercom:extension-register", (registration) => {
        p.registration = registration;
        registration.onReady({
            snapshot: () => ({ connected: true, supported: true }),
            listSessions: async () => [...peers.keys()].map((id) => ({ id, endpointEpoch: `epoch-${id}`, trustedLocal: true })),
            publish(payload, options) {
                assert.equal(options.audience, "capable");
                assert.ok(Buffer.byteLength(JSON.stringify(payload)) <= 16 * 1024);
                for (const recipient of peers.values()) queueMicrotask(() => asPeer(recipient.id, () => recipient.registration?.onEvent({ type: "message", fromSessionId: id, payload })));
            },
        });
    });
    const loaded = await loadExtensions([resolve(root, "extensions/index.ts")], root, events, runtime);
    assert.deepEqual(loaded.errors, []);
    p.extension = loaded.extensions[0];
    assert.ok(p.extension);
    p.tool = p.extension.tools.get("intercom_hook")?.definition;
    assert.equal(p.tool?.parameters.type, "object");
    assert.ok(p.tool?.parameters.properties.to);
    assert.ok(p.extension.handlers.has("agent_settled"));
    assert.equal(p.extension.handlers.has("agent_end"), false);
    runtime.sendMessage = (message, options) => p.messages.push({ message, options });
    p.fire = async (type, payload = {}) => {
        for (const handler of p.extension.handlers.get(type) ?? []) await asPeer(id, () => handler({ type, ...payload }, { mode: "rpc" }));
    };
    p.execute = (input) => asPeer(id, () => p.tool.execute("smoke", input, undefined, undefined, { mode: "rpc" }));
    await p.fire("session_start", { reason: "new" });
    return p;
}
try {
    const coordinator = await peer("smoke-coordinator");
    const worker = await peer("smoke-worker");
    // Async tool code reads identity after roster resolution; model separate processes
    // by retaining the coordinator identity while the tool is running. Bus callbacks
    // temporarily switch to their receiving process identity synchronously.
    process.env.PI_INTERCOM_SESSION_ID = coordinator.id;
    const result = await coordinator.execute({ to: worker.id });
    assert.equal(result.details.watch.status, "watching");
    await worker.fire("agent_start");
    await worker.fire("message_end", { message: { role: "assistant", content: [{ type: "text", text: "intermediate" }], stopReason: "stop" } });
    await worker.fire("agent_end", { messages: [] });
    assert.equal(coordinator.messages.length, 0);
    await worker.fire("message_end", { message: { role: "assistant", content: [{ type: "text", text: "Final output, automatically returned" }], stopReason: "stop" } });
    await worker.fire("agent_settled", { aborted: false });
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(coordinator.messages.length, 1);
    assert.equal(coordinator.messages[0].message.details.output, "Final output, automatically returned");
    assert.deepEqual(coordinator.messages[0].options, { triggerTurn: true, deliverAs: "followUp" });
    console.log("PASS: installed Pi loaded two hook instances and tool schema; worker acknowledgement armed subscription; agent_end did not wake; agent_settled automatically returned final output and requested coordinator follow-up (no worker tool call)");
} finally {
    for (const p of peers.values()) { await p.fire?.("session_shutdown"); p.runtime.invalidate("Smoke finished"); }
    if (originalId === undefined) delete process.env.PI_INTERCOM_SESSION_ID;
    else process.env.PI_INTERCOM_SESSION_ID = originalId;
}

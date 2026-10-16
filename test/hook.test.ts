import assert from "node:assert/strict";
import test from "node:test";
import { MAX_OUTPUT_BYTES, captureText, resolveTarget } from "../src/hook.ts";
import { pair, flush } from "./support.ts";

for (const intercomFirst of [true, false]) {
    test(`acknowledged next-settlement subscription, either load order (${intercomFirst})`, async () => {
        const { coordinator: c, worker: w, frames } = await pair({ intercomFirst });
        const result = await c.watch("worker");
        assert.equal(result.watch.status, "watching");
        assert.equal((await w.list()).subscriptions.length, 1);
        await w.fire("agent_start");
        await w.text("intermediate answer");
        await w.fire("agent_end", { messages: [] });
        await flush();
        assert.equal(c.messages.length, 0, "agent_end is not final");
        // Simulate automatic continuation after agent_end.
        await w.fire("agent_start");
        await w.text("actual final answer");
        await w.fire("agent_end", { messages: [] });
        assert.equal(c.messages.length, 0);
        await w.fire("agent_settled", { aborted: false });
        await flush();
        assert.equal(c.messages.length, 1);
        assert.equal(c.messages[0].message.details.output, "actual final answer");
        assert.equal(c.messages[0].message.details.aborted, false);
        assert.deepEqual(c.messages[0].options, { triggerTurn: true, deliverAs: "followUp" });
        assert.match(c.messages[0].message.content, /not a task-success assertion/);
        assert.equal((await c.list()).watches.length, 0);
        assert.equal((await w.list()).subscriptions.length, 0);
        await w.fire("agent_settled", { aborted: false });
        for (const frame of frames.filter((f) => f.payload.kind === "settled")) c.deliver(frame.from, frame.payload);
        assert.equal(c.messages.length, 1, "one shot despite duplicate settled event/result");
    });
}

test("idle subscription does not replay old final output; multiple coordinators work", async () => {
    const { coordinator: c, worker: w, peer } = await pair();
    await w.text("old final");
    await w.fire("agent_settled", { aborted: false });
    const other = peer("other");
    await other.fire("session_start");
    await c.watch("worker");
    await other.watch("worker");
    assert.equal(c.messages.length, 0);
    await w.fire("agent_start");
    await w.text("new final");
    await w.fire("agent_settled", { aborted: false });
    await flush();
    assert.equal(c.messages[0].message.details.output, "new final");
    assert.equal(other.messages[0].message.details.output, "new final");
});

test("chunked multibyte text survives out-of-order and duplicate transport within broker cap", async () => {
    const { coordinator: c, worker: w, frames } = await pair();
    await c.watch("worker");
    const originalDeliver = c.deliver;
    c.deliver = () => {};
    const output = 'hello 🚀\n"\\'.repeat(4000);
    await w.text(output);
    await w.fire("agent_settled", { aborted: false });
    await flush();
    const parts = frames.filter((f) => f.payload.kind === "settled");
    assert.ok(parts.length > 1);
    for (const f of [...parts].reverse()) { originalDeliver(f.from, f.payload); originalDeliver(f.from, f.payload); }
    assert.equal(c.messages.length, 1);
    assert.equal(c.messages[0].message.details.output, output);
});

test("oversize output is explicitly truncated at UTF-8 boundary", async () => {
    const { coordinator: c, worker: w } = await pair();
    await c.watch("worker");
    await w.text("🚀".repeat(MAX_OUTPUT_BYTES));
    await w.fire("agent_settled", { aborted: false });
    await flush();
    const result = c.messages[0].message;
    assert.equal(result.details.truncated, true);
    assert.ok(Buffer.byteLength(result.details.output) <= MAX_OUTPUT_BYTES);
    assert.ok(!result.details.output.includes("\uFFFD"));
    assert.match(result.content, /Output truncated/);
});

for (const [text, stopReason, aborted] of [["", "unknown", true], ["partial failure", "error", false], ["stopped", "aborted", true]] as const) {
    test(`settlement metadata: stopReason=${stopReason} aborted=${aborted}`, async () => {
        const { coordinator: c, worker: w } = await pair();
        await c.watch("worker");
        await w.fire("agent_start");
        if (text) await w.text(text, stopReason);
        await w.fire("agent_settled", { aborted });
        await flush();
        assert.deepEqual(c.messages[0].message.details, { watchId: c.messages[0].message.details.watchId, sessionId: "worker", output: text, stopReason, aborted, truncated: false });
        if (!text) assert.match(c.messages[0].message.content, /No final assistant text/);
    });
}

test("missing worker hook times out, does not claim armed, and sends cancel", async () => {
    const { coordinator: c, worker: w, frames } = await pair({ timeout: 10 });
    w.deliver = () => {};
    await assert.rejects(c.watch("worker"), /did not acknowledge/);
    assert.equal((await c.list()).watches.length, 0);
    assert.equal(frames.at(-1).payload.kind, "cancel");
});

test("tool abort and local cancel suppress delayed acknowledgement and output", async () => {
    const { coordinator: c, worker: w } = await pair();
    const deferred = Promise.withResolvers<any>();
    w.channel.listSessions = () => deferred.promise;
    const controller = new AbortController();
    const pending = c.watch("worker", controller.signal);
    await flush();
    controller.abort();
    await assert.rejects(pending, /aborted/);
    await flush();
    deferred.resolve([{ id: "worker", endpointEpoch: w.epoch }]);
    await flush();
    assert.equal((await w.list()).subscriptions.length, 0);
    const watch = await c.watch("worker");
    assert.equal((await c.hook.execute({ action: "cancel", watchId: watch.watch.watchId }, "rpc")).cancelled, true);
    await flush();
    await w.text("late");
    await w.fire("agent_settled", { aborted: false });
    await flush();
    assert.equal(c.messages.length, 0);
});

test("session shutdown fences roster resolution and pending subscriptions", async () => {
    const { coordinator: c, worker: w } = await pair();
    const roster = Promise.withResolvers<any>();
    c.channel.listSessions = () => roster.promise;
    const registration = c.watch("worker");
    await c.fire("session_shutdown");
    roster.resolve([{ id: "worker", endpointEpoch: w.epoch, trustedLocal: true }]);
    await assert.rejects(registration, /connection|session changed/i);
    await c.fire("session_start");
    w.deliver = () => {};
    const pending = c.watch("worker");
    await flush();
    await c.fire("session_shutdown");
    await assert.rejects(pending, /shut down/);
    assert.equal((await c.list()).watches.length, 0);
});

test("worker shutdown clears subscriptions; next session can re-arm", async () => {
    const { coordinator: c, worker: w } = await pair();
    await c.watch("worker");
    await w.fire("session_shutdown");
    await w.fire("agent_settled", { aborted: false });
    assert.equal(c.messages.length, 0);
    assert.equal((await w.list()).subscriptions.length, 0);
    await w.fire("session_start");
    const next = await c.watch("worker");
    await w.text("next session");
    await w.fire("agent_settled", { aborted: false });
    await flush();
    assert.equal(c.messages.length, 1);
    assert.equal(c.messages[0].message.details.watchId, next.watch.watchId);
});

test("disconnect is not completion; fails local watches and clears worker subscriptions", async () => {
    const { coordinator: c, worker: w } = await pair();
    await c.watch("worker");
    c.disconnect();
    assert.equal((await c.list()).watches[0].status, "failed");
    assert.equal(c.messages.length, 0);
    await assert.rejects(c.watch("worker"), /connection/);
    w.disconnect();
    assert.equal((await w.list()).subscriptions.length, 0);
});

test("sender, endpoint epoch and addressed receiver are checked", async () => {
    const { coordinator: c, worker: w } = await pair();
    const result = await c.watch("worker");
    const payload = { version: 1, kind: "settled", to: "coordinator", watchId: result.watch.watchId, endpointEpoch: w.epoch, total: 1, index: 0, data: Buffer.from("forged").toString("base64"), aborted: false, stopReason: "stop", truncated: false };
    c.deliver("attacker", payload);
    c.deliver("worker", { ...payload, endpointEpoch: "old" });
    c.deliver("worker", { ...payload, to: "someone-else" });
    assert.equal(c.messages.length, 0);
    assert.equal((await c.list()).watches[0].status, "watching");
    c.deliver("worker", { ...payload, total: 500 });
    assert.equal((await c.list()).watches[0].status, "failed");
});

test("endpoint changed while registering is rejected by worker", async () => {
    const { coordinator: c, worker: w } = await pair();
    w.epoch = "replacement";
    // Force coordinator's first snapshot stale; worker sees its current epoch.
    c.channel.listSessions = async () => [{ id: "worker", endpointEpoch: "stale", trustedLocal: true }];
    await assert.rejects(c.watch("worker"), /endpoint changed/);
    assert.equal((await w.list()).subscriptions.length, 0);
});

test("dispatch and worker publish failures remain inspectable, not success claims", async () => {
    const { coordinator: c, worker: w } = await pair();
    await c.watch("worker");
    c.throwSend = true;
    await w.text("output");
    await w.fire("agent_settled", { aborted: false });
    await flush();
    assert.match((await c.list()).watches[0].error, /Wake request failed/);
    await c.watch("worker");
    w.throwPublish = true;
    await w.fire("agent_settled", { aborted: false });
    assert.match((await w.list()).subscriptions[0].error, /Output transport failed/);
});

test("64-record bounds on coordinator and worker", async () => {
    const { coordinator: c, worker: w, peer } = await pair();
    for (let i = 0; i < 64; i++) await c.watch("worker");
    await assert.rejects(c.watch("worker"), /Watch limit/);
    const other = peer("other");
    await other.fire("session_start");
    await assert.rejects(other.watch("worker"), /Worker subscription limit/);
    assert.equal((await w.list()).subscriptions.length, 64);
});

test("capture bounds memory before joining/encoding, preserving text-block and Unicode boundaries", () => {
    const captured = captureText([{ type: "thinking", text: "private" }, { type: "text", text: "x".repeat(20 * MAX_OUTPUT_BYTES) }, { type: "text", text: "tail" }], "stop");
    assert.equal(Buffer.byteLength(captured.text), MAX_OUTPUT_BYTES);
    assert.equal(captured.truncated, true);
    assert.deepEqual(captureText([{ type: "text", text: "a" }, { type: "text", text: "🚀" }], "stop"), { text: "a\n🚀", truncated: false, stopReason: "stop" });
    const boundary = captureText([{ type: "text", text: "x".repeat(MAX_OUTPUT_BYTES - 1) + "🚀" }], "stop");
    assert.equal(boundary.text.length, MAX_OUTPUT_BYTES - 1);
    assert.equal(boundary.truncated, true);
    const replacementChar = captureText([{ type: "text", text: "x".repeat(MAX_OUTPUT_BYTES - 3) + "\uFFFDtail" }], "stop");
    assert.ok(replacementChar.text.endsWith("\uFFFD"), "real replacement character must not be stripped");
});

test("settled output uses final persisted message after later message_end replacement/redaction", async () => {
    const { coordinator: c, worker: w } = await pair();
    await c.watch("worker");
    await w.fire("agent_start");
    await w.text("unredacted secret");
    w.branch.at(-1).message = { role: "assistant", content: [{ type: "text", text: "redacted final" }], stopReason: "stop" };
    await w.fire("agent_settled", { aborted: false });
    await flush();
    assert.equal(c.messages[0].message.details.output, "redacted final");
});

test("mode, argument and roster validation", async () => {
    const { coordinator: c } = await pair();
    for (const mode of ["print", "json"]) await assert.rejects(c.hook.execute({ to: "worker" }, mode), /long-lived/);
    for (const input of [null, {}, { action: "invalid" }, { action: "cancel" }]) await assert.rejects(c.hook.execute(input, "rpc"));
    const roster = [{ id: "worker-1", name: "Same", trustedLocal: true, endpointEpoch: "1" }, { id: "worker-2", name: "same", trustedLocal: true, endpointEpoch: "2" }];
    assert.throws(() => resolveTarget(roster, "same", "self"), /Ambiguous/);
    assert.throws(() => resolveTarget(roster, "worker", "self"), /Ambiguous/);
    assert.throws(() => resolveTarget(roster, "missing", "self"), /not found/);
    assert.throws(() => resolveTarget(roster, "worker-1", "worker-1"), /this session/);
    assert.throws(() => resolveTarget([{ id: "bad" }], "bad", "self"), /trusted local/);
    assert.equal(resolveTarget(roster, "worker-1", "self").id, "worker-1");
    const controller = new AbortController(); controller.abort();
    await assert.rejects(c.watch("worker", controller.signal), /abort/i);
});

import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
// Run with Bun (installed intercom sources use TypeScript parameter properties).
// Usage: bun scripts/broker-smoke.ts /path/to/pi-intercom
import { createHook } from '../src/hook.ts';
if (!process.argv[2]) throw new Error('Usage: bun scripts/broker-smoke.ts /path/to/pi-intercom');
const agentDir = mkdtempSync(join(tmpdir(), 'pi-hook-live-'));
process.env.PI_CODING_AGENT_DIR = agentDir;
process.env.PI_INTERCOM_SCOPE_ID = 'hook-isolated-smoke';
const intercomRoot = resolve(process.argv[2]);
const { IntercomClient } = await import(`${intercomRoot}/broker/client.ts`);
const broker = spawn(process.execPath, [`${intercomRoot}/broker/broker.ts`], { env: process.env, stdio: ['ignore', 'pipe', 'pipe'] });
let stderr = '';
broker.stderr.on('data', c => stderr += c);
const clients: any[] = [];
const peers: any[] = [];
try {
    await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`Broker startup timeout ${stderr}`)), 10000);
        broker.stdout.on('data', c => { if (c.toString().includes('Intercom broker started')) { clearTimeout(timer); resolve(); } });
        broker.once('exit', () => { clearTimeout(timer); reject(new Error(`Broker exited ${stderr}`)); });
    });
    async function peer(id: string) {
        const client = new IntercomClient(); clients.push(client);
        await client.connect({ name: id, cwd: '/tmp/pi-hook-smoke', model: 'no-model', pid: process.pid, startedAt: Date.now(), lastActivity: Date.now(), extensions: [{ namespace: 'pi-intercom-hook', ownerEligible: false }] }, id);
        const handlers = new Map<string, any[]>();
        let registration: any;
        const p: any = { id, messages: [] };
        const host: any = {
            events: {
                on: () => () => {},
                emit(name: string, reg: any) {
                    if (name !== 'intercom:extension-register') return;
                    registration = reg;
                    reg.onReady({
                        snapshot: () => ({ connected: client.isConnected(), supported: client.supportsFeature('extension-bus-v1') }),
                        listSessions: () => client.listSessions(),
                        publish(payload: any, options: any) { client.sendExtensionMessage({ type: 'extension_publish', namespace: 'pi-intercom-hook', audience: options.audience, payload }); },
                    });
                },
            },
            on(name: string, handler: any) { if (!handlers.has(name)) handlers.set(name, []); handlers.get(name)!.push(handler); },
            sendMessage(message: any, options: any) { p.messages.push({ message, options }); },
        };
        client.onBrokerMessage((m: any) => {
            if (m.type === 'extension_message') registration?.onEvent({ type: 'message', fromSessionId: m.fromSessionId, payload: m.payload });
            if (m.type === 'error') throw new Error(m.error);
        });
        p.hook = createHook(host, { selfId: () => id, ackTimeoutMs: 3000 });
        p.fire = async (name: string, data: any = {}) => { for (const h of handlers.get(name) ?? []) await h({ type: name, ...data }, {}); };
        await p.fire('session_start');
        peers.push(p);
        return p;
    }
    const c = await peer('hook-coordinator');
    const w = await peer('hook-worker');
    const watch = await c.hook.execute({ to: w.id }, 'rpc');
    assert.equal(watch.watch.status, 'watching');
    await w.fire('agent_start');
    const output = 'Real broker settled output 🚀\n'.repeat(2500);
    await w.fire('message_end', { message: { role: 'assistant', content: [{ type: 'text', text: output }], stopReason: 'stop' } });
    await w.fire('agent_end', { messages: [] });
    assert.equal(c.messages.length, 0);
    await w.fire('agent_settled', { aborted: false });
    const deadline = Date.now() + 3000;
    while (!c.messages.length && Date.now() < deadline) await new Promise(r => setTimeout(r, 10));
    assert.equal(c.messages.length, 1);
    assert.equal(c.messages[0].message.details.output, output);
    assert.deepEqual(c.messages[0].options, { triggerTurn: true, deliverAs: 'followUp' });
    assert.equal(w.messages.length, 0);
    console.log('PASS: isolated installed intercom broker + actual IntercomClient pair: acknowledged worker subscription, no agent_end wake, multi-frame Unicode final output at agent_settled requested coordinator wake without worker tool call');
} finally {
    for (const p of peers) await p.fire('session_shutdown');
    for (const client of clients) await client.disconnect();
    const exit = broker.exitCode === null ? once(broker, 'exit') : Promise.resolve();
    broker.kill('SIGTERM'); await exit;
    rmSync(agentDir, { recursive: true, force: true });
}

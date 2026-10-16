# pi-intercom-hook

Subscribe to a Pi worker's **next final settled output**. When the worker's run settles, a lifecycle hook automatically sends its final assistant text through intercom and requests a new coordinator turn. The worker does **not** need to remember to call a tool or send a report.

This is **not process-exit monitoring**. A worker can remain alive after returning its final output. `agent_end` is not enough: Pi can automatically retry, compact, or continue afterward. This extension uses the installed Pi API's notification-only **`agent_settled`** boundary.

## Load in both sessions

Both the **worker and coordinator** must load `pi-intercom` with extension-bus support and this package. Try it without changing settings:

```sh
pi -e /path/to/pi-intercom -e /Users/elijah/Workspace/pi/pi-intercom-hook
```

The package has not been installed automatically. No added runtime dependencies are required; it uses host-provided Pi packages and Node built-ins. Sources/tests require Node 22.19+; Pi must support `agent_settled` and intercom must expose endpoint epochs and extension-bus v1.

## Coordinator usage

```js
intercom_hook({ to: "worker-session-id" });
// Briefly waits for worker acknowledgement, then returns a watchId.
// At the worker's next agent_settled: final output arrives and wakes this coordinator.

intercom_hook({ action: "list" });
intercom_hook({ action: "cancel", watchId: "returned-watch-id" });
```

Targets support full intercom ID, exact unique name (case-insensitive), or unique ID prefix. Use `intercom` → `list` to find a worker. Self-subscriptions and untrusted/nonlocal endpoints are rejected. Each call creates a separate, one-shot watch; register again to watch another run.

### Lifecycle

1. Coordinator resolves the live worker endpoint and sends a subscription request over intercom's public extension bus.
2. Worker validates its endpoint epoch, stores the subscription, and acknowledges it. Only then does the tool report it as armed. If the worker lacks the hook, the call fails after five seconds rather than claiming success.
3. Intermediate `agent_end` events do not notify. After automatic continuations are finished, `agent_settled` sends the latest assistant response text, with `aborted`, `stopReason`, and `truncated` metadata.
4. Coordinator receives that output and calls Pi's `sendMessage` with `triggerTurn: true` and `deliverAs: "followUp"`. An idle coordinator starts a turn; a busy coordinator receives a follow-up. The watch is consumed. This is a **wake request**, not proof that the coordinator's model ran successfully.

An idle worker waits for its **next** settlement; there is no replay of an already-settled response. A settlement during subscription validation, before acknowledgement, is not covered. Subscribe before dispatching work when that boundary matters.

Only the latest assistant response's text blocks are sent—not thinking, tool arguments/results, images, or the whole transcript. An aborted/error run still reports its actual settlement metadata; no output is represented explicitly. Settlement never asserts task success or automatically resumes the worker.

## Limits and failures

- **Coordinator must remain reachable.** Arming requires a long-lived native Pi TUI/RPC session; print/JSON dispose their receiving endpoint after the prompt and cannot arm. There is no coordinator polling loop, background daemon, or process watcher. The only coordinator timer bounds the initial acknowledgement wait.
- **Memory-only, best-effort delivery.** No durable subscriptions or reconnect guarantees. Reload/session replacement/shutdown clears the corresponding session's state. A local intercom disconnect clears worker subscriptions and marks local watches failed. Worker-only disconnection/shutdown can leave a coordinator watch waiting with no result; inspect connectivity and cancel/re-arm. Process crashes and SIGKILL detection are outside scope.
- Cancelling immediately suppresses local delivery and sends best-effort remote cancellation. Timed-out/aborted registrations also attempt cancellation. Remote records can survive a lost cancellation until worker reset.
- Up to **64 coordinator watches** and **64 worker subscriptions**, including failures. `list` shows outgoing watches and incoming subscriptions; failed wake/transport records expose errors and are not retried automatically. `cancel` removes outgoing watches; incoming records clear when consumed or on worker reset.
- Output is bounded to **256 KiB UTF-8**, explicitly marked when truncated, and chunked below the broker's 16 KiB frame limit. Missing chunks leave a watch in `receiving`; no delivery receipt or retry is implied. Read the worker session directly if full output is needed.
- The public extension bus is **same-scope capable broadcast**, with receiver/worker IDs and endpoint epochs filtered by this hook. Sender IDs come from the broker, not payload claims. Other sessions capable of this namespace in that scope can see payloads; this is **not confidential point-to-point transport**.
- Pi's `sendMessage` returns void. Synchronous dispatch failure is inspectable locally; asynchronous run-start failure is reported by Pi, not acknowledged to this extension.
- Native Pi only. Pi Pocket's partial terminal-plugin adapter is not supported; this work does not change or resume its isolation/bridge work.

## Checks

```sh
npm test
node scripts/smoke.mjs /path/to/node_modules/@earendil-works/pi-coding-agent
# Optional: real installed intercom broker/client, isolated temporary agent directory (requires Bun)
bun scripts/broker-smoke.ts /path/to/pi-intercom
```

Unit tests pair worker/coordinator hooks over a simulated public intercom bus, enforcing its 16 KiB payload bound. They cover both load orders, acknowledgement, idle subscriptions, intermediate vs settled events, continuation output, chunking/reordering/duplicates, Unicode/truncation, aborted/error/empty output, sender/endpoint checks, limits, cancellation/abort/timeouts, session cleanup, connection and dispatch failures.

The loader smoke uses the **actual installed Pi extension loader**, schema, tool definition and lifecycle handlers for two hook instances over a paired simulated bus. It proves a settled final response requests the coordinator wake without a worker tool call. It does **not** use a live broker or call a paid model, and does not prove model execution of the requested coordinator turn. The separate broker smoke uses the installed intercom broker and actual clients in a fresh temporary agent directory, checks a chunked Unicode final response, and cleans up its broker/files. It uses a minimal Pi host adapter rather than a model-backed session. Neither smoke makes settings changes, installs, or shared broker mutations.

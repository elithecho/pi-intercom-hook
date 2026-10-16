/** Subscribe to a worker's next final settled output over the intercom extension bus. */
import { Type } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { createHook } from "../src/hook.ts";

export default function (pi: ExtensionAPI) {
    const hook = createHook(pi);
    pi.registerTool({
        name: "intercom_hook",
        label: "Intercom hook",
        description: "Subscribe to a worker intercom session's next final output (agent_settled, after all automatic continuation). Both sessions must load pi-intercom and pi-intercom-hook. Waits briefly for worker acknowledgement, then returns; worker automatically sends output and wakes this reachable TUI/RPC coordinator without a worker tool call. Not process-exit monitoring or task success; no worker resumption. Idle workers wait for their next run. One-shot; list/cancel manage local watches. Subscriptions are memory-only and lost on disconnect/reload/session change.",
        parameters: Type.Object({
            action: Type.Optional(Type.Union([Type.Literal("watch"), Type.Literal("list"), Type.Literal("cancel")])),
            to: Type.Optional(Type.String({ description: "Worker full intercom ID, unique ID prefix or exact unique name" })),
            watchId: Type.Optional(Type.String({ description: "Watch ID returned by watch/list; used for cancel" })),
        }, { additionalProperties: false }),
        async execute(_id, input, signal, _update, ctx) {
            const details = await hook.execute(input, ctx.mode, signal);
            return { content: [{ type: "text", text: JSON.stringify(details) }], details };
        },
    });
}

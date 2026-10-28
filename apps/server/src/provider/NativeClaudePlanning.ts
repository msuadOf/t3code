import type { OrchestrationV2PlanStep } from "@t3tools/contracts";
import type { NativeHistoryItem } from "@t3tools/provider-core/server/nativeSessions";

type NativePlan =
  | { readonly kind: "todo_list"; readonly steps: ReadonlyArray<OrchestrationV2PlanStep> }
  | { readonly kind: "proposed_plan"; readonly markdown: string };

/** Read native tool fields only; ordinary tool output and assistant prose are not plans. */
export function nativeClaudePlan(item: NativeHistoryItem): NativePlan | undefined {
  if (item.type !== "tool" || item.status === "failed" || item.status === "cancelled") return;
  if (item.title === "TodoWrite" && item.status === "completed") {
    const todos = item.result?.newTodos;
    if (!Array.isArray(todos)) return;
    const steps: OrchestrationV2PlanStep[] = [];
    for (const [index, todo] of todos.entries()) {
      if (typeof todo !== "object" || todo === null) return;
      const { content, status } = todo;
      if (
        typeof content !== "string" ||
        content.trim() === "" ||
        (status !== "pending" && status !== "in_progress" && status !== "completed")
      )
        return;
      steps.push({
        id: `todo-${index}`,
        text: content.trim(),
        status: status === "in_progress" ? "running" : status,
      });
    }
    return { kind: "todo_list", steps };
  }
  if (item.title === "ExitPlanMode") {
    const plan =
      item.result !== undefined && "plan" in item.result ? item.result.plan : item.input?.plan;
    if (typeof plan === "string" && plan.trim() !== "")
      return { kind: "proposed_plan", markdown: plan };
  }
}

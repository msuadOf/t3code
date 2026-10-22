import { createFileRoute, redirect } from "@tanstack/react-router";

import { NativeCodexPage } from "../codex-native/NativeCodexPage";

export const Route = createFileRoute("/native-codex")({
  beforeLoad: ({ context }) => {
    if (context.authGateState.status !== "authenticated") {
      throw redirect({ to: "/pair", replace: true });
    }
  },
  component: NativeCodexPage,
});

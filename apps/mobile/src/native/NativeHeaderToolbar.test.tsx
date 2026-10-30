import type { NativeStackHeaderItem } from "@react-navigation/native-stack";
import { act, createContext, type ReactElement } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, expect, it, vi } from "vite-plus/test";

type HeaderOptions = {
  unstable_headerLeftItems?: () => NativeStackHeaderItem[];
  unstable_headerRightItems?: (props?: { tintColor: string }) => NativeStackHeaderItem[];
  unstable_headerToolbarItems?: () => NativeStackHeaderItem[];
};

const harness = vi.hoisted(() => ({
  params: { path: ["first", "guide.pdf"] },
  appliedOptions: {} as HeaderOptions,
  renderedItems: [] as NativeStackHeaderItem[],
  setOptionsCalls: 0,
  openedDocuments: [] as string[],
  itemProps: undefined as { tintColor: string } | undefined,
}));

// Native headers retain the actions materialized when navigation applies options. Like React
// Navigation, the header renders again only when the options are no longer shallowly equal.
vi.mock("@react-navigation/native", () => {
  const navigation = {
    setOptions(options: HeaderOptions) {
      harness.setOptionsCalls += 1;
      const previous: Record<string, unknown> = harness.appliedOptions;
      const next: Record<string, unknown> = { ...previous, ...options };
      harness.appliedOptions = next;
      if (
        Object.keys(next).length === Object.keys(previous).length &&
        Object.keys(next).every((key) => next[key] === previous[key])
      )
        return;
      harness.renderedItems =
        options.unstable_headerLeftItems?.() ??
        options.unstable_headerRightItems?.(harness.itemProps) ??
        options.unstable_headerToolbarItems?.() ??
        [];
    },
  };
  return {
    useNavigation: () => navigation,
    useRoute: () => ({ key: "file", name: "ThreadFile", params: harness.params }),
  };
});

import { NativeHeaderToolbar } from "./NativeHeaderToolbar";
import { NativeStackScreenOptions } from "./StackHeader";

let root: Root | null = null;
type Placement = "left" | "right" | "bottom";

function DocumentToolbar(props: { readonly path: string; readonly placement: Placement }) {
  return (
    <NativeHeaderToolbar placement={props.placement}>
      <NativeHeaderToolbar.Menu title="File actions" icon="ellipsis">
        <NativeHeaderToolbar.MenuAction onPress={() => harness.openedDocuments.push(props.path)}>
          Open PDF
        </NativeHeaderToolbar.MenuAction>
      </NativeHeaderToolbar.Menu>
    </NativeHeaderToolbar>
  );
}

function renderDocument(path: string, placement: Placement) {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  harness.params = { path: path.split("/") };
  root ??= createRoot(document.createElement("div"));
  act(() => root!.render(<DocumentToolbar path={path} placement={placement} />));
}

// Like React Navigation's NavigationStateContext, whose default value throws when
// read. React restores a context's default outside render, so layout effects see it.
const ContainerStateContext = createContext<object>({
  get getIsInitial(): never {
    throw new Error("Couldn't find a navigation context.");
  },
});
const containerState = {};

function ConnectingToolbar(props: { readonly label: string }) {
  return (
    <NativeHeaderToolbar placement="right">
      <NativeHeaderToolbar.Custom>
        <span aria-label={props.label} />
      </NativeHeaderToolbar.Custom>
    </NativeHeaderToolbar>
  );
}

function renderConnecting(label: string) {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  root ??= createRoot(document.createElement("div"));
  act(() =>
    root!.render(
      <ContainerStateContext value={containerState}>
        <ConnectingToolbar label={label} />
      </ContainerStateContext>,
    ),
  );
}

function renderedCustomLabel() {
  const item = harness.renderedItems[0];
  if (item?.type !== "custom") throw new Error("Expected the custom header element");
  return (item.element as ReactElement<{ "aria-label": string }>).props["aria-label"];
}

function openRenderedDocument() {
  const menu = harness.renderedItems[0];
  if (menu?.type !== "menu") throw new Error("Expected the native file menu");
  const action = menu.menu.items[0];
  if (action?.type !== "action") throw new Error("Expected the native Open PDF action");
  action.onPress?.();
}

afterEach(() => {
  act(() => root?.unmount());
  root = null;
  harness.appliedOptions = {};
  harness.renderedItems = [];
  harness.setOptionsCalls = 0;
  harness.openedDocuments = [];
  harness.itemProps = undefined;
  vi.unstubAllGlobals();
});

it.each(["left", "right", "bottom"] as const)(
  "updates the %s header action when switching between documents with the same name",
  (placement) => {
    renderDocument("first/guide.pdf", placement);
    openRenderedDocument();
    renderDocument("second/guide.pdf", placement);
    openRenderedDocument();

    expect(harness.openedDocuments).toEqual(["first/guide.pdf", "second/guide.pdf"]);
  },
);

it("applies a custom header element rendered inside a navigation container", () => {
  renderConnecting("Connecting");
  expect(renderedCustomLabel()).toBe("Connecting");

  const applied = harness.setOptionsCalls;
  renderConnecting("Connecting");
  expect(harness.setOptionsCalls).toBe(applied);

  renderConnecting("Reconnecting");
  expect(renderedCustomLabel()).toBe("Reconnecting");
});

it.each(["left", "right", "bottom"] as const)(
  "uses the latest form values from the originally registered %s native button",
  (placement) => {
    vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
    function PairToolbar(props: { readonly host: string; readonly code: string }) {
      return (
        <NativeHeaderToolbar placement={placement}>
          <NativeHeaderToolbar.Button
            label="Add environment"
            onPress={() => harness.openedDocuments.push(`${props.host}|${props.code}`)}
          />
        </NativeHeaderToolbar>
      );
    }
    root = createRoot(document.createElement("div"));
    act(() => root!.render(<PairToolbar host="h" code="" />));
    const original = harness.renderedItems[0];
    if (original?.type !== "button") throw new Error("Expected native pairing button");
    const applied = harness.setOptionsCalls;
    act(() => root!.render(<PairToolbar host="http://127.0.0.1:3773" code="PAIRCODE" />));
    expect(harness.setOptionsCalls).toBe(applied);
    original.onPress?.();
    expect(harness.openedDocuments).toEqual(["http://127.0.0.1:3773|PAIRCODE"]);
  },
);

it("refreshes an originally registered nested menu action without reapplying unchanged options", () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  function Menu(props: { path: string }) {
    return (
      <NativeHeaderToolbar placement="right">
        <NativeHeaderToolbar.Menu title="Actions">
          <NativeHeaderToolbar.Menu title="Document">
            <NativeHeaderToolbar.MenuAction
              onPress={() => harness.openedDocuments.push(props.path)}
            >
              Open
            </NativeHeaderToolbar.MenuAction>
          </NativeHeaderToolbar.Menu>
        </NativeHeaderToolbar.Menu>
      </NativeHeaderToolbar>
    );
  }
  root = createRoot(document.createElement("div"));
  act(() => root!.render(<Menu path="first" />));
  const menu = harness.renderedItems[0];
  if (menu?.type !== "menu") throw new Error("Expected menu");
  const submenu = menu.menu.items[0];
  if (submenu?.type !== "submenu") throw new Error("Expected submenu");
  const original = submenu.items[0];
  if (original?.type !== "action") throw new Error("Expected action");
  const applied = harness.setOptionsCalls;
  act(() => root!.render(<Menu path="latest" />));
  expect(harness.setOptionsCalls).toBe(applied);
  original.onPress?.();
  expect(harness.openedDocuments).toEqual(["latest"]);
});

it("refreshes direct item-factory callbacks using actual native factory arguments", () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  harness.itemProps = { tintColor: "native-blue" };
  function Options(props: { host: string }) {
    return (
      <NativeStackScreenOptions
        options={{
          unstable_headerRightItems: (native: { tintColor: string }) => [
            {
              type: "button",
              label: "Connect",
              onPress: () => harness.openedDocuments.push(`${props.host}|${native.tintColor}`),
            },
          ],
        }}
      />
    );
  }
  root = createRoot(document.createElement("div"));
  act(() => root!.render(<Options host="first" />));
  const original = harness.renderedItems[0];
  if (original?.type !== "button") throw new Error("Expected button");
  const applied = harness.setOptionsCalls;
  act(() => root!.render(<Options host="latest" />));
  expect(harness.setOptionsCalls).toBe(applied);
  expect(harness.openedDocuments).toEqual([]);
  original.onPress?.();
  expect(harness.openedDocuments).toEqual(["latest|native-blue"]);
});

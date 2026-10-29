import "../global.css";

import { registerRootComponent } from "expo";
import { useEffect, useState } from "react";
import { Button, ScrollView, StyleSheet, Text, View } from "react-native";
import { Uniwind } from "uniwind";
import { StreamedTextContent } from "../src/features/threads/StreamedTextContent";

// CI replaces package.json's main only in its disposable checkout. The shipped
// entry point never loads this fixture, and no daemon or provider is simulated.
Uniwind.setTheme("light");

function NativeStreamSmoke() {
  const [mode, setMode] = useState<"body" | "thinking">("body");
  const [count, setCount] = useState(0);
  const [phase, setPhase] = useState<"idle" | "streaming" | "interrupted" | "complete">("idle");
  const token = mode === "body" ? "BODY_TOKEN" : "THINKING_TOKEN";
  const text = Array.from({ length: count }, (_, index) => `${token}_${index + 1}`).join("\n");

  useEffect(() => {
    if (phase !== "streaming" || count >= 4) return;
    const timer = setTimeout(() => setCount((value) => value + 1), 200);
    return () => clearTimeout(timer);
  }, [phase, count]);

  function begin(nextMode: typeof mode) {
    setMode(nextMode);
    setCount(1);
    setPhase("streaming");
  }

  return (
    <ScrollView contentContainerStyle={styles.screen}>
      <Text style={styles.title}>Native stream component smoke</Text>
      <Text>Fixture events, production renderer. No provider connection.</Text>
      <Button title="Begin body stream" onPress={() => begin("body")} />
      <Button title="Begin thinking stream" onPress={() => begin("thinking")} />
      <Button title="Interrupt" onPress={() => setPhase("interrupted")} />
      <Button title="Restore complete" onPress={() => setPhase("complete")} />
      <Text>{`${mode}: ${phase}`}</Text>
      <View style={styles.output}>
        <StreamedTextContent
          text={text}
          streaming={phase === "streaming"}
          textPrefixMissing={phase === "streaming" || phase === "interrupted"}
        >
          <Text testID="complete-text">
            {phase === "complete" ? `PREFIX_RECOVERED\n${text}` : "Idle"}
          </Text>
        </StreamedTextContent>
      </View>
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  screen: { paddingHorizontal: 20, paddingTop: 80, paddingBottom: 40, gap: 12 },
  title: { fontSize: 20, fontWeight: "600" },
  output: { padding: 12, backgroundColor: "#f4f4f5", borderRadius: 8 },
});

registerRootComponent(NativeStreamSmoke);

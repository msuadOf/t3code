var response = http.get(CONTROL_URL + "/visible?stage=" + STAGE);
var result = JSON.parse(response.body);
if (!result.passed) {
  throw new Error("Native UI visibility exceeded 2000ms: " + JSON.stringify(result));
}
output.claudeLatencyMs = result.latencyMs;

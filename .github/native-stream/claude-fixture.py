"""Stock Claude foreground stdout -> real T3 -> native app latency fixture. No accounts."""
import http.server, json, os, pathlib, secrets, shlex, shutil, signal, socket, subprocess, tempfile, threading, time, urllib.request, urllib.parse
from pathlib import Path
root = Path(tempfile.mkdtemp(prefix="t3-native-claude-")).resolve()
work = root / "project"; work.mkdir()
config = root / "config"; config.mkdir()
native_tmp = root / "native-tmp"; native_tmp.mkdir()
stop = threading.Event(); advance = threading.Event(); complete = threading.Event()
processes = []; timing = {}; lock = threading.Lock()
def write_private(path, value):
    path.parent.mkdir(parents=True, exist_ok=True); path.write_text(json.dumps(value, indent=2)); path.chmod(0o600)
def shutdown(*_):
    stop.set(); advance.set(); complete.set(); [(root / ("advance-" + str(stage))).touch() for stage in range(1,4)]; (root / "complete").touch()
class Model(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_): pass
    def do_GET(self):
        url = urllib.parse.urlparse(self.path)
        path = url.path
        stage = urllib.parse.parse_qs(url.query).get("stage", ["1"])[0]
        if stage not in {"1", "2", "3"}:
            self.send_error(400, "Invalid stage"); return
        with lock:
            if path == "/advance": advance.set(); (root / ("advance-" + stage)).touch()
            elif path == "/complete": complete.set(); (root / "complete").touch()
            elif path == "/emitted": timing.setdefault(stage, {})["emittedNs"] = time.monotonic_ns()
            elif path == "/visible":
                sample = timing.setdefault(stage, {})
                if "visibleNs" in sample:
                    self.send_error(409, "Sample already acknowledged"); return
                sample["visibleNs"] = time.monotonic_ns()
                sample["stillBlocked"] = not (root / "complete").exists()
                if "emittedNs" in sample: sample["latencyMs"] = (sample["visibleNs"] - sample["emittedNs"]) / 1e6
                sample["passed"] = sample["stillBlocked"] and 0 <= sample.get("latencyMs", 999999) <= 2000
                write_private(root / "latency.json", timing)
            elif path == "/shutdown": shutdown()
            result = dict(timing.get(stage, {})) if path == "/visible" else dict(timing)
        self.send_response(200); self.send_header("Content-Type", "application/json"); self.end_headers(); self.wfile.write(json.dumps(result).encode())
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or "{}")
        if "count_tokens" in self.path:
            self.send_response(200); self.end_headers(); self.wfile.write(b'{"input_tokens":16}'); return
        has_result = any(isinstance(m.get("content"), list) and any(isinstance(c, dict) and c.get("type") == "tool_result" for c in m["content"]) for m in body.get("messages", []))
        streamed = body.get("stream")
        content = ([{"type":"text", "text":"NATIVE_CLAUDE_DONE"}] if has_result else [{"type":"tool_use", "id":"native_foreground_ci", "name":"Bash", "input":{"command":"python3 -u " + shlex.quote(str(root / "native-progress.py")), "description":"Native foreground progress probe", "timeout":600000}}]) if streamed else [{"type":"text", "text":"Native stream smoke"}]
        reason = "tool_use" if content[0]["type"] == "tool_use" else "end_turn"
        message = {"id":"msg_native_" + secrets.token_hex(10), "type":"message", "role":"assistant", "model":body.get("model"), "content":content, "stop_reason":reason, "usage":{"input_tokens":16,"output_tokens":8}}
        self.send_response(200); self.send_header("Content-Type", "text/event-stream" if streamed else "application/json"); self.end_headers()
        if not streamed: self.wfile.write(json.dumps(message).encode()); return
        def send(kind, **data):
            self.wfile.write(("event: " + kind + "\ndata: " + json.dumps({"type":kind, **data}) + "\n\n").encode()); self.wfile.flush()
        try:
            send("message_start", message={**message, "content":[], "stop_reason":None})
            for index, c in enumerate(content):
                tool = c["type"] == "tool_use"
                send("content_block_start", index=index, content_block={"type":"tool_use", "id":c.get("id"), "name":"Bash", "input":{}} if tool else {"type":"text", "text":""})
                send("content_block_delta", index=index, delta={"type":"input_json_delta", "partial_json":json.dumps(c["input"])} if tool else {"type":"text_delta", "text":c["text"]})
                send("content_block_stop", index=index)
            send("message_delta", delta={"stop_reason":reason}, usage={"output_tokens":8}); send("message_stop")
        except (BrokenPipeError, ConnectionResetError): pass
model = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Model)
threading.Thread(target=model.serve_forever, daemon=True).start()
control_url = f"http://127.0.0.1:{model.server_port}"
(root / "native-progress.py").write_text(
    "import pathlib,time,urllib.request\nroot=pathlib.Path(__file__).parent\n"
    "print('NATIVE_CLAUDE_READY',flush=True)\n"
    "for stage in range(1,4):\n"
    "    while not (root/('advance-'+str(stage))).exists(): time.sleep(.02)\n"
    + "    urllib.request.urlopen(" + repr(control_url + "/emitted?stage=") + "+str(stage)).read()\n"
    "    print('NATIVE_CLAUDE_LATENCY_'+str(stage),flush=True)\n"
    "while not (root/'complete').exists(): time.sleep(.02)\n"
    "print('NATIVE_CLAUDE_FINAL_OUTPUT',flush=True)\n")

env = {k: v for k,v in os.environ.items() if k in ['PATH','LANG','SHELL','USER']}
env.update(HOME=str(root / "home"), CLAUDE_CONFIG_DIR=str(config), CLAUDE_CODE_TMPDIR=str(native_tmp), TMPDIR=str(native_tmp) + "/", TERM="xterm-256color", ANTHROPIC_API_KEY="local-fixture-no-account", ANTHROPIC_BASE_URL=control_url, CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC="1", DISABLE_AUTOUPDATER="1", DISABLE_TELEMETRY="1", DISABLE_ERROR_REPORTING="1", NO_PROXY="127.0.0.1,localhost")
Path(env['HOME']).mkdir()
write_private(config / "settings.json", {"skipDangerousModePermissionPrompt":True, "permissions":{"defaultMode":"bypassPermissions"}, "env":{k:env[k] for k in ['ANTHROPIC_API_KEY','ANTHROPIC_BASE_URL','CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC']}})
write_private(config / ".claude.json", {"hasCompletedOnboarding":True, "theme":"dark", "lastOnboardingVersion":"2.1.314", "customApiKeyResponses":{"approved":[env['ANTHROPIC_API_KEY'][-20:]],"rejected":[]},"projects":{str(work):{"hasTrustDialogAccepted":True}}})
binary = os.environ['CLAUDE_BINARY']
def run(args): return subprocess.run([binary, *args], env=env, cwd=work, capture_output=True, text=True, timeout=30, check=True)
signal.signal(signal.SIGTERM, shutdown); signal.signal(signal.SIGINT, shutdown)
try:
    launch = run(['--bg','--dangerously-skip-permissions','--name','Native stream smoke','Run the native foreground progress probe.'])
    (root / 'native.log').write_text(launch.stdout + launch.stderr)
    jobs = json.loads(run(['agents','--json','--all']).stdout)
    job = next(j for j in jobs if j.get('kind') == 'background'); sid = job['sessionId']
    instance = {"driver":"claudeAgent", "enabled":True, "environment":[{"name":k,"value":v} for k,v in env.items()], "config":{"binaryPath":binary,"configDirectory":str(config),"sharedSessionRoot":str(work)}}
    metadata = {"fixturePid":os.getpid(),"root":str(root),"project":str(work),"sessionId":sid,"version":run(['--version']).stdout.strip(),"controlUrl":control_url}
    repository = Path(__file__).resolve().parents[2]
    t3_home = root / "t3"
    instances = {name: {"driver": name, "enabled": False, "config": {}}
                 for name in ["codex", "claudeAgent", "cursor", "grok", "muse", "antigravity", "pi"]}
    instances["opencode"] = {"driver":"opencode","enabled":False,"config":{}}
    instances["claudeAgent"] = instance
    write_private(t3_home / "userdata/settings.json", {
        "providerInstances": instances, "enableProviderUpdateChecks": False})
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        t3_port = reservation.getsockname()[1]
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    if not node:
        raise RuntimeError("Node is required for the isolated T3 daemon")
    t3_env = dict(os.environ)
    t3_env.update(HOME=env["HOME"], T3CODE_DEV_SEED="0",
                  T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD="0")
    t3_log = (root / "t3.log").open("w")
    command = [node, str(repository / "apps/server/src/bin.ts")]
    processes.append(subprocess.Popen(command + ["serve", "--base-dir", str(t3_home),
        "--host", "127.0.0.1", "--port", str(t3_port)], cwd=repository,
        env=t3_env, stdout=t3_log, stderr=subprocess.STDOUT))
    for attempt in range(300):
        if processes[-1].poll() is not None:
            raise RuntimeError("T3 daemon exited; see " + str(root / "t3.log"))
        try:
            with socket.create_connection(("127.0.0.1", t3_port), timeout=0.2):
                break
        except OSError:
            if stop.wait(0.1):
                raise RuntimeError("Fixture stopped before T3 startup")
    else:
        raise RuntimeError("T3 daemon did not start")
    issued = subprocess.run(command + ["auth", "pairing", "create", "--base-dir",
        str(t3_home), "--ttl", "30 minutes", "--json"], cwd=repository, env=t3_env,
        capture_output=True, text=True, timeout=30, check=True)
    pairing = json.loads(issued.stdout[issued.stdout.index("{"):])
    write_private(root / "pairing.json", {"host": f"http://127.0.0.1:{t3_port}",
                                          "code": pairing["credential"]})
    for attempt in range(120):
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{t3_port}/.well-known/t3/environment", timeout=2
            ) as response:
                json.load(response)
            break
        except (OSError, ValueError):
            if stop.wait(0.1):
                raise RuntimeError("Fixture stopped before environment discovery became ready")
    else:
        raise RuntimeError("T3 environment discovery did not become ready")
    metadata.update(t3Pid=processes[-1].pid, t3Port=t3_port, t3Home=str(t3_home),
                    pairingPath=str(root / "pairing.json"))
    write_private(root / "fixture.json", metadata)
    print(json.dumps(metadata), flush=True)
    while not stop.wait(.25):
        if any(p.poll() is not None for p in processes): raise RuntimeError("T3 exited during native test")
finally:
    shutdown()
    try:
        for job in json.loads(run(['agents','--json','--all']).stdout):
            if job.get('kind') == 'background': run(['stop',job['id']])
        run(['daemon','stop','--any'])
    except Exception as error: (root / 'cleanup-error.txt').write_text(str(error))
    for process in reversed(processes):
        if process.poll() is None:
            process.terminate()
            try: process.wait(timeout=5)
            except subprocess.TimeoutExpired: process.kill(); process.wait()
    write_private(root / 'final-native-state.json', {'timing':timing,'accountUsed':False,'commandSchedulingChanged':False})
    model.shutdown(); model.server_close()

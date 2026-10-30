"""Account-free stock OpenCode fixture for native client end-to-end checks."""
import base64
import http.server
import json
import os
from pathlib import Path
import pty
import secrets
import shlex
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time
import urllib.request

root = Path(tempfile.mkdtemp(prefix="t3-native-opencode-")).resolve()
work = root / "project"
work.mkdir()
stop = threading.Event()
advance = threading.Event()
complete = threading.Event()
processes = []
markers = {"prefix": False, "suffix": False, "completed": False}
stream_kind = os.environ.get("NATIVE_STREAM_KIND", "text")
if stream_kind not in {"text", "reasoning", "tool"}:
    raise ValueError("Unsupported native stream kind")
if stream_kind == "tool":
    (root / "native-progress.py").write_text(
        "import pathlib, time\n"
        "root = pathlib.Path(__file__).parent\n"
        "for index, gate in [(1, 'advance'), (2, 'complete')]:\n"
        "    print(f'NATIVE_TOOL_{index:02d}', flush=True)\n"
        "    (root / f'tool-{index}').touch()\n"
        "    while not (root / gate).exists(): time.sleep(.05)\n"
        "print('NATIVE_TOOL_03', flush=True)\n"
    )


def write_private(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2))
    path.chmod(0o600)


class Model(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def do_GET(self):
        if self.path == "/shutdown":
            shutdown(None, None)
        elif self.path == "/advance":
            advance.set()
            (root / "advance").touch()
        elif self.path == "/complete":
            complete.set()
            (root / "complete").touch()
        if stream_kind == "tool":
            markers["prefix"] = (root / "tool-1").exists()
            markers["suffix"] = (root / "tool-2").exists()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(markers).encode())

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        with (root / "model-events.jsonl").open("a") as log:
            log.write(json.dumps({"roles": [message.get("role") for message in body.get("messages", [])],
                "tools": [tool.get("function", {}).get("name") for tool in body.get("tools", [])]}) + "\n")
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()

        def emit(delta, finish=None):
            data = {"id": "native-smoke", "object": "chat.completion.chunk", "created": 1,
                    "model": "smoke", "choices": [{"index": 0, "delta": delta,
                    "finish_reason": finish}]}
            self.wfile.write(("data: " + json.dumps(data) + "\n\n").encode())
            self.wfile.flush()

        def gate(event):
            while not event.wait(0.25) and not stop.is_set():
                self.wfile.write(b": heartbeat\n\n")
                self.wfile.flush()

        try:
            emit({"role": "assistant"})
            tool_result = any(message.get("role") == "tool"
                              for message in body.get("messages", []))
            if not body.get("tools"):
                emit({"content": "Native stream smoke"})
            elif stream_kind == "tool" and tool_result:
                emit({"content": "NATIVE_TOOL_DONE"})
            elif stream_kind == "tool":
                shell = next(tool["function"] for tool in body["tools"]
                             if tool.get("function", {}).get("name") == "shell")
                args = {"command": "python3 -u " + shlex.quote(str(root / "native-progress.py")),
                        "timeout": 0}
                if "workdir" in shell.get("parameters", {}).get("required", []):
                    args["workdir"] = str(work)
                emit({"tool_calls": [{"index": 0, "id": "call_native_progress",
                    "type": "function", "function": {"name": "shell",
                    "arguments": json.dumps(args)}}]})
                emit({}, "tool_calls")
                self.wfile.write(b"data: [DONE]\n\n")
                self.wfile.flush()
                return
            else:
                for index in range(1, 25):
                    if stop.is_set():
                        return
                    field = "reasoning_content" if stream_kind == "reasoning" else "content"
                    emit({field: f"NATIVE_TOKEN_{index:02d}\n"})
                    if index == 5:
                        markers["prefix"] = True
                        gate(advance)
                    time.sleep(0.15)
                markers["suffix"] = True
                gate(complete)
            emit({}, "stop")
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
            if body.get("tools"):
                markers["completed"] = True
        except (BrokenPipeError, ConnectionResetError):
            pass


model = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Model)
threading.Thread(target=model.serve_forever, daemon=True).start()
env = {key: os.environ[key] for key in ("PATH", "TMPDIR", "LANG") if key in os.environ}
env.update(HOME=str(root / "home"), XDG_CONFIG_HOME=str(root / "config"),
           XDG_STATE_HOME=str(root / "state"), XDG_DATA_HOME=str(root / "data"),
           XDG_CACHE_HOME=str(root / "cache"), NO_PROXY="127.0.0.1,localhost",
           TERM="xterm-256color", OPENCODE_PASSWORD=secrets.token_hex(24))
write_private(root / "config/opencode/opencode.json", {
    "model": "localfake/smoke", "update": "disable", "share": "disabled", "snapshots": False,
    "providers": {"localfake": {"name": "Local CI model",
        "package": "@opencode/ai/providers/openai-compatible",
        "settings": {"baseURL": f"http://127.0.0.1:{model.server_port}/v1",
                     "apiKey": "local-fixture-only", "provider": "localfake"},
        "models": {"smoke": {"capabilities": {"tools": True, "input": ["text"], "output": ["text"]},
                             "limit": {"context": 32000, "output": 4096}}}}}})
with socket.socket() as reservation:
    reservation.bind(("127.0.0.1", 0))
    port = reservation.getsockname()[1]
url = f"http://127.0.0.1:{port}"
auth = base64.b64encode(("opencode:" + env["OPENCODE_PASSWORD"]).encode()).decode()


def api(path, body=None):
    request = urllib.request.Request(url + path,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Authorization": "Basic " + auth, "Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=10) as response:
        data = response.read()
        return json.loads(data) if data else {}


def shutdown(_signal, _frame):
    stop.set()
    advance.set()
    complete.set()
    (root / "advance").touch()
    (root / "complete").touch()


signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)
master = None
try:
    binary = os.environ["OPENCODE_BINARY"]
    native_log = (root / "native.log").open("w")
    processes.append(subprocess.Popen([binary, "serve", "--hostname", "127.0.0.1", "--port", str(port)],
                                     cwd=work, env=env, stdout=native_log, stderr=subprocess.STDOUT))
    for attempt in range(120):
        try:
            version = api("/api/info")["version"]
            break
        except (OSError, ValueError):
            if stop.wait(0.1):
                raise RuntimeError("Fixture stopped before OpenCode startup")
    else:
        raise RuntimeError("OpenCode did not start")
    session = api("/api/session", {"location": {"directory": str(work)},
        "title": "Native stream smoke", "model": {"providerID": "localfake", "id": "smoke"},
        "permissions": [{"action": "*", "resource": "*", "effect": "allow"}]})
    sid = session.get("data", session)["id"]
    master, slave = pty.openpty()
    processes.append(subprocess.Popen([binary, "--server", url, "--session", sid,
                                      "--prompt", "Stream the native CI markers.", str(work)],
                                     cwd=work, env=env, stdin=slave, stdout=slave, stderr=slave))
    os.close(slave)

    def drain_terminal():
        try:
            with (root / "cli-terminal.bin").open("wb") as output:
                while not stop.is_set():
                    data = os.read(master, 65536)
                    if not data:
                        break
                    output.write(data)
        except OSError:
            pass

    threading.Thread(target=drain_terminal, daemon=True).start()
    instance = {"driver": "opencode", "enabled": True,
        "environment": [{"name": key, "value": value} for key, value in env.items()],
        "config": {"enabled": True, "binaryPath": binary, "serverUrl": url,
                   "serverPassword": env["OPENCODE_PASSWORD"], "sharedSessionRoot": str(work)}}
    write_private(root / "provider-settings.json", {"providerInstances": {"opencode": instance}})
    metadata = {"fixturePid": os.getpid(), "root": str(root), "project": str(work), "sessionId": sid,
                "version": version, "streamKind": stream_kind,
                "controlUrl": f"http://127.0.0.1:{model.server_port}",
                "settingsPath": str(root / "provider-settings.json"),
                "nativePid": processes[0].pid, "cliPid": processes[1].pid}
    repository = Path(__file__).resolve().parents[2]
    t3_home = root / "t3"
    instances = {name: {"driver": name, "enabled": False, "config": {}}
                 for name in ["codex", "claudeAgent", "cursor", "grok", "muse", "antigravity", "pi"]}
    instances["opencode"] = instance
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
    while not stop.wait(0.25):
        if any(process.poll() is not None for process in processes):
            raise RuntimeError("Native server or CLI exited during verification")
finally:
    shutdown(None, None)
    if "sid" in locals() and processes and processes[0].poll() is None:
        try:
            write_private(root / "final-native-state.json", {
                "messages": api("/api/session/" + sid + "/message?order=desc&limit=20"),
                "active": api("/api/session/active"), "markers": markers})
        except (OSError, ValueError) as error:
            write_private(root / "final-native-state.json", {"error": str(error)})
    for process in reversed(processes):
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
    if master is not None:
        os.close(master)
    model.shutdown()
    model.server_close()
    write_private(root / "cleanup.json", {"processes": [
        {"pid": process.pid, "exited": process.poll() is not None} for process in processes]})

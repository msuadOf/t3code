"""Strict, opt-in reuse of an unchanged iOS native shell with current JS assets."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import plistlib
import re
import shutil
import subprocess

INPUTS = ["apps/mobile", "packages", "scripts", "assets", "pnpm-lock.yaml", "package.json",
          "pnpm-workspace.yaml", "third-party-licenses.config.json"]
ALLOWED = {
    "apps/mobile/src/features/threads/thread-feed-live-follow.ts",
    "apps/mobile/src/features/threads/thread-feed-live-follow.test.ts",
    "apps/mobile/src/features/threads/thread-work-log.tsx",
    "apps/mobile/src/features/threads/thread-command-output.tsx",
}


def run(args, *, cwd=None):
    return subprocess.check_output(args, cwd=cwd, timeout=600).decode().strip()


def source_digest(revision, variant, updates, runner=run):
    # Match the existing cache key byte for byte, including the trailing newline.
    hashes = runner(["git", "rev-parse", *[revision + ":" + p for p in INPUTS]])
    return hashlib.sha256((hashes + "\n" + variant + ":" + updates + "\n").encode()).hexdigest()


def compatibility(base, current, runner=run):
    if not re.fullmatch(r"[0-9a-fA-F]{40}", base):
        raise ValueError("iosShellRevision must be a full 40-character commit SHA")
    changed = runner(["git", "diff", "--no-renames", "--name-only", "-z", base, current, "--", *INPUTS])
    paths = [p for p in changed.split("\0") if p]
    if any(p not in ALLOWED for p in paths):
        raise ValueError("Native inputs changed outside the explicit JS-only allowlist: " + repr(paths))
    for revision in (base, current):
        for path in paths:
            entry = runner(["git", "ls-tree", revision, "--", path])
            if not entry.startswith("100644 blob "):
                raise ValueError("Only existing regular JS source files may change: " + path)
    return paths


def output(name, value):
    with open(os.environ["GITHUB_OUTPUT"], "a") as file:
        file.write(name + "=" + value + "\n")


def save(path, report):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(report, indent=2) + "\n")


def prepare(args):
    report = {"requestedRevision": args.revision, "currentRevision": run(["git", "rev-parse", "HEAD"]),
              "eligible": False}
    try:
        if not re.fullmatch(r"[0-9a-fA-F]{40}", args.revision):
            raise ValueError("iosShellRevision must be a full 40-character commit SHA")
        try:
            run(["git", "cat-file", "-e", args.revision + "^{commit}"])
        except subprocess.CalledProcessError:
            run(["git", "fetch", "--no-tags", "--depth=1", "origin", args.revision])
        report["changedNativeInputs"] = compatibility(args.revision, report["currentRevision"])
        report["baseDigest"] = source_digest(args.revision, os.environ["APP_VARIANT"],
                                              os.environ["T3CODE_MOBILE_UPDATES_ENABLED"])
        patch = Path(args.report).with_suffix(".patch")
        patch.write_text(run(["git", "diff", "--no-ext-diff", args.revision, report["currentRevision"], "--", *INPUTS]) + "\n")
        report["diffArtifact"] = str(patch)
        report["eligible"] = True
        output("digest", report["baseDigest"])
        output("eligible", "true")
    except (ValueError, subprocess.SubprocessError) as error:
        report["reason"] = str(error)
        output("eligible", "false")
        print("Native shell reuse declined; building current sources:", error)
    save(args.report, report)


def rebundle(args):
    app = Path(args.app).resolve()
    report = json.loads(Path(args.report).read_text())
    report["reused"] = False
    try:
        # Legacy shell cache contains no separate toolchain manifest. Check its
        # actual build metadata before allowing it to bypass native compilation.
        info = plistlib.loads((app / "Info.plist").read_bytes())
        xcode = run(["xcodebuild", "-version"])
        sdk = run(["xcrun", "--sdk", "iphonesimulator", "--show-sdk-version"])
        sdk_build = run(["xcrun", "--sdk", "iphonesimulator", "--show-sdk-build-version"])
        report.update(xcode=xcode, simulatorSdk=sdk, simulatorSdkBuild=sdk_build,
                      shellXcodeBuild=info.get("DTXcodeBuild"), shellSdkBuild=info.get("DTSDKBuild"))
        xcode_build = xcode.split("Build version ")[-1].strip()
        if info.get("DTXcodeBuild") != xcode_build or info.get("DTSDKBuild") != sdk_build:
            raise ValueError("Cached shell toolchain differs from current Xcode / simulator SDK")
        executable = app / info["CFBundleExecutable"]
        architectures = run(["lipo", "-archs", str(executable)]).split()
        report["shellArchitectures"] = architectures
        report["shellExecutableSha256"] = hashlib.sha256(executable.read_bytes()).hexdigest()
        if "iPhoneSimulator" not in info.get("CFBundleSupportedPlatforms", []) or architectures != ["arm64"]:
            raise ValueError("Cached shell is not an ARM64 iOS simulator app")
        if os.environ["T3CODE_MOBILE_UPDATES_ENABLED"] != "0":
            raise ValueError("Shell reuse requires native updates disabled")
    except (ValueError, KeyError, OSError, subprocess.SubprocessError) as error:
        report["reason"] = str(error)
        save(args.report, report)
        output("reused", "false")
        # A full rebuild must not inherit resources from an incompatible shell.
        if app.exists():
            shutil.rmtree(app)
        print("Native shell reuse declined; building current sources:", error)
        return
    entitlements = Path(args.report).with_suffix(".entitlements.plist")
    with entitlements.open("wb") as file:
        subprocess.run(["codesign", "--display", "--entitlements", ":-", str(app)],
                       stdout=file, check=True, timeout=60)
    plistlib.loads(entitlements.read_bytes())
    bundle = app / "main.jsbundle"
    report["previousBundleSha256"] = hashlib.sha256(bundle.read_bytes()).hexdigest()
    bundle.unlink()
    subprocess.run(["pnpm", "exec", "expo", "export:embed", "--platform", "ios", "--entry-file", "index.ts",
                    "--dev", "false", "--bytecode", "--bundle-output", str(bundle), "--assets-dest", str(app)],
                   cwd="apps/mobile", check=True, timeout=600)
    if bundle.read_bytes()[:8] != bytes.fromhex("c61fbc03c103191f"):
        raise ValueError("Current export did not produce Hermes bytecode")
    subprocess.run(["codesign", "--force", "--sign", "-", "--entitlements", str(entitlements), str(app)],
                   check=True, timeout=60)
    subprocess.run(["codesign", "--verify", "--deep", "--strict", "--verbose=2", str(app)],
                   check=True, timeout=60)
    report.update(reused=True, bundleSha256=hashlib.sha256(bundle.read_bytes()).hexdigest(),
                  currentDigest=source_digest("HEAD", os.environ["APP_VARIANT"],
                                              os.environ["T3CODE_MOBILE_UPDATES_ENABLED"]))
    save(args.report, report)
    output("reused", "true")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="operation", required=True)
    pre = sub.add_parser("prepare")
    pre.add_argument("--revision", required=True)
    pre.add_argument("--report", required=True)
    reembed = sub.add_parser("rebundle")
    reembed.add_argument("--app", required=True)
    reembed.add_argument("--report", required=True)
    args = parser.parse_args()
    (prepare if args.operation == "prepare" else rebundle)(args)

"""Operations entry point: packaging, user-level install, resident run and clean.

Usable both from the source tree (``python3 ops.py ...``) and from an installed
package (``python3 /path/app.pyz ...``); all paths are passed explicitly, so the
current working directory and the development directory are irrelevant.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import plistlib
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import zipfile

from localio import Gate, LocalBlobs, Protector, clean as clean_blobs, worker
from pipeline import Engine, connect

RUNTIME_MODULES = ("pipeline.py", "localio.py", "ops.py")


# --------------------------------------------------------------------------
# packaging
# --------------------------------------------------------------------------

def _sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _running_from_package():
    candidate = Path(sys.argv[0]).resolve()
    try:
        return candidate.is_file() and zipfile.is_zipfile(candidate)
    except OSError:
        return False


def _write_package(source_dir, target):
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".app-", suffix=".pyz.new",
                                dir=str(target.parent))
    os.close(fd)
    try:
        with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(
                "__main__.py",
                "import ops\n"
                "ops.main()\n")
            for name in RUNTIME_MODULES:
                archive.write(str(source_dir / name), arcname=name)
        os.replace(tmp, str(target))
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def cmd_build(out):
    out = Path(out).resolve()
    out.mkdir(parents=True, exist_ok=True)
    app = out / "app.pyz"
    if _running_from_package():
        # Re-packaging from an installed entry copies the exact running code,
        # so the package never depends on the (possibly removed) source tree.
        shutil.copyfile(sys.argv[0], app)
    else:
        source_dir = Path(__file__).resolve().parent
        _write_package(source_dir, app)
    manifest = {"sha256": _sha256_file(app)}
    fd, tmp = tempfile.mkstemp(prefix=".manifest-", dir=str(out))
    os.close(fd)
    Path(tmp).write_text(json.dumps(manifest, ensure_ascii=True,
                                    sort_keys=True), encoding="utf-8")
    os.replace(tmp, str(out / "manifest.json"))
    return {"package": str(out)}


# --------------------------------------------------------------------------
# install / LaunchAgent
# --------------------------------------------------------------------------

def _verify_package(package):
    package = Path(package)
    app = package / "app.pyz"
    manifest_path = package / "manifest.json"
    if not app.is_file() or not manifest_path.is_file():
        raise ValueError("PACKAGE_SHAPE")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        raise ValueError("PACKAGE_MANIFEST")
    if set(manifest) != {"sha256"} or not isinstance(manifest["sha256"], str):
        raise ValueError("PACKAGE_MANIFEST")
    if _sha256_file(app) != manifest["sha256"]:
        raise ValueError("PACKAGE_HASH")
    if not zipfile.is_zipfile(app):
        raise ValueError("PACKAGE_NOT_ZIP")
    with zipfile.ZipFile(app) as archive:
        if archive.testzip() is not None:
            raise ValueError("PACKAGE_CORRUPT")
        names = set(archive.namelist())
        required = set(RUNTIME_MODULES) | {"__main__.py"}
        if not required <= names:
            raise ValueError("PACKAGE_CONTENTS")
    return app, manifest


def _label_for(dest):
    digest = hashlib.sha1(str(dest).encode("utf-8")).hexdigest()[:16]
    return "com.lake.segmented." + digest


def _plist_payload(label, python, app, root, dest):
    return {
        "Label": label,
        "ProgramArguments": [python, str(app), "run", str(root)],
        "RunAtLoad": True,
        "KeepAlive": True,
        "StandardOutPath": str(dest / "launchd.out.log"),
        "StandardErrorPath": str(dest / "launchd.err.log"),
    }


def _atomic_write_bytes(target, data):
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".plist-", dir=str(target.parent))
    try:
        with os.fdopen(fd, "wb") as out:
            out.write(data)
            out.flush()
            os.fsync(out.fileno())
        os.replace(tmp, str(target))
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def _load_agent(agent_plist, label):
    uid = os.getuid()
    target = "gui/%s/%s" % (uid, label)
    bootstrap = ["launchctl", "bootstrap", "gui/%s" % uid, str(agent_plist)]
    proc = subprocess.run(bootstrap, text=True, capture_output=True)
    if proc.returncode == 0:
        return
    # A stale registration for the same label: replace it, then bootstrap again.
    subprocess.run(["launchctl", "bootout", target], text=True,
                   capture_output=True)
    proc = subprocess.run(bootstrap, text=True, capture_output=True)
    if proc.returncode != 0:
        raise ValueError("LAUNCHCTL_BOOTSTRAP: " + proc.stderr.strip())


def cmd_install(package, dest, python, no_load):
    package = Path(package)
    dest = Path(dest).resolve()
    python = str(Path(python).resolve())
    if not Path(python).is_file():
        raise ValueError("PYTHON_NOT_FOUND")
    # Everything is validated before any existing file is touched.
    app_candidate, _manifest = _verify_package(package)
    with tempfile.TemporaryDirectory(prefix="lake-smoke-") as smoke:
        probe = subprocess.run(
            [python, str(app_candidate), "status", smoke],
            text=True, capture_output=True, timeout=30)
        if probe.returncode != 0:
            raise ValueError("PACKAGE_SMOKE_FAILED: " + probe.stderr.strip())

    dest.mkdir(parents=True, exist_ok=True)
    root = dest / "data"
    root.mkdir(parents=True, exist_ok=True)
    app = dest / "app.pyz"
    fd, tmp = tempfile.mkstemp(prefix=".app-install-", dir=str(dest))
    os.close(fd)
    try:
        shutil.copyfile(app_candidate, tmp)
        if _sha256_file(tmp) != _sha256_file(app_candidate):
            raise ValueError("PACKAGE_COPY")
        os.replace(tmp, app)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)

    label = _label_for(dest)
    payload = _plist_payload(label, python, app, root, dest)
    plist_data = plistlib.dumps(payload, sort_keys=True)
    local_plist = dest / (label + ".plist")
    _atomic_write_bytes(local_plist, plist_data)

    if not no_load:
        agents = Path.home() / "Library" / "LaunchAgents"
        agent_plist = agents / (label + ".plist")
        _atomic_write_bytes(agent_plist, plist_data)
        _load_agent(agent_plist, label)
        plist_returned = agent_plist
    else:
        plist_returned = local_plist

    return {"app": str(app), "root": str(root),
            "plist": str(plist_returned), "label": label}


# --------------------------------------------------------------------------
# runtime commands
# --------------------------------------------------------------------------

def _open_engine(root, hook):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    db = connect(root / "state.sqlite")
    protector = Protector(root)
    blobs = LocalBlobs(root, hook, protector)
    return db, Engine(db, blobs, hook, protector)


def cmd_submit(root, input_path):
    if not input_path:
        raise ValueError("SUBMIT_NEEDS_INPUT")
    with open(input_path, encoding="utf-8") as stream:
        batch = json.load(stream)
    hook = Gate()
    db, engine = _open_engine(root, hook)
    try:
        return engine.accept(batch)
    finally:
        db.close()


def cmd_read(root):
    hook = Gate()
    db, engine = _open_engine(root, hook)
    try:
        return engine.read()
    finally:
        db.close()


def cmd_status(root):
    hook = Gate()
    db, engine = _open_engine(root, hook)
    try:
        result = engine.status()
        result["inventory"] = engine.blobs.inventory()
        return result
    finally:
        db.close()


def cmd_clean(root):
    hook = Gate()
    return clean_blobs(root, hook)


def cmd_run(root, once):
    hook = Gate()
    root = Path(root)
    stop = {"flag": False}

    def _terminate(signum, frame):
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _terminate)
    signal.signal(signal.SIGINT, _terminate)
    try:
        with worker(root):
            db, engine = _open_engine(root, hook)
            try:
                if once:
                    return {"progressed": bool(engine.step())}
                while not stop["flag"]:
                    if not engine.step():
                        time.sleep(0.5)
            finally:
                db.close()
    except ValueError as exc:
        if str(exc) == "WORKER_BUSY":
            raise ValueError("WORKER_BUSY")
        raise
    return {"stopped": True}


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def main(argv=None):
    parser = argparse.ArgumentParser(prog="ops")
    sub = parser.add_subparsers(dest="command", required=True)

    p_build = sub.add_parser("build")
    p_build.add_argument("out")

    p_install = sub.add_parser("install")
    p_install.add_argument("package")
    p_install.add_argument("dest")
    p_install.add_argument("--python", required=True)
    p_install.add_argument("--no-load", action="store_true")

    p_submit = sub.add_parser("submit")
    p_submit.add_argument("root")
    p_submit.add_argument("input", nargs="?")

    p_read = sub.add_parser("read")
    p_read.add_argument("root")

    p_status = sub.add_parser("status")
    p_status.add_argument("root")

    p_clean = sub.add_parser("clean")
    p_clean.add_argument("root")

    p_run = sub.add_parser("run")
    p_run.add_argument("root")
    p_run.add_argument("--once", action="store_true")

    args = parser.parse_args(argv)
    try:
        if args.command == "build":
            result = cmd_build(args.out)
        elif args.command == "install":
            result = cmd_install(args.package, args.dest, args.python,
                                 args.no_load)
        elif args.command == "submit":
            result = cmd_submit(args.root, args.input)
        elif args.command == "read":
            result = cmd_read(args.root)
        elif args.command == "status":
            result = cmd_status(args.root)
        elif args.command == "clean":
            result = cmd_clean(args.root)
        else:
            result = cmd_run(args.root, args.once)
    except (ValueError, OSError, json.JSONDecodeError, sqlite3.Error) as exc:
        print("ERROR: %s" % exc, file=sys.stderr)
        raise SystemExit(1)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()

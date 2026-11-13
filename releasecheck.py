"""Black-box release smoke test, used after implementing ops.py."""
import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time

from agentcheck import inspect
from checks import EXPECTED, fixtures
from pipeline import digest


def verify(demo=False):
    base = Path(__file__).resolve().parent
    python = str(Path(sys.executable).resolve())
    env = dict(os.environ)
    for key in ("PYTHONPATH", "PYTHONHOME", "POINTWISE_PAUSE", "POINTWISE_GATE"):
        env.pop(key, None)
    env["PYTHONNOUSERSITE"] = "1"
    children = []
    with tempfile.TemporaryDirectory(prefix="湖表 release ") as directory:
        area = Path(directory).resolve()
        source, package, dest = area / "source", area / "package", area / "安装目录"
        source.mkdir()
        for path in base.glob("*.py"):
            shutil.copy2(path, source / path.name)
        shutil.copytree(base / "samples", source / "samples")
        work = area / "empty"
        work.mkdir()

        def call(argv, success=True):
            proc = subprocess.run(argv, cwd=str(work), env=env, capture_output=True,
                                  text=True, timeout=30)
            if success:
                assert proc.returncode == 0, (argv, proc.stdout, proc.stderr)
                return json.loads(proc.stdout)
            assert proc.returncode != 0, (argv, proc.stdout)
            return proc

        call([python, str(source / "ops.py"), "build", str(package)])
        info = call([python, str(source / "ops.py"), "install", str(package),
                     str(dest), "--python", python, "--no-load"])
        app, root = Path(info["app"]), Path(info["root"])
        assert app == dest / "app.pyz" and root == dest / "data"
        assert inspect(info["plist"], app, root, python) == info["label"]
        shutil.rmtree(source)

        def command(*args):
            return [python, str(app)] + [str(x) for x in args]

        def run(*args):
            return call(command(*args))

        def submit(batch):
            path = area / (batch["batch"] + ".json")
            path.write_text(json.dumps(batch), encoding="utf-8")
            return run("submit", root, path)

        def status():
            return run("status", root)

        def finish():
            for _ in range(128):
                if not status()["pending"]:
                    return
                run("run", root, "--once")
            raise AssertionError("fixture did not complete")

        def paused(event, *args):
            gate = area / ("gate-" + str(len(children)))
            gate.mkdir()
            child_env = dict(env, POINTWISE_PAUSE=event, POINTWISE_GATE=str(gate))
            child = subprocess.Popen(command(*args), cwd=str(work), env=child_env,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            children.append(child)
            until = time.monotonic() + 30
            while not (gate / "ready.json").exists():
                assert child.poll() is None, child.communicate()
                if time.monotonic() > until:
                    raise AssertionError("pause point not reached: " + event)
                time.sleep(0.02)
            ready = json.loads((gate / "ready.json").read_text(encoding="utf-8"))
            return child, gate, ready

        try:
            # A checksum failure must leave the installed program byte-identical.
            before = app.read_bytes()
            broken = area / "broken"
            shutil.copytree(package, broken)
            with (broken / "app.pyz").open("ab") as out:
                out.write(b"corrupted")
            call(command("install", broken, dest, "--python", python, "--no-load"), success=False)
            assert app.read_bytes() == before

            a, b = fixtures()
            ack = submit(a)
            submit(b)
            shared = digest(a["segments"][0])
            assert shared == digest(b["segments"][0])
            run("clean", root)
            assert (root / "blobs" / (shared + ".json")).is_file()

            child, gate, ready = paused("object_ready", "run", root, "--once")
            run("clean", root)  # Must finish while the processing worker is paused.
            assert (root / "blobs" / (ready["detail"]["oid"] + ".json")).is_file()
            (gate / "continue").touch()
            out, err = child.communicate(timeout=30)
            assert child.returncode == 0, (out, err)
            while not any(x["batch"] == "a" for x in run("read", root)):
                run("run", root, "--once")
            run("clean", root)
            assert (root / "blobs" / (shared + ".json")).is_file()
            finish()
            result = run("read", root)
            assert {x["batch"]: x["rows"] for x in result} == EXPECTED
            assert submit(a) == ack
            first_clean = run("clean", root)
            assert first_clean["released_bytes"] > 0
            assert not status()["inventory"]
            assert run("clean", root) == {"removed_files": [], "released_bytes": 0}

            c = copy.deepcopy(a)
            c["batch"] = "c"
            c_ack = submit(c)
            child, gate, ready = paused("after_checkpoint", "run", root, "--once")
            child.kill()
            child.communicate(timeout=30)
            run("clean", root)
            finish()
            assert submit(c) == c_ack
            after_crash = run("read", root)
            assert next(x for x in after_crash if x["batch"] == "c")["rows"] == EXPECTED["a"]

            # Finished C left real unreachable payloads; stop the cleaner before unlink.
            child, gate, ready = paused("before_delete", "clean", root)
            child.kill()
            child.communicate(timeout=30)
            run("clean", root)
            assert not status()["inventory"]
            assert run("read", root) == after_crash
            evidence = {"installed_app": str(app), "results": after_crash,
                        "first_clean": first_clean, "final_status": status()}
            if demo:
                print(json.dumps(evidence, ensure_ascii=False, sort_keys=True))
            else:
                print("INSTALLED_RELEASE_REGRESSION_OK")
        finally:
            for child in children:
                if child.poll() is None:
                    child.kill()
                child.communicate(timeout=30)


if __name__ == "__main__":
    verify(demo=len(sys.argv) > 1 and sys.argv[1] == "demo")

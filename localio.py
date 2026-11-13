"""Local immutable payloads, shared-reference protection and the online cleaner.

Protection model
----------------
A *holder* is a short-lived registration (one per accept/step transition or one
per payload publication) that lists every blob oid and every temporary write the
holder is currently reading or publishing.  Holder liveness is proven by an
exclusive ``flock``: a dead process releases its lock automatically, so stale
protection can never block reclamation permanently.

The cleaner never takes the long-lived worker lock and never holds a global
coordination lock across payload reads.  It computes a protection snapshot from
(1) every committed checkpoint's dependencies and (2) every live holder, then
re-validates each individual file immediately before unlinking it (after the
``before_delete`` gate returns), so a same-named payload re-published while the
cleaner was paused survives.
"""
import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import tempfile
import uuid

from pipeline import connect, dependencies, encoded


def sync_dir(path):
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _atomic_json(path, value):
    path = Path(path)
    fd, name = tempfile.mkstemp(prefix=".state-", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as out:
            json.dump(value, out, ensure_ascii=False, sort_keys=True)
            out.flush()
            os.fsync(out.fileno())
        os.replace(name, str(path))
    finally:
        if os.path.exists(name):
            os.unlink(name)


class Gate:
    def __init__(self):
        self.fired = False

    def __call__(self, event, detail):
        if self.fired or os.environ.get("POINTWISE_PAUSE") != event:
            return
        self.fired = True
        gate = Path(os.environ["POINTWISE_GATE"])
        gate.mkdir(parents=True, exist_ok=True)
        (gate / "ready.json").write_text(
            json.dumps({"event": event, "pid": os.getpid(), "detail": detail}),
            encoding="utf-8")
        while not (gate / "continue").exists():
            import time
            time.sleep(0.02)


class Hold:
    """One live protection registration; closed automatically on context exit."""

    def __init__(self, protector, oids=()):
        self.protector = protector
        self.token = uuid.uuid4().hex
        self.oids = {oid for oid in oids if oid}
        self.temps = set()
        self.dir = protector.directory
        self.dir.mkdir(parents=True, exist_ok=True)
        self.lock_path = self.dir / (self.token + ".lock")
        self.state_path = self.dir / (self.token + ".json")
        self.lock = self.lock_path.open("a+b")
        # Blocking lock is fine: each token owns a distinct file.
        fcntl.flock(self.lock.fileno(), fcntl.LOCK_EX)
        self._publish()

    def _publish(self):
        _atomic_json(self.state_path, {
            "token": self.token, "pid": os.getpid(),
            "oids": sorted(self.oids), "temps": sorted(self.temps)})

    def add(self, oid):
        if oid:
            self.oids.add(oid)
            self._publish()
        return self

    def add_temp(self, name):
        self.temps.add(name)
        self._publish()
        return self

    def close(self):
        if self.lock is None:
            return
        try:
            fcntl.flock(self.lock.fileno(), fcntl.LOCK_UN)
        finally:
            self.lock.close()
            self.lock = None
            for path in (self.state_path, self.lock_path):
                try:
                    path.unlink()
                except FileNotFoundError:
                    pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


class Protector:
    """Process-local factory for holder registrations under ``ROOT/holders``."""

    def __init__(self, root):
        self.root = Path(root)
        self.directory = self.root / "holders"

    def hold(self, oids=()):
        return Hold(self, oids)


def _scan_holders(directory):
    """Return state dicts of live holders; remove registrations of dead ones."""
    live = []
    if not directory.is_dir():
        return live
    for lock_path in sorted(directory.glob("*.lock")):
        try:
            handle = lock_path.open("a+b")
        except FileNotFoundError:
            continue
        try:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                state_path = lock_path.with_suffix(".json")
                try:
                    state = json.loads(state_path.read_text(encoding="utf-8"))
                except (FileNotFoundError, json.JSONDecodeError):
                    state = {"oids": [], "temps": []}
                live.append(state)
                continue
            # Lock acquired: the owning process is gone.  Reap the dead
            # registration so its protection cannot outlive the process.
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()
        state_path = lock_path.with_suffix(".json")
        for path in (state_path, lock_path):
            try:
                path.unlink()
            except FileNotFoundError:
                pass
    return live


class LocalBlobs:
    def __init__(self, root, hook=None, protector=None):
        self.directory = Path(root) / "blobs"
        self.directory.mkdir(parents=True, exist_ok=True)
        self.hook = hook or (lambda event, detail: None)
        # When no shared protector is supplied (legacy callers), publications
        # still register a holder so in-flight writes cannot be reaped.
        self.protector = protector or Protector(root)

    def put(self, value):
        data = encoded(value)
        oid = hashlib.sha256(data).hexdigest()
        with self.protector.hold() as held:
            fd, name = tempfile.mkstemp(
                prefix=".writing-%s-" % held.token, dir=str(self.directory))
            temp_name = Path(name).name
            held.add(oid).add_temp(temp_name)
            try:
                with os.fdopen(fd, "wb") as out:
                    out.write(data)
                    out.flush()
                    os.fsync(out.fileno())
                self.hook("before_object_publish",
                          {"oid": oid, "temporary": name})
                os.replace(name, str(self.directory / (oid + ".json")))
                sync_dir(self.directory)
            finally:
                if os.path.exists(name):
                    os.unlink(name)
        return oid

    def get(self, oid):
        data = (self.directory / (oid + ".json")).read_bytes()
        if hashlib.sha256(data).hexdigest() != oid:
            raise ValueError("PAYLOAD_HASH")
        return json.loads(data)

    def inventory(self):
        return {p.stem: p.stat().st_size for p in self.directory.glob("*.json")}


def _protected_snapshot(root):
    """Short metadata read: checkpoint dependencies plus every live holder."""
    protected, live_temps = set(), set()
    db_path = root / "state.sqlite"
    if db_path.exists():
        db = connect(db_path)
        try:
            rows = db.execute(
                "SELECT checkpoint FROM jobs WHERE checkpoint IS NOT NULL"
            ).fetchall()
            for (cp_text,) in rows:
                protected.update(dependencies(json.loads(cp_text)))
        finally:
            db.close()
    for state in _scan_holders(root / "holders"):
        protected.update(state.get("oids", ()))
        live_temps.update(state.get("temps", ()))
    return protected, live_temps


def clean(root, hook=None):
    """Delete only unreferenced published payloads and orphaned write temps.

    Never takes ``worker.lock``; per-file revalidation happens after the
    ``before_delete`` gate, immediately before the actual unlink.
    """
    hook = hook or (lambda event, detail: None)
    root = Path(root)
    blobs_dir = root / "blobs"
    blobs_dir.mkdir(parents=True, exist_ok=True)
    protected, live_temps = _protected_snapshot(root)
    removed, released = [], 0

    def delete_if_still_unprotected(path, rel, key, protected_key, temp_key=None):
        nonlocal released
        hook("before_delete", {"path": rel})
        # Re-adjudicate: a paused cleaner must not trust an expired snapshot.
        snap_oids, snap_temps = _protected_snapshot(root)
        if key in (snap_oids if protected_key else snap_temps):
            return False
        try:
            size = path.stat().st_size
            path.unlink()
        except FileNotFoundError:
            return False
        removed.append(rel)
        released += size
        return True

    for path in sorted(blobs_dir.glob("*.json"), key=lambda p: p.name):
        oid = path.stem
        if oid in protected:
            continue
        delete_if_still_unprotected(
            path, "blobs/" + path.name, oid, protected_key=True)

    for path in sorted(blobs_dir.glob(".writing-*"), key=lambda p: p.name):
        if path.name in live_temps:
            continue
        delete_if_still_unprotected(
            path, "blobs/" + path.name, path.name, protected_key=False,
            temp_key=True)

    sync_dir(blobs_dir)
    return {"removed_files": sorted(removed), "released_bytes": released}


@contextlib.contextmanager
def worker(root):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    with (root / "worker.lock").open("a+b") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("WORKER_BUSY")
        try:
            yield
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

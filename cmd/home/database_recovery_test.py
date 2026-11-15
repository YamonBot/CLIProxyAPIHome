"""Disrupt only the explicitly disposable CI PostgreSQL service; exercise real Home."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--postgres-container", required=True)
parser.add_argument("--home-binary", required=True, type=Path)
args = parser.parse_args()
if os.environ.get("CLIPROXY_HOME_DISPOSABLE_POSTGRES") != "1":
    raise SystemExit("Requires explicit disposable-test PostgreSQL opt-in")
binary = args.home_binary.resolve(strict=True)
container = args.postgres_container
info = json.loads(subprocess.check_output(["docker", "inspect", container]))[0]
assert "POSTGRES_DB=home_test" in info["Config"]["Env"], "Refusing non-fixture database"


def run(*command):
    return subprocess.check_output(command, text=True, stderr=subprocess.STDOUT, timeout=30).strip()


def sql(query):
    return run("docker", "exec", container, "psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "home_recovery", "-Atc", query)


def wait_until(check, timeout=45):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.1)
    raise AssertionError("Timed out waiting for database recovery condition")


run("docker", "exec", container, "createdb", "-U", "postgres", "home_recovery")
with tempfile.TemporaryDirectory(prefix="home-db-recovery-") as directory:
    root = Path(directory)
    (root / "cluster.yaml").write_text("""pgsql:
  postgres_uri: postgres://postgres:postgres@127.0.0.1:5432/home_recovery?sslmode=disable&connect_timeout=2
node:
  external-ip: 127.0.0.1
  port: 18327
  heartbeat-interval: 1s
  heartbeat-timeout: 20s
  event-poll-interval: 1s
""")
    (root / "config.yaml").write_text("""config-version: 8
management:
  secret-key: disposable-recovery-test
  allow-remote: false
  disable-control-panel: true
routing:
  strategy: round-robin
  session-affinity: true
  session-affinity-ttl: 1h
""")
    subprocess.run([str(binary), "-config", "config.yaml", "-import"], cwd=root, check=True, stdout=subprocess.DEVNULL, timeout=60)
    with (root / "home.log").open("w+") as log:
        home = subprocess.Popen([str(binary), "-config", "config.yaml", "-addr", "127.0.0.1:18327"], cwd=root, stdout=log, stderr=subprocess.STDOUT)
        stopped = False
        try:
            def healthy():
                assert home.poll() is None, "Home exited: " + (root / "home.log").read_text()[-3000:]
                request = urllib.request.Request("http://127.0.0.1:18327/v8/management/config", headers={"Authorization": "Bearer disposable-recovery-test"})
                try:
                    with urllib.request.urlopen(request, timeout=2) as response:
                        return response.status == 200
                except (urllib.error.URLError, TimeoutError):
                    return False

            wait_until(healthy)
            key = "a" * 64
            sql("INSERT INTO session_affinity(key,credential_id,revision,expires_at) VALUES ('" + key + "','fixture-credential','00000000-0000-0000-0000-000000000001',now()+interval '1 hour')")
            binding_query = "SELECT row_to_json(s)::text FROM session_affinity s WHERE key='" + key + "'"
            before_binding = sql(binding_query)
            before_home = sql("SELECT started_at::text||'|'||state||'|'||last_seen_at::text FROM home_process_incarnation ORDER BY started_at DESC LIMIT 1").split("|")
            run("docker", "stop", "--time", "1", container)
            stopped = True
            deadline = time.monotonic() + 4
            while time.monotonic() < deadline:
                assert home.poll() is None, "Home exited during real DB outage: " + (root / "home.log").read_text()[-3000:]
                time.sleep(0.1)
            run("docker", "start", container)
            stopped = False
            wait_until(lambda: subprocess.run(["docker", "exec", container, "pg_isready", "-U", "postgres", "-d", "home_recovery"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0)
            wait_until(healthy)
            after_home = []

            def heartbeat_resumed():
                current = sql("SELECT started_at::text||'|'||state||'|'||last_seen_at::text FROM home_process_incarnation ORDER BY started_at DESC LIMIT 1").split("|")
                after_home[:] = current
                return current[0] == before_home[0] and current[1] == "active" and current[2] > before_home[2]

            wait_until(heartbeat_resumed)
            assert sql(binding_query) == before_binding, "Session affinity binding changed across DB outage"
            text = (root / "home.log").read_text()
            assert "cluster event database unavailable; retrying" in text, "Event watcher outage not exercised"
            assert "cluster heartbeat database unavailable; retrying" in text, "Coordinator outage not exercised"
            print(json.dumps({"actual_postgres_stop_start": True, "outage_seconds": 4, "home_pid": home.pid, "same_process_alive": home.poll() is None, "same_incarnation": after_home[0] == before_home[0], "heartbeat_resumed": True, "session_affinity_unchanged": True, "management_recovered": True}))
        finally:
            if stopped:
                run("docker", "start", container)
            home.terminate()
            try:
                home.wait(timeout=10)
            except subprocess.TimeoutExpired:
                home.kill()
                home.wait(timeout=5)
run("docker", "exec", container, "dropdb", "-U", "postgres", "home_recovery")

#!/usr/bin/env python3
"""Staging gate for hermes-proxy-relay v1.10.0 on :4003 against the REAL
upstream through the REAL Decodo pool, using the production systemd env +
prod config.json. Never prints secrets."""
import json
import os
import subprocess
import sys
import time
import urllib.request

REPO = "/home/hindsight/hermes-proxy-relay"
CONFIG = "/home/hindsight/.hermes/proxy-relay/config.json"
UNIT = "/tmp/hpr-unit.env"
PORT = 4003
VENV = "/usr/local/lib/hermes-agent/venv"


def load_unit_env(unit_path):
    env = {}
    if os.path.exists(unit_path):
        with open(unit_path) as f:
            for line in f:
                line = line.strip()
                if not line.startswith("Environment="):
                    continue
                for pair in line[len("Environment="):].split():
                    if "=" in pair:
                        k, v = pair.split("=", 1)
                        env[k] = v
                    else:
                        env[pair] = ""
    return env


def main():
    unit_env = load_unit_env(UNIT)
    required = ["DECODO_HOST", "DECODO_USER", "DECODO_PASS",
                "DECODO_START_PORT", "DECODO_END_PORT"]
    missing = [k for k in required if k not in unit_env or not unit_env[k]]
    if missing:
        print("MISSING_ENV:", missing)
        sys.exit(1)

    # The unit's DECODO_* env does NOT carry the upstream URL/key; those live
    # in the prod config.json. The upstream key is read from config by the
    # relay; we never print it. This check just confirms the config is the
    # production one (the relay does its own --check validation).
    relay_env = dict(os.environ)
    relay_env.update(unit_env)
    relay_env["RELAY_PORT"] = str(PORT)

    # 1) config check
    cmd = [f"{VENV}/bin/python", f"{REPO}/relay/relay.py",
           "--config", CONFIG, "--check"]
    r = subprocess.run(cmd, env=relay_env, capture_output=True, text=True)
    print("config-check rc=", r.returncode)
    if r.returncode != 0:
        print(r.stdout[-2000:])
        print(r.stderr[-2000:])
        sys.exit(1)

    # 2) launch staging relay
    cmd = [f"{VENV}/bin/python", f"{REPO}/relay/relay.py",
           "--config", CONFIG]
    proc = subprocess.Popen(cmd, env=relay_env,
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)
    print("STAGING PID=", proc.pid)

    # 3) wait for /health
    hurl = f"http://127.0.0.1:{PORT}/health"
    health = None
    for _ in range(40):
        try:
            with urllib.request.urlopen(hurl, timeout=2) as resp:
                health = json.loads(resp.read())
                if health.get("version") == "1.10.0":
                    break
        except Exception:
            pass
        time.sleep(1)
    if health is None or health.get("version") != "1.10.0":
        print("STAGING FAILED to come up")
        proc.terminate()
        sys.exit(1)

    print("STAGING UP version=", health.get("version"),
          "proxies=", health.get("pool_stats", {}).get("total"),
          "dynamic_cap=", health.get("dynamic_cap", {}).get("enabled"),
          "effective_max=", health.get("dynamic_cap", {}).get("effective_max"))

    # 4) live completion through the staging pool (alias-translated model)
    body = json.dumps({
        "model": "oc-deepseek-v4-flash",
        "messages": [{"role": "user", "content": "Reply with exactly: STAGING-OK"}],
        "max_tokens": 10,
        "stream": False,
    }).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{PORT}/v1/chat/completions",
        data=body,
        headers={"Content-Type": "application/json",
                 "Authorization": "Bearer public"},
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read())
            status = 200
    except urllib.error.HTTPError as e:
        status = e.code
        data = {"error": e.read().decode()[:300]}
    print("live-completion status=", status, "choices=",
          len(data.get("choices", [])) if isinstance(data, dict) else 0)

    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()

    ok = status == 200
    print("STAGING GATE PASSED" if ok else "STAGING FAILED")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()

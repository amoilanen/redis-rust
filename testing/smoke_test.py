#!/usr/bin/env python3
"""
Semi-automated smoke test for redis-rust.

This script starts a master + 3 replicas, runs a series of checks, and
prints a human-readable pass/fail report.  Useful for quick sanity checks
without the full pytest suite.

Usage:
    python3 testing/smoke_test.py
    python3 testing/smoke_test.py --master-port 6379
"""

import argparse
import sys
import time
from contextlib import contextmanager
from typing import List

# Ensure the testing package can find conftest helpers
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent))
from conftest import ServerProcess, make_client, REPLICA_HANDSHAKE_WAIT

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PASS = "\033[92m PASS \033[0m"
FAIL = "\033[91m FAIL \033[0m"
WARN = "\033[93m WARN \033[0m"

results: List[tuple] = []


def check(name: str, condition: bool, detail: str = ""):
    status = PASS if condition else FAIL
    results.append((name, condition, detail))
    extra = f"  ({detail})" if detail else ""
    print(f"  [{status}] {name}{extra}")


@contextmanager
def section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")
    yield


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Redis-Rust Smoke Test")
    parser.add_argument("--master-port", type=int, default=16400, help="Port for master")
    args = parser.parse_args()

    master_port = args.master_port
    replica_ports = [master_port + 1, master_port + 2, master_port + 3]

    servers: List[ServerProcess] = []

    try:
        # ---- Start servers ----
        with section("Starting servers"):
            print(f"  Starting master on port {master_port}...")
            master = ServerProcess(port=master_port)
            master.start()
            servers.append(master)
            print(f"  Master ready.")

            for rp in replica_ports:
                print(f"  Starting replica on port {rp}...")
                replica = ServerProcess(
                    port=rp,
                    replicaof=f"127.0.0.1 {master_port}",
                )
                replica.start()
                servers.append(replica)
                print(f"  Replica {rp} ready.")

            print(f"  Waiting {REPLICA_HANDSHAKE_WAIT}s for handshake...")
            time.sleep(REPLICA_HANDSHAKE_WAIT)

        mc = master.client()

        # ---- Basic commands ----
        with section("Basic commands (master)"):
            check("PING", mc.ping() is True)
            check("ECHO", mc.echo("hello") == "hello")
            mc.set("smoke_key", "smoke_val")
            check("SET/GET", mc.get("smoke_key") == "smoke_val")
            check("GET missing", mc.get("no_such_key") is None)

            info = mc.execute_command("INFO", "replication")
            if isinstance(info, dict):
                check("INFO replication role", info.get("role") == "master")
                check("INFO replication replid", "master_replid" in info)
            else:
                check("INFO replication role", "role:master" in info)
                check("INFO replication replid", "master_replid:" in info)

            # Test COMMAND via raw socket (redis-py tries to parse the response)
            import socket as sock
            s = sock.socket(sock.AF_INET, sock.SOCK_STREAM)
            s.settimeout(3)
            s.connect(("127.0.0.1", master_port))
            s.sendall(b"*1\r\n$7\r\nCOMMAND\r\n")
            cmd_data = s.recv(1024)
            s.close()
            check("COMMAND", cmd_data == b"+OK\r\n")

        # ---- Expiration ----
        with section("Key expiration"):
            mc.set("ttl_key", "ttl_val", px=800)
            check("SET PX immediate", mc.get("ttl_key") == "ttl_val")
            time.sleep(1.2)
            check("SET PX expired", mc.get("ttl_key") is None)

        # ---- Replication ----
        with section("Replication: write propagation"):
            mc.set("rep_key1", "alice")
            mc.set("rep_key2", "bob")
            mc.set("rep_key3", "charlie")
            time.sleep(1.0)

            for rp in replica_ports:
                rc = make_client(rp)  # lightweight client
                for key, expected in [("rep_key1", "alice"), ("rep_key2", "bob"), ("rep_key3", "charlie")]:
                    val = rc.get(key)
                    check(
                        f"Replica {rp} GET {key}",
                        val == expected,
                        f"expected={expected!r} got={val!r}",
                    )

        with section("Replication: overwrite propagation"):
            mc.set("ow_key", "v1")
            time.sleep(0.5)
            mc.set("ow_key", "v2")
            time.sleep(1.0)

            for rp in replica_ports:
                rc = make_client(rp)
                val = rc.get("ow_key")
                check(f"Replica {rp} overwrite", val == "v2", f"got={val!r}")

        with section("Replication: replica INFO"):
            for rp in replica_ports:
                rc = make_client(rp)
                info = rc.execute_command("INFO", "replication")
                if isinstance(info, dict):
                    check(f"Replica {rp} role:slave", info.get("role") == "slave")
                else:
                    check(f"Replica {rp} role:slave", "role:slave" in info)

        # ---- Concurrent clients ----
        with section("Concurrent clients"):
            c1 = master.client()
            c2 = master.client()
            c1.set("cc_key", "from_c1")
            check("Client 2 reads client 1 write", c2.get("cc_key") == "from_c1")
            c2.set("cc_key", "from_c2")
            check("Client 1 reads client 2 write", c1.get("cc_key") == "from_c2")

    finally:
        # ---- Cleanup ----
        print(f"\n{'='*60}")
        print("  Stopping servers...")
        for srv in reversed(servers):
            srv.stop()
        print("  All servers stopped.")

    # ---- Summary ----
    total = len(results)
    passed = sum(1 for _, ok, _ in results if ok)
    failed = total - passed

    print(f"\n{'='*60}")
    print(f"  RESULTS: {passed}/{total} passed", end="")
    if failed:
        print(f", \033[91m{failed} FAILED\033[0m")
    else:
        print(f"  \033[92mALL PASSED\033[0m")
    print(f"{'='*60}\n")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()

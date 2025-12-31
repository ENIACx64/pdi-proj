# File: gen.py
# Author: Jindrich Vodak (xvodak06)
# Date: 31. 12. 2025

import json
import time
import random
import argparse
import socket
import sys


# Randomly generates an IP address
def random_ip(prefix):
    return f"{prefix}.{random.randint(0,255)}.{random.randint(0,255)}"


# Randomly generates an event
def generate_event(mode, cfg):
    now_ms = int(time.time() * 1000)
    event_time = now_ms

    if random.random() < cfg.late_prob:
        event_time -= random.randint(1, cfg.max_late_ms)

    if mode == "normal":
        return {
            "eventTime": event_time,
            "srcIp": random_ip("10"),
            "dstIp": random_ip("192"),
            "srcPort": random.randint(1024, 65535),
            "dstPort": random.choice([80, 443, 53]),
            "protocol": "TCP",
            "packets": random.randint(1, 10),
            "bytes": random.randint(100, 1500),
            "syn": False
        }

    if mode == "syn_flood":
        return {
            "eventTime": event_time,
            "srcIp": random_ip("10"),
            "dstIp": cfg.target_ip,
            "srcPort": random.randint(1024, 65535),
            "dstPort": cfg.target_port,
            "protocol": "TCP",
            "packets": 1,
            "bytes": 60,
            "syn": True
        }

    if mode == "port_scan":
        return {
            "eventTime": event_time,
            "srcIp": cfg.attacker_ip,
            "dstIp": cfg.scan_target_ip,
            "srcPort": random.randint(1024, 65535),
            "dstPort": random.randint(1, 1024),
            "protocol": "TCP",
            "packets": 1,
            "bytes": 60,
            "syn": True
        }

    raise ValueError("Unknown mode")


# -------------------------
# Main
# -------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--mode",
        choices=["normal", "syn_flood", "port_scan"],
        help="Generate a single traffic mode"
    )
    group.add_argument(
        "--scenario",
        choices=[
            "normal_only",
            "syn_flood_only",
            "port_scan_only",
            "mixed_syn_flood",
            "mixed_port_scan"
        ],
        help="Run a time-based scenario"
    )

    parser.add_argument("--late-prob", type=float, default=0.05)
    parser.add_argument("--max-late-ms", type=int, default=20000)

    parser.add_argument("--normal-rate", type=int, default=5)
    parser.add_argument("--attack-rate", type=int, default=200)

    parser.add_argument("--attack-start", type=int, default=30)
    parser.add_argument("--attack-duration", type=int, default=20)

    parser.add_argument("--target-ip", default="192.168.1.10")
    parser.add_argument("--target-port", type=int, default=80)

    parser.add_argument("--attacker-ip", default="10.0.0.5")
    parser.add_argument("--scan-target-ip", default="192.168.1.20")

    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9999)

    cfg = parser.parse_args()

    # -------------------------
    # Socket server setup
    # -------------------------
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((cfg.host, cfg.port))
    server.listen(1)

    print(f"[GEN] Listening on {cfg.host}:{cfg.port}, waiting for Flink...")
    conn, addr = server.accept()
    print(f"[GEN] Flink connected from {addr}")

    start_time = time.time()

    try:
        while True:
            elapsed = time.time() - start_time

            if cfg.mode:
                mode = cfg.mode
                rate = cfg.attack_rate if mode != "normal" else cfg.normal_rate
            else:
                if cfg.scenario == "normal_only":
                    mode = "normal"
                    rate = cfg.normal_rate

                elif cfg.scenario == "syn_flood_only":
                    mode = "syn_flood"
                    rate = cfg.attack_rate

                elif cfg.scenario == "port_scan_only":
                    mode = "port_scan"
                    rate = cfg.attack_rate

                elif cfg.scenario == "mixed_syn_flood":
                    if cfg.attack_start <= elapsed < cfg.attack_start + cfg.attack_duration:
                        mode = "syn_flood"
                        rate = cfg.attack_rate
                    else:
                        mode = "normal"
                        rate = cfg.normal_rate

                elif cfg.scenario == "mixed_port_scan":
                    if cfg.attack_start <= elapsed < cfg.attack_start + cfg.attack_duration:
                        mode = "port_scan"
                        rate = cfg.attack_rate
                    else:
                        mode = "normal"
                        rate = cfg.normal_rate

            event = generate_event(mode, cfg)
            line = json.dumps(event) + "\n"
            conn.sendall(line.encode("utf-8"))

            time.sleep(max(0.001, 1.0 / rate))

    except BrokenPipeError:
        print("[GEN] Flink disconnected.")
    finally:
        conn.close()
        server.close()

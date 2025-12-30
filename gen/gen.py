import json
import time
import random
import argparse
from datetime import datetime

def random_ip(prefix):
    return f"{prefix}.{random.randint(0,255)}.{random.randint(0,255)}"

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
            "dstIp": random_ip("192"),
            "srcPort": random.randint(1024, 65535),
            "dstPort": cfg.scan_port,
            "protocol": "TCP",
            "packets": 1,
            "bytes": 60,
            "syn": True
        }

    raise ValueError("Unknown mode")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True, choices=["normal", "syn_flood", "port_scan"])
    parser.add_argument("--rate", type=int, default=1)
    parser.add_argument("--late-prob", type=float, default=0.05)
    parser.add_argument("--max-late-ms", type=int, default=20000)
    parser.add_argument("--target-ip", default="192.168.1.10")
    parser.add_argument("--target-port", type=int, default=80)
    parser.add_argument("--attacker-ip", default="10.0.0.5")
    parser.add_argument("--scan-port", type=int, default=22)

    cfg = parser.parse_args()

    while True:
        event = generate_event(cfg.mode, cfg)
        print(json.dumps(event), flush=True)
        time.sleep(max(0.001, 1.0 / cfg.rate))

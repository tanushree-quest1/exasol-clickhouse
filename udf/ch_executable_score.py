#!/usr/bin/env python3
"""ClickHouse Executable UDF script: reads TabSeparated input from stdin and outputs float probability."""
import math
import sys

# Pre-loaded logistic weights
WEIGHTS = [0.0085, 0.0012, 0.045, 1.85, -0.65]
BIAS = -2.85


def score_row(amount: float, mcc: float, hour: float, is_cnp: float, is_contactless: float) -> float:
    z = (
        amount * WEIGHTS[0]
        + mcc * WEIGHTS[1]
        + hour * WEIGHTS[2]
        + is_cnp * WEIGHTS[3]
        + is_contactless * WEIGHTS[4]
        + BIAS
    )
    return 1.0 / (1.0 + math.exp(-max(min(z, 20.0), -20.0)))


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t")
        try:
            amount = float(parts[0])
            mcc = float(parts[1])
            hour = float(parts[2])
            is_cnp = float(parts[3])
            is_contactless = float(parts[4])
            prob = score_row(amount, mcc, hour, is_cnp, is_contactless)
            sys.stdout.write(f"{prob:.4f}\n")
            sys.stdout.flush()
        except Exception:
            sys.stdout.write("0.0000\n")
            sys.stdout.flush()


if __name__ == "__main__":
    main()

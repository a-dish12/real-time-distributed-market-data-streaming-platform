
import csv
import random

# ---------------------------------------------------------------- parameters
SEED = 20260819

T = 1787200000.0          # base time, on a whole-second window boundary
WINDOW = 1.0              # must match consumer.WINDOW
DELAY = 0.2               # must match consumer.DELAY

N_WINDOWS = 10            # victim data spans [T, T + N_WINDOWS * WINDOW)
EVENTS_PER_VICTIM = 1000  # per victim symbol
EVENTS_PER_DRIVER = 100   # per driver symbol

DRIVER_OFFSET = 15.0      # floor is N_WINDOWS * WINDOW + DELAY = 10.2

VICTIM_SYMBOLS = {"AAPL": 0, "MSFT": 0, "INTC": 1}
DRIVER_SYMBOLS = {"TSLA": 2, "NVDA": 2}

START_PRICE = 100.0
OUTFILE = "csvs/skew_workload.csv"

FIELDS = [
    "phase", "symbol", "price", "event_time", "seq", "size",
    "expected_partition", "expected_window_start", "expected_drop_shared",
]


def check_parameters():
    """The experiment is only valid if phase 1 clears the last victim window."""
    floor = N_WINDOWS * WINDOW + DELAY
    assert DRIVER_OFFSET >= floor, (
        f"DRIVER_OFFSET {DRIVER_OFFSET} is below the floor {floor}. "
        f"Victim windows after T + {DRIVER_OFFSET - DELAY} would survive."
    )
    assert EVENTS_PER_VICTIM >= N_WINDOWS, "Need >= 1 event per victim window."
    return floor, N_WINDOWS * WINDOW


def build_rows():
    rng = random.Random(SEED)
    prices = {s: START_PRICE for s in list(VICTIM_SYMBOLS) + list(DRIVER_SYMBOLS)}
    rows = []

    # phase 1 driver, late event_times, sent first
    # only the earliest driver event matters for the mechanism- the rest give
    # the partition a realistic stream rather than a single tick
    for symbol, partition in DRIVER_SYMBOLS.items():
        for seq in range(EVENTS_PER_DRIVER):
            prices[symbol] += rng.gauss(0, 0.05)
            event_time = T + DRIVER_OFFSET + seq * (WINDOW / EVENTS_PER_DRIVER)
            ws = (event_time // WINDOW) * WINDOW
            rows.append({
                "phase": 1,
                "symbol": symbol,
                "price": round(prices[symbol], 6),
                "event_time": round(event_time, 6),
                "seq": seq,
                "size": 1000,
                "expected_partition": partition,
                "expected_window_start": round(ws, 6),
                "expected_drop_shared": 0,
            })

    # phase 2 victims- early event_times, sent second
    # spread evenly across [T, T + N_WINDOWS * WINDOW) so the loss is sustained
    # across many consecutive windows rather than concentrated in one
    span = N_WINDOWS * WINDOW
    victim_rows = []
    for symbol, partition in VICTIM_SYMBOLS.items():
        for seq in range(EVENTS_PER_VICTIM):
            prices[symbol] += rng.gauss(0, 0.05)
            event_time = T + (seq + 0.5) * (span / EVENTS_PER_VICTIM)
            ws = (event_time // WINDOW) * WINDOW
            victim_rows.append({
                "phase": 2,
                "symbol": symbol,
                "price": round(prices[symbol], 6),
                "event_time": round(event_time, 6),
                "seq": seq,
                "size": 1000,
                "expected_partition": partition,
                "expected_window_start": round(ws, 6),
                "expected_drop_shared": 1,
            })

    # sort phase 2 by event_time so each partition's stream is monotone in
    # event time, matching a real feed. this keeps within-partition
    # out-of-order arrival out of the experiment entirely
    victim_rows.sort(key=lambda r: (r["event_time"], r["symbol"]))
    rows.extend(victim_rows)
    return rows


def main():
    floor, span = check_parameters()
    rows = build_rows()

    with open(OUTFILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
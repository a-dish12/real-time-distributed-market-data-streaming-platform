# Measuring the shared-watermark bug

A controlled before/after run of the bug that motivated [per-partition
watermarks](watermarks.md). One variable changes between the two runs: whether the
watermark is a single module-level `float` or a `dict[int, float]` keyed by partition.
Everything else — the workload file, the broker, the topic, the counters, the run
script — is identical.

- **before:** `experiment/shared-watermark`, one watermark for all partitions
- **after:** `main`, one watermark per partition

## The drop condition

The aggregator seals a window when the watermark reaches its end, and drops any event
whose window is already sealed. Both use the same comparison:

```
watermark   = event_time - DELAY
window_start = floor(event_time / WINDOW) * WINDOW
window_end   = window_start + WINDOW

drop  iff  watermark >= window_end
```

Nothing in that arithmetic reads the wall clock. Delivery rate cannot change which
windows seal or which events drop — only event-time values can. That is why the replay
sends flat out.

With a shared watermark, `event_time` on *any* partition moves the watermark for *every*
partition. So for a driver event on partition 2 to seal a victim window on partition 0,
its event time only has to clear that window's end by the grace period:

```
DRIVER_OFFSET >= N_WINDOWS * WINDOW + DELAY
              >= 10 * 1.0 + 0.2
              >= 10.2
```

The workload uses `DRIVER_OFFSET = 15.0`, comfortably above the floor, so phase 1 seals
*every* victim window rather than only the later ones.

## The workload

Generated once by [`scripts/skewed.py`](../scripts/skewed.py) into
`csvs/skew_workload.csv`, committed, and replayed byte-identically by both runs. It
inverts arrival order against event-time order:

| phase | symbols | partition | event_time | events | sent |
|---|---|---|---|---|---|
| 1 (driver) | TSLA, NVDA | 2 | `T + 15.0 .. T + 15.99` | 200 | first |
| 2 (victim) | AAPL, MSFT / INTC | 0 / 1 | `T + 0.005 .. T + 9.995` | 3000 | second |

`T = 1787200000.0`, chosen on a whole-second window boundary. Victim events are spread
evenly across ten consecutive windows so the loss is sustained rather than concentrated
in one, and are sorted by event time so each partition's stream is monotone — within-
partition out-of-order arrival stays out of the experiment entirely. Driver events span
0.99s, landing all 200 inside the single window starting `T + 15.0`.

Partition assignment was verified independently against kafka-python's default
partitioner (`murmur2(key) & 0x7fffffff % 3`), and the run script aborts unless
`market-events` has exactly 3 partitions. A different partition count rehashes the
symbols and silently invalidates the design.

The producer takes `event_time` from the CSV column, never from `time.time()`. Stamping
at send time would make arrival order and event-time order agree, and no drop could
occur at all. `flush()` at each phase boundary keeps phase 1 strictly ahead of phase 2.

## Prediction

Made before running. The bar count of 32 assumes the 200 driver events fall in one
window per symbol, which the CSV confirms — 32 distinct `(symbol, window_start)` pairs.

| | dropped | distinct bars | unaccounted | watermark / backstop / drain |
|---|---|---|---|---|
| `main` | 0 | 32 (30 victim + 2 driver) | 0 | 27 / 5 / 0 |
| `experiment/shared-watermark` | 3000 | 2 (driver only) | 0 | 0 / 2 / 0 |

All 3000 drops on partitions 0 and 1; none on partition 2. The five backstop seals on
`main` are the three trailing victim windows (`window_start = T + 9.0`, end `T + 10.0`,
above the final victim watermark of `T + 9.795`) and the two driver windows (end
`T + 16.0`, above `T + 15.79`).

## Measured

Reports: [`reports/main.json`](../reports/main.json),
[`reports/shared-watermark.json`](../reports/shared-watermark.json).

| | `main` | `experiment/shared-watermark` |
|---|---|---|
| events consumed | 3200 | 3200 |
| events represented in bars | 3200 | 200 |
| events dropped | 0 | 3000 |
| unaccounted | 0 | 0 |
| bar send failures | 0 | 0 |
| distinct bars emitted | 32 | 2 |
| seal categories | 27 watermark, 5 backstop | 2 backstop |
| drain bars | 0 | 0 |

Per partition:

| | `main` drops / bars | shared drops / bars |
|---|---|---|
| p0 (AAPL, MSFT) | 0 / 20 | 2000 / 0 |
| p1 (INTC) | 0 / 10 | 1000 / 0 |
| p2 (TSLA, NVDA) | 0 / 2 | 0 / 2 |

Every prediction held, including the 27/5/0 seal split. Thirty bars present on `main`
are absent on the branch; the two that survive are the driver's own.

**The failure shape is silent loss, not corruption.** Victim events hit the drop branch
before an accumulator is ever created, so `windows[0]` and `windows[1]` stay empty and
the backstop's `if not windows[p]: continue` guard skips those partitions entirely.
Nothing is late and nothing is wrong — 30 bars simply never exist. The `bars` topic
carries two well-formed candles and no indication that 3000 events went missing.

One detail the audit trail surfaced: the watermark recorded at every one of the 3000
drops is `T + 16.0`, not the `T + 15.79` that `handle_tick` alone would produce. During
the two-second `PHASE_GAP`, partition 2 went idle and the backstop raised the shared
watermark to `max(window_start) + WINDOW`. Both values clear every victim `window_end`
(the largest is `T + 10.0`), so the count is unaffected — but it is worth recording that
on this branch two different paths advance the shared watermark past another partition's
data, not one.

## Deviation from `bd12fed`

`bd12fed` is the original shared-watermark implementation, read for reference and
deliberately **not** checked out. It predates per-partition watermarks, the idle
backstop, the `bars` topic and the poll-based loop; it uses a flat `windows` dict keyed
by `(symbol, window_start)` with no partition dimension, no `count` on the accumulator,
and `for message in consumer` rather than `consumer.poll()`. Checking it out would
change a dozen things at once and destroy attribution.

So the branch keeps `windows` and `last_activity` keyed by partition, keeps `sweep`'s
`partition` argument, keeps it sweeping only `windows[partition]`, and keeps the
backstop's `target` computed from `windows[p]` alone. **This is a controlled deviation,
not carelessness.** With a flat window dict, an idle partition's backstop could seal
another partition's windows — a second possible cause for the same drops. Preserving the
partition dimension removes that confound and leaves cross-partition watermark
advancement as the only mechanism that can produce a drop.

The branch diff against `main` is seven lines: the declaration and three call sites,
plus two `global` statements.

## Reproducing

```
git checkout main                          # or experiment/shared-watermark
make experiment REPORT=reports/main.json
```

[`scripts/run_experiment.sh`](../scripts/run_experiment.sh) wipes Kafka (`down -v`),
creates the topics explicitly, aborts unless `market-events` has 3 partitions, starts
the aggregator with a fresh consumer group, replays the workload, waits out the idle
backstop, then sends SIGINT to trigger the drain and the report.

Three properties make a run self-invalidating rather than quietly wrong:

- **conservation**: `consumed - represented - dropped` must be 0, so a loss path that no
  counter catches shows up as a non-zero number instead of a plausible one
- **bar send failures**: `on_send_error` counts permanent failures, and the report is
  built after `flush()` because that is when they surface. Without it, bars that never
  reached the topic would still count as represented
- **drain bars**: the unconditional shutdown drain is tagged separately, so a non-zero
  drain count means the consumer was stopped before the backstop finished

All three were zero on both runs.

## Result

Reverting to a single shared watermark across 3 Kafka partitions silently dropped
**3000 of 3000 events (100%) on the two lagging partitions and lost 30 of 32 candles**,
with zero errors logged and a conservation check that still balanced.

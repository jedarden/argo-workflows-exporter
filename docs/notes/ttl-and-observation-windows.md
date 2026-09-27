# TTL and observation windows

This is the constraint the whole design turns on.

## A live listing is a biased sample

Argo deletes completed `Workflow` objects on the schedule set by
`ttlStrategy`. A representative configuration:

```yaml
ttlStrategy:
  secondsAfterSuccess: 1800     # 30 minutes
  secondsAfterFailure: 7200     # 2 hours
  secondsAfterCompletion: 3600  # 1 hour
```

Those three numbers are not usually equal, and failures are normally kept
longest — they are the ones someone might come back to read. The effect is
that the number of workflows of a given outcome still visible at any moment
is roughly `arrival rate x that outcome's TTL`, so **the listing over-represents
whatever is retained longest**.

Worked example, at the values above. Take a pipeline that is genuinely
healthy: 90 successes and 10 failures per hour.

- successes visible: `90 x 0.5h = 45`
- failures visible: `10 x 2h = 20`

A dashboard reading the live list reports 69% success against a true rate of
90%. The error is not noise and does not average out — it is a fixed bias,
and it always points the same way.

It gets starker as the true rate improves, because the numerator shrinks
while the failure backlog does not. A measurement taken on a real
installation running the TTLs above: 2 Succeeded, 41 Failed, 8 Error, 13
Running. That reads as a fleet on fire. It is mostly an artifact of
successes being deleted four times faster than failures, plus a tail of
`Error` workflows that never ran at all and are not reaped on the same
schedule.

**Never compute a rate, a trend or a count of outcomes from
`workflows.parquet`.** It answers exactly one question honestly: what exists
right now. Everything historical comes from `runs.parquet`.

## Choosing the poll interval

The ledger can only record what it observed. A workflow object exists from
its creation until `finish + TTL`, so:

`POLL_INTERVAL_SECONDS` is the quiet time **after a cycle completes** before
the next cycle starts. It is not the time from one cycle start to the next.
Cycles run serially, so a slow cycle cannot overlap the following cycle. If a
cycle takes `C` seconds, consecutive starts are separated by `C +
POLL_INTERVAL_SECONDS`. A cycle that takes longer than the configured
interval therefore simply makes the effective start-to-start cadence longer;
there is no catch-up cycle. The same delay applies after an incomplete or
failed cycle, so failures are retried on the next scheduled cycle rather than
immediately.

Assuming cycles continue to complete successfully, let `C_max` be the
worst-case duration of a cycle. To guarantee that every run is observed at
least once **after it finishes** and before it is deleted, configure:

> `C_max + POLL_INTERVAL_SECONDS < the shortest TTL in effect`

The proof is short: after a run finishes, its object survives for `TTL`
seconds. In the worst case it finishes just after a cycle's listing, so the
next listing begins only after that cycle's remaining runtime plus the
post-cycle delay. If that effective cadence is shorter than `TTL`, the next
poll lands inside the surviving window and sees the final state. The strict
inequality leaves room for scheduling jitter and TTL-controller latency.

The same bound is the consumer-side heartbeat threshold. `generated_at` is the
cycle-start instant, captured once before collection and committed only by a
successful publication. If `C_max` is the consumer's configured upper bound
for a complete cycle, a `meta.json` sidecar is fresh only while:

```text
consumer_now - generated_at < C_max + POLL_INTERVAL_SECONDS
```

At equality or beyond, the consumer must treat the generation as stale. The
age includes a successful cycle's runtime because it starts at cycle start; a
slow cycle is covered by `C_max`. A failed cycle or failed publication does
not commit a new sidecar, so this test catches the frozen heartbeat even when
the previous `meta.json` and both Parquet footers still agree on one
generation id.

A failed cycle cannot provide this guarantee for the workflows it failed to
list; the condition describes the cadence between successful observations,
not an outage or an unavailable cluster.

Above that threshold the loss is silent — a missed run leaves no trace to
count, so the ledger simply under-reports, most severely for the fastest and
most successful runs. The default `POLL_INTERVAL_SECONDS=300` sits six times
inside a 1800s success TTL, leaving up to 1500 seconds for the worst-case
cycle if that success TTL is the shortest one in effect.

Two things that do **not** relax this:

- `podGC` (e.g. `OnPodCompletion`) deletes the *pods*, not the `Workflow`
  objects. It makes step logs unavailable but has no effect on what this
  exporter reads.
- A longer `secondsAfterFailure` does not help, because the binding
  constraint is the *shortest* TTL, which is normally the success one.

## Why not just enable the workflow archive

Argo can persist completed workflows to Postgres (`persistence:` in the
controller config), which solves retention properly and at the source. Where
that is already running, it is the better record.

This exporter is for the case where it is not: it needs no database, no
change to the controller's configuration, and no write access to anything in
the cluster. It reads through the same read-only API a human would, and its
output is columnar files a browser or query engine can read directly. The two
are not exclusive — the archive is the system of record, this is a
low-dependency observation log.

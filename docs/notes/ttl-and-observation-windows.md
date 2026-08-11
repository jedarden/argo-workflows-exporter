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

> If `POLL_INTERVAL_SECONDS <= the shortest TTL in effect`, every run is
> observed at least once **after it finished** and before it is deleted, and
> the ledger therefore records its terminal phase.

The proof is short: after a run finishes, its object survives for `TTL`
seconds. If the polling period `P` is no greater than `TTL`, at least one
poll must land inside that surviving window, and that poll sees the final
state.

Above that threshold the loss is silent — a missed run leaves no trace to
count, so the ledger simply under-reports, most severely for the fastest and
most successful runs. The default `POLL_INTERVAL_SECONDS=300` sits six times
inside a 1800s success TTL, which leaves room for a slow cycle without
crossing the line.

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

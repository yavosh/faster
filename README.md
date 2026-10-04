# faster

A tiny, dependency-free Go clone of [fast-cli](https://github.com/sindresorhus/fast-cli) — an internet speed test using Netflix's [fast.com](https://fast.com) infrastructure.

It scrapes fast.com's rotating API token, asks the fast.com API for CDN targets, then saturates them with parallel connections and reports the steady-state throughput.

## Install

```sh
go install github.com/yavosh/faster@latest
```

This drops a `faster` binary in `$(go env GOPATH)/bin` (make sure that's on your `PATH`).

Or build from source:

```sh
git clone https://github.com/yavosh/faster
cd faster
go build -o faster .
```

## Usage

```sh
faster                 # download speed, live, with latency
faster -upload         # also measure upload
faster -json           # machine-readable output
faster -simple         # just the download number in Mbps (for scripts)
```

### Flags

| Flag       | Default | Description                                              |
|------------|---------|----------------------------------------------------------|
| `-upload`  | `false` | Also measure upload speed                                |
| `-json`    | `false` | Emit results as JSON (download/upload/latency/client)    |
| `-simple`  | `false` | Print only the download speed in Mbps                    |
| `-avg`     | `false` | Report cumulative average rate instead of steady-state   |
| `-conns`   | `5`     | Number of parallel connections                           |
| `-bytes`   | `26214400` | Payload size per request in bytes                     |
| `-timeout` | `30s`   | Max duration per measurement                             |

`-conns`, `-bytes`, and `-timeout` must be greater than zero.
JSON output contains one object per line, suitable for JSON Lines files.

### Examples

```sh
# Steady-state download over 8 connections
faster -conns 8

# Log a JSON line to a file
faster -json -upload >> speedlog.jsonl

# Just the number, e.g. for a status bar
faster -simple
```

## How it works

1. **Token** — fetches `fast.com`, finds the `app-*.js` bundle, extracts the `token:"…"` embedded in it (the token rotates, so it's re-fetched each run).
2. **Targets** — calls `https://api.fast.com/netflix/speedtest/v2?token=…` for a list of CDN endpoints.
3. **Measure** — parallel workers stream `/range/0-N` payloads (or POST for upload) into a shared atomic byte counter; a sampler reports the smoothed instantaneous rate and stops once it plateaus (±2%) or the timeout hits.

By default `faster` reports the **steady-state** rate (what the live line settles on, matching fast.com's UI). Pass `-avg` for the cumulative average over the whole run.

## Notes

- This relies on an **unofficial**, undocumented endpoint that can change without notice.
- Numbers are in **megabits per second** (Mbps).
- Latency probes share a five-second deadline. Failed probes do not prevent throughput measurements.
- Transfer workers stop after three consecutive failures, with delays of 100 ms and 200 ms between attempts.
- Invalid options, exhausted retries, cancellation, and measurements without transferred data produce a nonzero exit status.
- The final speed includes the last partial sampling interval, including measurements shorter than 200 ms.

## License

MIT

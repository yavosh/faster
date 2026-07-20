package main

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"time"
)

// defaultChunkBytes is the payload size requested per download/upload request
// when none is configured. Large enough to keep a connection saturated, small
// enough to loop quickly.
const defaultChunkBytes = 26_214_400 // 25 MiB

// Direction is the transfer direction being measured.
type Direction int

const (
	Download Direction = iota
	Upload
)

// Result holds the outcome of one measurement pass. It reports both the
// cumulative average rate and the smoothed instantaneous (steady-state) rate;
// the caller chooses which to present.
type Result struct {
	Direction   Direction
	CumBytesPS  float64 // total bytes / total elapsed
	InstBytesPS float64 // smoothed rate over the recent window
	Bytes       int64
	Elapsed     time.Duration
}

// sample is one reading from the sampler loop, streamed to the caller for live
// UI. BytesPS is the smoothed instantaneous rate, matching what fast.com shows.
type sample struct {
	BytesPS float64
	Bytes   int64
	Elapsed time.Duration
}

// measure runs count parallel workers in the given direction against the
// targets, sampling throughput until the speed stabilizes or maxDur elapses.
// Progress samples are sent on progress (closed when done).
func measure(ctx context.Context, c *http.Client, targets []Target, dir Direction, count int, chunk int64, maxDur time.Duration, progress chan<- sample) Result {
	defer close(progress)

	var total atomic.Int64
	ctx, cancel := context.WithTimeout(ctx, maxDur)
	defer cancel()

	// Spawn workers, round-robining over the available targets.
	done := make(chan struct{})
	for i := range count {
		t := targets[i%len(targets)]
		go func() {
			for ctx.Err() == nil {
				worker(ctx, c, t, dir, chunk, &total)
			}
			done <- struct{}{}
		}()
	}

	start := time.Now()
	res := sampleUntilStable(ctx, &total, start, progress)

	cancel()
	for range count {
		<-done
	}
	res.Direction = dir
	return res
}

// stabilityWindow is how many recent samples must agree for us to call it
// stable. At the 200ms tick cadence this is a ~1.2s smoothing window.
const stabilityWindow = 6

// sampleUntilStable polls the byte counter at a fixed cadence, emits progress
// as a smoothed instantaneous rate, and returns once throughput plateaus or the
// context is done.
func sampleUntilStable(ctx context.Context, total *atomic.Int64, start time.Time, progress chan<- sample) Result {
	const tick = 200 * time.Millisecond
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	recent := make([]float64, 0, stabilityWindow) // per-tick instantaneous rates
	prevBytes := int64(0)
	prevTime := start

	for {
		select {
		case <-ctx.Done():
			return finalResult(total, start, recent)
		case <-ticker.C:
			bytes := total.Load()
			now := time.Now()

			// Instantaneous rate over just this tick interval.
			inst := float64(bytes-prevBytes) / now.Sub(prevTime).Seconds()
			prevBytes, prevTime = bytes, now

			recent = append(recent, inst)
			if len(recent) > stabilityWindow {
				recent = recent[1:]
			}
			smoothed := mean(recent)

			progress <- sample{BytesPS: smoothed, Bytes: bytes, Elapsed: now.Sub(start)}

			// Consider stable once the smoothed rate barely moves across the
			// window, but only after a short warm-up.
			if now.Sub(start) > 3*time.Second && stable(recent) {
				return Result{
					CumBytesPS:  float64(bytes) / now.Sub(start).Seconds(),
					InstBytesPS: smoothed,
					Bytes:       bytes,
					Elapsed:     now.Sub(start),
				}
			}
		}
	}
}

func mean(s []float64) float64 {
	if len(s) == 0 {
		return 0
	}
	var sum float64
	for _, v := range s {
		sum += v
	}
	return sum / float64(len(s))
}

// stable reports whether the samples in the window are within 2% of their mean.
func stable(s []float64) bool {
	if len(s) < stabilityWindow {
		return false
	}
	m := mean(s)
	if m == 0 {
		return false
	}
	for _, v := range s {
		if d := (v - m) / m; d > 0.02 || d < -0.02 {
			return false
		}
	}
	return true
}

func finalResult(total *atomic.Int64, start time.Time, recent []float64) Result {
	elapsed := time.Since(start)
	bytes := total.Load()
	return Result{
		CumBytesPS:  float64(bytes) / elapsed.Seconds(),
		InstBytesPS: mean(recent),
		Bytes:       bytes,
		Elapsed:     elapsed,
	}
}

// worker performs a single download or upload of chunk bytes, adding the number
// of bytes transferred to total as it goes.
func worker(ctx context.Context, c *http.Client, t Target, dir Direction, chunk int64, total *atomic.Int64) {
	u, err := rangeURL(t.URL, chunk)
	if err != nil {
		return
	}
	switch dir {
	case Download:
		download(ctx, c, u, total)
	case Upload:
		upload(ctx, c, u, chunk, total)
	}
}

func download(ctx context.Context, c *http.Client, u string, total *atomic.Int64) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return
	}
	req.Header.Set("User-Agent", userAgent)
	resp, err := c.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, &countingReader{r: resp.Body, total: total})
}

func upload(ctx context.Context, c *http.Client, u string, chunk int64, total *atomic.Int64) {
	body := &countingReader{r: io.LimitReader(rand.Reader, chunk), total: total}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, body)
	if err != nil {
		return
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = chunk
	resp, err := c.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
}

// countingReader tallies bytes read into an atomic counter as they stream past.
type countingReader struct {
	r     io.Reader
	total *atomic.Int64
}

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	if n > 0 {
		cr.total.Add(int64(n))
	}
	return n, err
}

// latency measures the minimum round-trip over a few tiny range requests.
func latency(ctx context.Context, c *http.Client, t Target, samples int) (time.Duration, error) {
	u, err := rangeURL(t.URL, 0)
	if err != nil {
		return 0, err
	}
	best := time.Duration(1<<63 - 1)
	var lastErr error
	for range samples {
		start := time.Now()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		req.Header.Set("User-Agent", userAgent)
		resp, err := c.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if d := time.Since(start); d < best {
			best = d
		}
	}
	if best == time.Duration(1<<63-1) {
		if lastErr == nil {
			lastErr = errors.New("no successful latency probe")
		}
		return 0, lastErr
	}
	return best, nil
}

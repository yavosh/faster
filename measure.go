package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
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
func measure(parent context.Context, c *http.Client, targets []Target, dir Direction, count int, chunk int64, maxDur time.Duration, progress chan<- sample) (Result, error) {
	defer close(progress)
	if count < 1 || chunk < 1 || maxDur <= 0 || len(targets) == 0 {
		return Result{}, errors.New("measurement requires positive connections, bytes, duration, and at least one target")
	}

	var total atomic.Int64
	ctx, cancel := context.WithTimeout(parent, maxDur)
	defer cancel()
	start := time.Now()

	// Spawn workers, round-robining over the available targets.
	done := make(chan struct{})
	failures := make(chan error, count)
	for i := range count {
		t := targets[i%len(targets)]
		go func() {
			var lastErr error
			defer func() {
				if lastErr != nil {
					failures <- lastErr
				}
				done <- struct{}{}
			}()
			attempts := 0
			for ctx.Err() == nil {
				err := worker(ctx, c, t, dir, chunk, &total)
				if ctx.Err() != nil {
					return
				}
				lastErr = err
				if err == nil {
					attempts = 0
					continue
				}
				attempts++
				if attempts == 3 {
					lastErr = fmt.Errorf("transfer failed after %d attempts: %w", attempts, err)
					cancel()
					return
				}
				timer := time.NewTimer(time.Duration(attempts) * 100 * time.Millisecond)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}()
	}

	res := sampleUntilStable(ctx, &total, start, progress)

	cancel()
	for range count {
		<-done
	}
	res.Direction = dir
	if err := parent.Err(); err != nil {
		return res, err
	}
	select {
	case err := <-failures:
		return res, err
	default:
	}
	if res.Bytes == 0 {
		return res, errors.New("no data transferred before measurement ended")
	}
	return res, nil
}

// stabilityWindow is how many recent samples must agree for us to call it
// stable. At the 200ms tick cadence this is a ~1.2s smoothing window.
const stabilityWindow = 6

type interval struct {
	bytes   int64
	elapsed time.Duration
}

// sampleUntilStable polls the byte counter at a fixed cadence, emits progress
// as a smoothed instantaneous rate, and returns once throughput plateaus or the
// context is done.
func sampleUntilStable(ctx context.Context, total *atomic.Int64, start time.Time, progress chan<- sample) Result {
	const tick = 200 * time.Millisecond
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	recent := make([]interval, 0, stabilityWindow)
	prevBytes := int64(0)
	prevTime := start

	for {
		select {
		case <-ctx.Done():
			return finalResult(total, start, recent, prevBytes, prevTime)
		case <-ticker.C:
			bytes := total.Load()
			now := time.Now()

			// Instantaneous rate over just this tick interval.
			inst := interval{bytes: bytes - prevBytes, elapsed: now.Sub(prevTime)}
			prevBytes, prevTime = bytes, now

			recent = append(recent, inst)
			if len(recent) > stabilityWindow {
				recent = recent[1:]
			}
			smoothed := mean(recent)

			select {
			case progress <- sample{BytesPS: smoothed, Bytes: bytes, Elapsed: now.Sub(start)}:
			case <-ctx.Done():
				return finalResult(total, start, recent, prevBytes, prevTime)
			}

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

func mean(s []interval) float64 {
	var bytes int64
	var elapsed time.Duration
	for _, v := range s {
		bytes += v.bytes
		elapsed += v.elapsed
	}
	if elapsed <= 0 {
		return 0
	}
	return float64(bytes) / elapsed.Seconds()
}

// stable reports whether the samples in the window are within 2% of their mean.
func stable(s []interval) bool {
	if len(s) < stabilityWindow {
		return false
	}
	m := mean(s)
	if m == 0 {
		return false
	}
	for _, v := range s {
		if v.elapsed <= 0 {
			return false
		}
		if d := (float64(v.bytes)/v.elapsed.Seconds() - m) / m; d > 0.02 || d < -0.02 {
			return false
		}
	}
	return true
}

func finalResult(total *atomic.Int64, start time.Time, recent []interval, prevBytes int64, prevTime time.Time) Result {
	bytes := total.Load()
	now := time.Now()
	elapsed := now.Sub(start)
	recent = append(recent, interval{bytes: bytes - prevBytes, elapsed: now.Sub(prevTime)})
	return Result{
		CumBytesPS:  float64(bytes) / elapsed.Seconds(),
		InstBytesPS: mean(recent),
		Bytes:       bytes,
		Elapsed:     elapsed,
	}
}

// worker performs a single download or upload of chunk bytes, adding the number
// of bytes transferred to total as it goes.
func worker(ctx context.Context, c *http.Client, t Target, dir Direction, chunk int64, total *atomic.Int64) error {
	u, err := rangeURL(t.URL, chunk)
	if err != nil {
		return errors.New("invalid target URL")
	}
	switch dir {
	case Download:
		return download(ctx, c, u, total)
	case Upload:
		return upload(ctx, c, u, chunk, total)
	}
	return errors.New("invalid transfer direction")
}

func download(ctx context.Context, c *http.Client, u string, total *atomic.Int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return errors.New("invalid download request")
	}
	req.Header.Set("User-Agent", userAgent)
	resp, err := c.Do(req)
	if err != nil {
		return requestError(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP status %d", resp.StatusCode)
	}
	n, err := io.Copy(io.Discard, &countingReader{r: resp.Body, total: total})
	if err == nil && n == 0 {
		return errors.New("empty download response")
	}
	return err
}

func upload(ctx context.Context, c *http.Client, u string, chunk int64, total *atomic.Int64) error {
	body := &countingReader{r: io.LimitReader(rand.Reader, chunk), total: total}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, body)
	if err != nil {
		return errors.New("invalid upload request")
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = chunk
	resp, err := c.Do(req)
	if err != nil {
		return requestError(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP status %d", resp.StatusCode)
	}
	_, err = io.Copy(io.Discard, resp.Body)
	return err
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
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	u, err := rangeURL(t.URL, 0)
	if err != nil {
		return 0, errors.New("invalid latency target URL")
	}
	best := time.Duration(1<<63 - 1)
	var lastErr error
	for range samples {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		start := time.Now()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return 0, errors.New("invalid latency request")
		}
		req.Header.Set("User-Agent", userAgent)
		resp, err := c.Do(req)
		if err != nil {
			lastErr = requestError(err)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			resp.Body.Close()
			lastErr = fmt.Errorf("HTTP status %d", resp.StatusCode)
			continue
		}
		_, err = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
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

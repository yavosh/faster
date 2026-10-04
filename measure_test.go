package main

import (
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func response(status int, body io.ReadCloser) *http.Response {
	return &http.Response{StatusCode: status, Body: body, Header: make(http.Header)}
}

func TestDownloadRejectsHTTPError(t *testing.T) {
	c := &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) {
		return response(503, io.NopCloser(strings.NewReader("unavailable"))), nil
	})}
	var total atomic.Int64
	if err := download(context.Background(), c, "https://example.invalid", &total); err == nil {
		t.Fatal("expected HTTP error")
	}
	if total.Load() != 0 {
		t.Fatal("counted error response as download traffic")
	}
}

type brokenReader struct{}

func (brokenReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestDownloadRejectsEmptyAndBrokenBodies(t *testing.T) {
	for name, body := range map[string]io.Reader{"empty": strings.NewReader(""), "broken": brokenReader{}} {
		t.Run(name, func(t *testing.T) {
			c := &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) {
				return response(200, io.NopCloser(body)), nil
			})}
			var total atomic.Int64
			if err := download(context.Background(), c, "https://example.invalid", &total); err == nil {
				t.Fatal("expected transfer error")
			}
		})
	}
}

type trackedBody struct {
	reader         io.Reader
	closed         bool
	drained        bool
	readAfterClose bool
}

func (b *trackedBody) Read(p []byte) (int, error) {
	b.readAfterClose = b.closed
	n, err := b.reader.Read(p)
	if err == io.EOF {
		b.drained = true
	}
	return n, err
}
func (b *trackedBody) Close() error { b.closed = true; return nil }

func TestUploadDrainsBeforeClose(t *testing.T) {
	body := &trackedBody{reader: strings.NewReader("ack")}
	c := &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		_, err := io.Copy(io.Discard, r.Body)
		r.Body.Close()
		return response(200, body), err
	})}
	var total atomic.Int64
	if err := upload(context.Background(), c, "https://example.invalid", 32, &total); err != nil {
		t.Fatal(err)
	}
	if !body.closed || !body.drained || body.readAfterClose {
		t.Fatalf("response lifecycle: %+v", body)
	}
	if total.Load() != 32 {
		t.Fatalf("uploaded %d bytes, want 32", total.Load())
	}
}

func TestMeasureBoundsRetries(t *testing.T) {
	for _, dir := range []Direction{Download, Upload} {
		t.Run(map[Direction]string{Download: "download", Upload: "upload"}[dir], func(t *testing.T) {
			var requests atomic.Int64
			c := &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
				requests.Add(1)
				if r.Body != nil {
					io.Copy(io.Discard, r.Body)
					r.Body.Close()
				}
				return response(503, io.NopCloser(strings.NewReader("unavailable"))), nil
			})}
			start := time.Now()
			_, err := measure(context.Background(), c, []Target{{URL: "https://example.invalid"}}, dir, 1, 32, 2*time.Second, make(chan sample, 16))
			if err == nil || !strings.Contains(err.Error(), "3 attempts") {
				t.Fatalf("expected retry exhaustion, got %v", err)
			}
			if requests.Load() != 3 || time.Since(start) < 300*time.Millisecond {
				t.Fatalf("retries were not bounded and delayed: %d requests", requests.Load())
			}
		})
	}
}

func TestMeasureCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	c := &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		close(started)
		<-r.Context().Done()
		return nil, r.Context().Err()
	})}
	go func() { <-started; cancel() }()
	_, err := measure(ctx, c, []Target{{URL: "https://example.invalid"}}, Download, 1, 32, time.Second, make(chan sample))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
}

func TestMeasureRecoversFromTransientFailure(t *testing.T) {
	var requests atomic.Int64
	c := &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		if requests.Add(1) == 1 {
			return response(503, io.NopCloser(strings.NewReader("unavailable"))), nil
		}
		timer := time.NewTimer(10 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-r.Context().Done():
			return nil, r.Context().Err()
		case <-timer.C:
			return response(200, io.NopCloser(strings.NewReader("payload"))), nil
		}
	})}
	res, err := runMeasure(context.Background(), c, []Target{{URL: "https://example.invalid"}}, Download,
		options{conns: 1, bytes: 32, timeout: 500 * time.Millisecond}, true)
	if err != nil {
		t.Fatal(err)
	}
	if res.Bytes <= 0 || res.InstBytesPS <= 0 || requests.Load() < 2 {
		t.Fatalf("measurement did not recover: %+v", res)
	}
}

func TestRunMeasurePropagatesFailure(t *testing.T) {
	c := &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) {
		return response(503, io.NopCloser(strings.NewReader("unavailable"))), nil
	})}
	_, err := runMeasure(context.Background(), c, []Target{{URL: "https://example.invalid"}}, Download,
		options{conns: 1, bytes: 32, timeout: time.Second}, true)
	if err == nil {
		t.Fatal("measurement failure was swallowed")
	}
}

func TestMeasureNoData(t *testing.T) {
	c := &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		<-r.Context().Done()
		return nil, r.Context().Err()
	})}
	_, err := measure(context.Background(), c, []Target{{URL: "https://example.invalid"}}, Download, 1, 32, 10*time.Millisecond, make(chan sample))
	if err == nil {
		t.Fatal("expected no-data failure")
	}
}

func TestLatencyDeadline(t *testing.T) {
	c := &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		deadline, ok := r.Context().Deadline()
		if !ok || time.Until(deadline) > 5*time.Second {
			t.Error("latency probe lacks bounded deadline")
		}
		return response(200, io.NopCloser(strings.NewReader("x"))), nil
	})}
	if _, err := latency(context.Background(), c, Target{URL: "https://example.invalid"}, 3); err != nil {
		t.Fatal(err)
	}
}

type stalledBody struct{ ctx context.Context }

func (b stalledBody) Read([]byte) (int, error) { <-b.ctx.Done(); return 0, b.ctx.Err() }
func (b stalledBody) Close() error             { return nil }

func TestLatencyBodyRespectsCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	c := &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		return response(200, stalledBody{ctx: r.Context()}), nil
	})}
	if _, err := latency(ctx, c, Target{URL: "https://example.invalid"}, 3); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline, got %v", err)
	}
}

func TestLatencyRejectsFailedResponses(t *testing.T) {
	for name, fixture := range map[string]*http.Response{
		"status": response(503, io.NopCloser(strings.NewReader("unavailable"))),
		"body":   response(200, io.NopCloser(brokenReader{})),
	} {
		t.Run(name, func(t *testing.T) {
			c := &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) { return fixture, nil })}
			if _, err := latency(context.Background(), c, Target{URL: "https://example.invalid"}, 1); err == nil {
				t.Fatal("expected latency error")
			}
		})
	}
}

func TestShortMeasurementIncludesTransferredBytes(t *testing.T) {
	var total atomic.Int64
	total.Store(1000)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := sampleUntilStable(ctx, &total, time.Now().Add(-50*time.Millisecond), make(chan sample))
	if result.InstBytesPS <= 0 || result.InstBytesPS != result.CumBytesPS {
		t.Fatalf("short measurement lost bytes: %+v", result)
	}
}

func TestFinalResultIncludesPartialInterval(t *testing.T) {
	var total atomic.Int64
	total.Store(900)
	now := time.Now()
	result := finalResult(&total, now.Add(-300*time.Millisecond), []interval{{bytes: 200, elapsed: 200 * time.Millisecond}}, 200, now.Add(-100*time.Millisecond))
	want := 900 / result.Elapsed.Seconds()
	if math.Abs(result.InstBytesPS-want) > 0.001 {
		t.Fatalf("got %f, want %f", result.InstBytesPS, want)
	}
}

func TestStable(t *testing.T) {
	steady := make([]interval, stabilityWindow)
	for i := range steady {
		steady[i] = interval{bytes: 200, elapsed: 200 * time.Millisecond}
	}
	if !stable(steady) {
		t.Fatal("steady intervals rejected")
	}
	steady[0].bytes = 100
	if stable(steady) {
		t.Fatal("varying intervals accepted")
	}
	if stable(make([]interval, stabilityWindow)) {
		t.Fatal("empty intervals accepted")
	}
}

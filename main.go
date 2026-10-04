package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"
)

type options struct {
	upload  bool
	json    bool
	simple  bool
	avg     bool
	conns   int
	bytes   int64
	timeout time.Duration
}

func (o options) validate() error {
	if o.conns < 1 {
		return fmt.Errorf("-conns must be greater than zero")
	}
	if o.bytes < 1 {
		return fmt.Errorf("-bytes must be greater than zero")
	}
	if o.timeout <= 0 {
		return fmt.Errorf("-timeout must be greater than zero")
	}
	return nil
}

// rate returns the bytes/sec to report for a result: the cumulative average
// with -avg, otherwise the smoothed steady-state rate (matching the live line).
func (o options) rate(r Result) float64 {
	if o.avg {
		return r.CumBytesPS
	}
	return r.InstBytesPS
}

// jsonOutput is the machine-readable result emitted with -json.
type jsonOutput struct {
	DownloadMbps float64 `json:"download_mbps"`
	UploadMbps   float64 `json:"upload_mbps,omitempty"`
	LatencyMs    float64 `json:"latency_ms,omitempty"`
	Client       Client  `json:"client"`
}

func main() {
	var opt options
	flag.BoolVar(&opt.upload, "upload", false, "also measure upload speed")
	flag.BoolVar(&opt.json, "json", false, "emit results as JSON")
	flag.BoolVar(&opt.simple, "simple", false, "print only the download speed in Mbps")
	flag.BoolVar(&opt.avg, "avg", false, "report cumulative average rate instead of steady-state")
	flag.IntVar(&opt.conns, "conns", 5, "number of parallel connections")
	flag.Int64Var(&opt.bytes, "bytes", defaultChunkBytes, "payload size per request in bytes")
	flag.DurationVar(&opt.timeout, "timeout", 30*time.Second, "max duration per measurement")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := run(ctx, opt); err != nil {
		fmt.Fprintln(os.Stderr, "faster:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, opt options) error {
	if err := opt.validate(); err != nil {
		return err
	}
	c := &http.Client{
		Timeout: 0, // per-request deadlines come from ctx
		Transport: &http.Transport{
			MaxIdleConnsPerHost: opt.conns * 2,
			ForceAttemptHTTP2:   true,
		},
	}

	setupCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	token, err := fetchToken(setupCtx, c)
	if err != nil {
		return err
	}
	client, targets, err := fetchTargets(setupCtx, c, token, opt.conns)
	if err != nil {
		return err
	}

	quiet := opt.json || opt.simple

	if !quiet {
		loc := client.Location
		fmt.Fprintf(os.Stderr, "Testing from %s (%s, %s) via %d connections\n\n",
			client.IP, loc.City, loc.Country, opt.conns)
	}

	var lat time.Duration
	if d, err := latency(ctx, c, targets[0], 3); err == nil {
		lat = d
	}

	dl, err := runMeasure(ctx, c, targets, Download, opt, quiet)
	if err != nil {
		return fmt.Errorf("download: %w", err)
	}

	var ul Result
	if opt.upload {
		ul, err = runMeasure(ctx, c, targets, Upload, opt, quiet)
		if err != nil {
			return fmt.Errorf("upload: %w", err)
		}
	}

	return report(opt, client, dl, ul, lat)
}

// runMeasure drives one measurement, rendering a live line unless quiet.
func runMeasure(ctx context.Context, c *http.Client, targets []Target, dir Direction, opt options, quiet bool) (Result, error) {
	progress := make(chan sample, 8)
	var res Result
	errCh := make(chan error, 1)
	go func() {
		var err error
		res, err = measure(ctx, c, targets, dir, opt.conns, opt.bytes, opt.timeout, progress)
		errCh <- err
	}()

	label := "Download"
	if dir == Upload {
		label = "Upload  "
	}
	for s := range progress {
		if !quiet {
			fmt.Fprintf(os.Stderr, "\r%s: %8.2f Mbps", label, mbps(s.BytesPS))
		}
	}
	if err := <-errCh; err != nil {
		if !quiet {
			fmt.Fprintln(os.Stderr)
		}
		return res, err
	}
	if !quiet {
		fmt.Fprintf(os.Stderr, "\r%s: %8.2f Mbps\n", label, mbps(opt.rate(res)))
	}
	return res, nil
}

func report(opt options, client Client, dl, ul Result, lat time.Duration) error {
	switch {
	case opt.json:
		out := jsonOutput{
			DownloadMbps: round2(mbps(opt.rate(dl))),
			Client:       client,
		}
		if opt.upload {
			out.UploadMbps = round2(mbps(opt.rate(ul)))
		}
		if lat > 0 {
			out.LatencyMs = round2(float64(lat.Microseconds()) / 1000)
		}
		enc := json.NewEncoder(os.Stdout)
		return enc.Encode(out)
	case opt.simple:
		fmt.Printf("%.2f\n", mbps(opt.rate(dl)))
		return nil
	default:
		if lat > 0 {
			fmt.Fprintf(os.Stderr, "Latency : %8.2f ms\n", float64(lat.Microseconds())/1000)
		}
		return nil
	}
}

// mbps converts bytes/sec to megabits/sec.
func mbps(bytesPerSec float64) float64 { return bytesPerSec * 8 / 1_000_000 }

func round2(v float64) float64 { return float64(int64(v*100+0.5)) / 100 }

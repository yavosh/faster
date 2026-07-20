package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
)

const (
	fastBase  = "https://fast.com"
	apiBase   = "https://api.fast.com/netflix/speedtest/v2"
	userAgent = "faster/1.0 (+https://github.com/yavosh/faster)"
)

// scriptRe finds the app bundle referenced by the landing page, e.g. /app-a1b2c3.js
var scriptRe = regexp.MustCompile(`app-[0-9a-f]+\.js`)

// tokenRe extracts the API token embedded in the bundle, e.g. token:"YXaby..."
var tokenRe = regexp.MustCompile(`token:"([^"]+)"`)

// fetchToken scrapes the rotating API token out of fast.com's JS bundle.
func fetchToken(ctx context.Context, c *http.Client) (string, error) {
	page, err := getString(ctx, c, fastBase+"/")
	if err != nil {
		return "", fmt.Errorf("load landing page: %w", err)
	}
	m := scriptRe.FindString(page)
	if m == "" {
		return "", fmt.Errorf("could not locate app bundle on landing page")
	}
	bundle, err := getString(ctx, c, fastBase+"/"+m)
	if err != nil {
		return "", fmt.Errorf("load bundle %s: %w", m, err)
	}
	t := tokenRe.FindStringSubmatch(bundle)
	if t == nil {
		return "", fmt.Errorf("could not extract token from bundle %s", m)
	}
	return t[1], nil
}

// Client is the caller info fast.com reports back about you.
type Client struct {
	IP       string   `json:"ip"`
	ASN      string   `json:"asn"`
	Location Location `json:"location"`
}

// Location is a city/country pair for a client or CDN target.
type Location struct {
	City    string `json:"city"`
	Country string `json:"country"`
}

// Target is a single CDN endpoint to measure against.
type Target struct {
	Name     string   `json:"name"`
	URL      string   `json:"url"`
	Location Location `json:"location"`
}

type targetsResponse struct {
	Client  Client   `json:"client"`
	Targets []Target `json:"targets"`
}

// fetchTargets asks the fast.com API for CDN endpoints to measure against.
func fetchTargets(ctx context.Context, c *http.Client, token string, count int) (Client, []Target, error) {
	q := url.Values{
		"https":    {"true"},
		"token":    {token},
		"urlCount": {fmt.Sprint(count)},
	}
	body, err := getBytes(ctx, c, apiBase+"?"+q.Encode())
	if err != nil {
		return Client{}, nil, err
	}
	var tr targetsResponse
	if err := json.Unmarshal(body, &tr); err != nil {
		return Client{}, nil, fmt.Errorf("decode targets: %w", err)
	}
	if len(tr.Targets) == 0 {
		return Client{}, nil, fmt.Errorf("api returned no targets")
	}
	return tr.Client, tr.Targets, nil
}

// rangeURL rewrites a target's /speedtest URL to request a fixed byte range,
// which is how fast.com serves a download payload of a chosen size.
func rangeURL(raw string, bytes int64) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	u.Path = strings.TrimRight(u.Path, "/") + fmt.Sprintf("/range/0-%d", bytes)
	return u.String(), nil
}

func getString(ctx context.Context, c *http.Client, u string) (string, error) {
	b, err := getBytes(ctx, c, u)
	return string(b), err
}

func getBytes(ctx context.Context, c *http.Client, u string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s", u, resp.Status)
	}
	return io.ReadAll(resp.Body)
}

package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestAPIErrorsOmitRequestURL(t *testing.T) {
	for _, transportFailure := range []bool{false, true} {
		c := &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) {
			if transportFailure {
				return nil, context.DeadlineExceeded
			}
			return response(503, io.NopCloser(strings.NewReader("unavailable"))), nil
		})}
		_, err := getBytes(context.Background(), c, "https://example.invalid/private-request-path")
		if err == nil || strings.Contains(err.Error(), "example.invalid") || strings.Contains(err.Error(), "private-request-path") {
			t.Fatal("API error was missing or included the request URL")
		}
		if transportFailure && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("underlying transport error was lost")
		}
	}
}

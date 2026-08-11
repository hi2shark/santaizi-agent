package utls_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	utls "github.com/refraction-networking/utls"

	"github.com/hi2shark/santaizi-agent/pkg/util"
	utlsx "github.com/hi2shark/santaizi-agent/pkg/utls"
)

func TestRoundTripperUsesBackdropAndBrowserHeadersForHTTP(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.UserAgent() == "" {
			t.Error("browser user-agent was not applied")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	headers := util.BrowserHeaders()
	client := &http.Client{Transport: utlsx.NewUTLSHTTPRoundTripperWithProxy(
		utls.HelloChrome_Auto,
		new(utls.Config),
		http.DefaultTransport,
		nil,
		&headers,
	)}
	resp, err := doRequest(client, server.URL)
	if err != nil {
		t.Fatalf("local request failed: %v", err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
}

func doRequest(client *http.Client, url string) (*http.Response, error) {
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return resp, nil
}

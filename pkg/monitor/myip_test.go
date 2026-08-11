package monitor

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFetchIPUsesConfiguredEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("User-Agent") == "" {
			t.Error("missing User-Agent")
		}
		_, _ = response.Write([]byte("fl=example\nip=192.0.2.10\n"))
	}))
	defer server.Close()

	original := httpClientV4
	httpClientV4 = server.Client()
	t.Cleanup(func() { httpClientV4 = original })
	if got := fetchIP([]string{server.URL}, false); got != "192.0.2.10" {
		t.Fatalf("fetchIP()=%q, want 192.0.2.10", got)
	}
}

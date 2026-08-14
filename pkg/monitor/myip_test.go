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
		_, _ = response.Write([]byte("fl=example\nip=192.0.2.10\nloc=HK\n"))
	}))
	defer server.Close()

	original := httpClientV4
	httpClientV4 = server.Client()
	t.Cleanup(func() { httpClientV4 = original })
	got := fetchIP([]string{server.URL}, false)
	if got.IP != "192.0.2.10" {
		t.Fatalf("fetchIP().IP=%q, want 192.0.2.10", got.IP)
	}
	if got.Loc != "HK" {
		t.Fatalf("fetchIP().Loc=%q, want HK", got.Loc)
	}
}

func TestParseCloudflareTrace(t *testing.T) {
	ip, loc := parseCloudflareTrace("fl=x\nip=198.51.100.8\nloc=SG\nwarp=off\n")
	if ip != "198.51.100.8" || loc != "SG" {
		t.Fatalf("ip=%q loc=%q", ip, loc)
	}
	ip, loc = parseCloudflareTrace("ip=198.51.100.8\n")
	if ip != "198.51.100.8" || loc != "" {
		t.Fatalf("missing loc: ip=%q loc=%q", ip, loc)
	}
}

func TestNormalizeTraceLoc(t *testing.T) {
	if got := normalizeTraceLoc(" HK "); got != "hk" {
		t.Fatalf("got %q", got)
	}
	if got := normalizeTraceLoc("xx"); got != "" {
		t.Fatalf("unknown loc=%q", got)
	}
	if got := normalizeTraceLoc("T1"); got != "" {
		t.Fatalf("tor loc=%q", got)
	}
	if got := normalizeTraceLoc("HKG"); got != "" {
		t.Fatalf("iso3=%q", got)
	}
}

func TestApplyPublicAddressesSetsCountryFromLoc(t *testing.T) {
	CachedIP, GeoQueryIP, CachedCountryCode = "", "", ""
	manualCountryCode = false
	t.Cleanup(func() {
		CachedIP, GeoQueryIP, CachedCountryCode = "", "", ""
		manualCountryCode = false
	})
	applyPublicAddresses(publicAddress{IP: "198.51.100.8", Loc: "HK"}, publicAddress{IP: "2001:db8::1", Loc: "JP"}, false)
	if CachedIP != "198.51.100.8/2001:db8::1" {
		t.Fatalf("ip=%q", CachedIP)
	}
	if GeoQueryIP != "198.51.100.8" {
		t.Fatalf("geo query=%q", GeoQueryIP)
	}
	if CachedCountryCode != "hk" {
		t.Fatalf("country=%q", CachedCountryCode)
	}

	applyPublicAddresses(publicAddress{IP: "198.51.100.8", Loc: "HK"}, publicAddress{IP: "2001:db8::1", Loc: "JP"}, true)
	if GeoQueryIP != "2001:db8::1" || CachedCountryCode != "jp" {
		t.Fatalf("ipv6 loc: geo=%q country=%q", GeoQueryIP, CachedCountryCode)
	}
}

func TestApplyPublicAddressesKeepsManualCountry(t *testing.T) {
	ConfigureIPReport("", "CN")
	t.Cleanup(func() {
		ConfigureIPReport("", "")
		CachedIP, GeoQueryIP, CachedCountryCode = "", "", ""
	})
	applyPublicAddresses(publicAddress{IP: "198.51.100.8", Loc: "US"}, publicAddress{}, false)
	if CachedCountryCode != "CN" {
		t.Fatalf("manual country overwritten: %q", CachedCountryCode)
	}
}

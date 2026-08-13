package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ebi-yade/altsvc-go"
	ping "github.com/prometheus-community/pro-bing"
	"github.com/quic-go/quic-go/http3"
	utls "github.com/refraction-networking/utls"

	utlsx "github.com/hi2shark/santaizi-agent/pkg/utls"
	pb "github.com/hi2shark/santaizi-agent/proto"
)

var (
	dnsResolver  = &net.Resolver{PreferGo: true}
	lookupIPAddr = func(ctx context.Context, host string) ([]net.IPAddr, error) {
		return dnsResolver.LookupIPAddr(ctx, host)
	}
	httpProbeClient = &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error { return http.ErrUseLastResponse },
		Timeout:       30 * time.Second,
	}
	http3ProbeClient = &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error { return http.ErrUseLastResponse },
		Timeout:       30 * time.Second,
		Transport:     &http3.Transport{},
	}
)

func initProbeClients() {
	headers := http.Header{
		"Accept":          []string{"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
		"Accept-Language": []string{"en-US,en;q=0.8"},
		"User-Agent":      []string{"Mozilla/5.0 Santaizi-Agent"},
	}
	httpProbeClient.Transport = utlsx.NewUTLSHTTPRoundTripperWithProxy(
		utls.HelloChrome_Auto, new(utls.Config), http.DefaultTransport, nil, &headers,
	)
}

func executeProbe(request *pb.ProbeRequest) *pb.ProbeResult {
	result := &pb.ProbeResult{ProbeId: request.GetProbeId()}
	defer func() { result.CompletedAtUnixNano = time.Now().UnixNano() }()
	if request.GetProbeId() == "" {
		result.Error = "probe_id is required"
		return result
	}
	switch target := request.GetTarget().(type) {
	case *pb.ProbeRequest_Http:
		result.Kind = pb.ProbeKind_PROBE_KIND_HTTP
		if !agentConfig.Capabilities.HTTPProbe {
			result.Error = "HTTP probe capability is disabled"
			return result
		}
		executeHTTPProbe(target.Http, result)
	case *pb.ProbeRequest_Icmp:
		result.Kind = pb.ProbeKind_PROBE_KIND_ICMP
		if !agentConfig.Capabilities.ICMPProbe {
			result.Error = "ICMP probe capability is disabled"
			return result
		}
		executeICMPProbe(target.Icmp, result)
	case *pb.ProbeRequest_Tcp:
		result.Kind = pb.ProbeKind_PROBE_KIND_TCP
		if !agentConfig.Capabilities.TCPProbe {
			result.Error = "TCP probe capability is disabled"
			return result
		}
		executeTCPProbe(target.Tcp, result)
	default:
		result.Error = "unsupported probe kind"
	}
	return result
}

func executeHTTPProbe(request *pb.HTTPProbeRequest, result *pb.ProbeResult) {
	parsed, err := url.ParseRequestURI(request.GetUrl())
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		result.Error = "HTTP probe requires an http or https URL"
		return
	}
	timeout := boundedProbeTimeout(request.GetTimeoutMs(), 30*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		result.Error = err.Error()
		return
	}
	started := time.Now()
	response, err := httpProbeClient.Do(httpRequest)
	result.DelayMs = float64(time.Since(started).Microseconds()) / 1000
	if err != nil {
		result.Error = err.Error()
		return
	}
	defer response.Body.Close()
	_, readErr := io.Copy(io.Discard, response.Body)
	detail := &pb.HTTPProbeResult{StatusCode: uint32(response.StatusCode)}
	if response.TLS != nil && len(response.TLS.PeerCertificates) > 0 {
		certificate := response.TLS.PeerCertificates[0]
		detail.TlsIssuer = certificate.Issuer.CommonName
		detail.TlsExpiresAtUnix = certificate.NotAfter.Unix()
	}
	result.Detail = &pb.ProbeResult_Http{Http: detail}
	if readErr != nil {
		result.Error = readErr.Error()
		return
	}
	if response.StatusCode < 200 || response.StatusCode >= 400 {
		result.Error = response.Status
		return
	}
	if altSvc := response.Header.Get("Alt-Svc"); altSvc != "" {
		if err := validateAlternativeService(ctx, parsed, altSvc); err != nil {
			result.Error = err.Error()
			return
		}
	}
	result.Successful = true
}

func validateAlternativeService(ctx context.Context, original *url.URL, header string) error {
	services, err := altsvc.Parse(header)
	if err != nil {
		return err
	}
	originalPort := original.Port()
	if originalPort == "" {
		if original.Scheme == "https" {
			originalPort = "443"
		} else {
			originalPort = "80"
		}
	}
	for _, service := range services {
		host := service.AltAuthority.Host
		if host == "" {
			host = original.Hostname()
		}
		port := service.AltAuthority.Port
		if port == "" || (host == original.Hostname() && port == originalPort) {
			continue
		}
		alternative := "https://" + net.JoinHostPort(host, port) + "/"
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, alternative, nil)
		if err != nil {
			return err
		}
		request.Host = original.Hostname()
		client := httpProbeClient
		if strings.HasPrefix(service.ProtocolID, "h3") {
			client = http3ProbeClient
		}
		response, err := client.Do(request)
		if err != nil {
			return fmt.Errorf("alternative service: %w", err)
		}
		_, readErr := io.Copy(io.Discard, response.Body)
		closeErr := response.Body.Close()
		if readErr != nil {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		if response.StatusCode < 200 || response.StatusCode >= 400 {
			return errors.New(response.Status)
		}
		break
	}
	return nil
}

func executeICMPProbe(request *pb.ICMPProbeRequest, result *pb.ProbeResult) {
	if strings.TrimSpace(request.GetHost()) == "" {
		result.Error = "ICMP probe host is required"
		return
	}
	ipAddress, err := lookupIP(request.GetHost())
	if err != nil {
		result.Error = err.Error()
		return
	}
	count := request.GetCount()
	if count == 0 {
		count = 5
	}
	if count > 10 {
		count = 10
	}
	pinger, err := ping.NewPinger(ipAddress)
	if err != nil {
		result.Error = err.Error()
		return
	}
	pinger.SetPrivileged(true)
	pinger.Count = int(count)
	pinger.Timeout = boundedProbeTimeout(request.GetTimeoutMs(), 20*time.Second)
	started := time.Now()
	err = pinger.Run()
	result.DelayMs = float64(time.Since(started).Microseconds()) / 1000
	if err != nil {
		result.Error = err.Error()
		return
	}
	statistics := pinger.Statistics()
	result.Detail = &pb.ProbeResult_Icmp{Icmp: &pb.ICMPProbeResult{
		PacketsSent: uint32(statistics.PacketsSent), PacketsReceived: uint32(statistics.PacketsRecv),
	}}
	if statistics.PacketsRecv == 0 {
		result.Error = "no ICMP reply received"
		return
	}
	result.DelayMs = float64(statistics.AvgRtt.Microseconds()) / 1000
	result.Successful = true
}

func executeTCPProbe(request *pb.TCPProbeRequest, result *pb.ProbeResult) {
	if strings.TrimSpace(request.GetHost()) == "" || request.GetPort() == 0 || request.GetPort() > 65535 {
		result.Error = "TCP probe requires a host and valid port"
		return
	}
	ipAddress, err := lookupIP(request.GetHost())
	if err != nil {
		result.Error = err.Error()
		return
	}
	address := net.JoinHostPort(ipAddress, fmt.Sprint(request.GetPort()))
	dialer := net.Dialer{Timeout: boundedProbeTimeout(request.GetTimeoutMs(), 10*time.Second)}
	started := time.Now()
	connection, err := dialer.DialContext(context.Background(), "tcp", address)
	result.DelayMs = float64(time.Since(started).Microseconds()) / 1000
	result.Detail = &pb.ProbeResult_Tcp{Tcp: &pb.TCPProbeResult{ResolvedIp: ipAddress}}
	if err != nil {
		result.Error = err.Error()
		return
	}
	_ = connection.Close()
	result.Successful = true
}

func boundedProbeTimeout(milliseconds uint32, fallback time.Duration) time.Duration {
	if milliseconds == 0 {
		return fallback
	}
	timeout := time.Duration(milliseconds) * time.Millisecond
	if timeout < time.Second {
		return time.Second
	}
	if timeout > time.Minute {
		return time.Minute
	}
	return timeout
}

func lookupIP(hostOrIP string) (string, error) {
	if parsed := net.ParseIP(hostOrIP); parsed != nil {
		return parsed.String(), nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	addresses, err := lookupIPAddr(ctx, hostOrIP)
	if err != nil {
		return "", err
	}
	if len(addresses) == 0 {
		return "", fmt.Errorf("cannot resolve %s", hostOrIP)
	}
	return addresses[0].IP.String(), nil
}

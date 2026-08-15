package pki

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestStoreAtomicReplaceAndPermissions(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	key, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	node := make([]byte, 16)
	for i := range node {
		node[i] = byte(i + 1)
	}
	csr, err := CreateCSR(key, node)
	if err != nil {
		t.Fatal(err)
	}
	if len(csr) == 0 {
		t.Fatal("empty csr")
	}
	certPEM := selfSignedClientCert(t, key)
	cert, err := ParseCertificatePEM(certPEM)
	if err != nil {
		t.Fatal(err)
	}
	bundle := &Bundle{Key: key, Cert: cert, CertPEM: certPEM, CAPEM: certPEM}
	if err := store.Save(bundle); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if string(loaded.CertPEM) != string(bundle.CertPEM) {
		t.Fatal("bundle mismatch")
	}
	info, err := os.Stat(filepath.Join(store.Dir(), clientKeyName))
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0600 {
		t.Fatalf("key perm=%o", info.Mode().Perm())
	}
}

func TestNeedsRenewAndExpiry(t *testing.T) {
	now := time.Now()
	bundle := &Bundle{Cert: &x509.Certificate{NotAfter: now.Add(30 * 24 * time.Hour)}}
	if bundle.NeedsRenew(now, DefaultRenewWindow) || bundle.Expired(now) {
		t.Fatal("fresh cert")
	}
	if !bundle.NeedsRenew(bundle.Cert.NotAfter.Add(-time.Hour), DefaultRenewWindow) {
		t.Fatal("renew window")
	}
	if !bundle.Expired(bundle.Cert.NotAfter.Add(time.Second)) {
		t.Fatal("expired")
	}
}

func TestIsLegacyPanelOnlyUnimplemented(t *testing.T) {
	if !IsLegacyPanel(status.Error(codes.Unimplemented, "no")) {
		t.Fatal("unimplemented")
	}
	for _, code := range []codes.Code{codes.Unavailable, codes.Unauthenticated, codes.PermissionDenied, codes.Internal, codes.Unknown} {
		if IsLegacyPanel(status.Error(code, "no")) {
			t.Fatalf("code %v must not fallback", code)
		}
	}
}

func TestNewClientTLSConfigKeepsSystemRoots(t *testing.T) {
	cfg, err := NewClientTLSConfig(ClientTLSOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RootCAs == nil || cfg.MinVersion != tlsVersion12 {
		t.Fatalf("tls config=%#v", cfg)
	}
}

func TestNewClientTLSConfigSetsServerName(t *testing.T) {
	cfg, err := NewClientTLSConfig(ClientTLSOptions{ServerName: "grpc.example.invalid"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ServerName != "grpc.example.invalid" {
		t.Fatalf("server name = %q", cfg.ServerName)
	}
}

const tlsVersion12 = 0x0303

func selfSignedClientCert(t *testing.T, key ed25519.PrivateKey) []byte {
	t.Helper()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

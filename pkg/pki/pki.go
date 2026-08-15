package pki

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/hi2shark/santaizi-agent/pkg/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	AgentURIPrefix     = "urn:santaizi:agent:"
	DefaultRenewWindow = 7 * 24 * time.Hour
	clientKeyName      = "client.key"
	clientCertName     = "client.crt"
	clientCAName       = "ca.crt"
	clientKeyFileMode  = 0600
	clientCertFileMode = 0644
)

var ErrNotFound = errors.New("client certificate bundle not found")

func EncodeAgentURI(nodeUUID []byte) string {
	return AgentURIPrefix + hex.EncodeToString(nodeUUID)
}

func GenerateKey() (ed25519.PrivateKey, error) {
	_, private, err := ed25519.GenerateKey(rand.Reader)
	return private, err
}

func CreateCSR(private ed25519.PrivateKey, nodeUUID []byte) ([]byte, error) {
	if len(private) == 0 {
		return nil, errors.New("private key is required")
	}
	uri := EncodeAgentURI(nodeUUID)
	parsed, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	return x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: uri},
		URIs:    []*url.URL{parsed},
	}, private)
}

func MarshalPrivateKeyPEM(private ed25519.PrivateKey) ([]byte, error) {
	der, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), nil
}

func ParsePrivateKeyPEM(pemBytes []byte) (ed25519.PrivateKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("private key PEM is required")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	private, ok := parsed.(ed25519.PrivateKey)
	if !ok {
		return nil, errors.New("private key must be Ed25519")
	}
	return private, nil
}

func ParseCertificatePEM(pemBytes []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, errors.New("certificate PEM is required")
	}
	return x509.ParseCertificate(block.Bytes)
}

type Bundle struct {
	Key     ed25519.PrivateKey
	Cert    *x509.Certificate
	CertPEM []byte
	CAPEM   []byte
}

func (b *Bundle) NeedsRenew(now time.Time, window time.Duration) bool {
	if b == nil || b.Cert == nil {
		return true
	}
	if window <= 0 {
		window = DefaultRenewWindow
	}
	return !now.Before(b.Cert.NotAfter.Add(-window))
}

func (b *Bundle) Expired(now time.Time) bool {
	return b == nil || b.Cert == nil || !now.Before(b.Cert.NotAfter)
}

type Store struct {
	mu  sync.RWMutex
	dir string
}

func Open(dir string) (*Store, error) {
	if dir == "" {
		return nil, errors.New("pki directory is empty")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	return &Store{dir: dir}, nil
}

func (s *Store) Dir() string { return s.dir }

func (s *Store) Load() (*Bundle, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.loadLocked()
}

func (s *Store) loadLocked() (*Bundle, error) {
	keyPEM, err := os.ReadFile(filepath.Join(s.dir, clientKeyName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	certPEM, err := os.ReadFile(filepath.Join(s.dir, clientCertName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	caPEM, err := os.ReadFile(filepath.Join(s.dir, clientCAName))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	key, err := ParsePrivateKeyPEM(keyPEM)
	if err != nil {
		return nil, err
	}
	cert, err := ParseCertificatePEM(certPEM)
	if err != nil {
		return nil, err
	}
	return &Bundle{Key: key, Cert: cert, CertPEM: certPEM, CAPEM: caPEM}, nil
}

func (s *Store) Save(bundle *Bundle) error {
	if bundle == nil || len(bundle.Key) == 0 || len(bundle.CertPEM) == 0 {
		return errors.New("client certificate bundle is incomplete")
	}
	keyPEM, err := MarshalPrivateKeyPEM(bundle.Key)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := writeAtomic(filepath.Join(s.dir, clientKeyName), keyPEM, clientKeyFileMode); err != nil {
		return err
	}
	if err := writeAtomic(filepath.Join(s.dir, clientCertName), bundle.CertPEM, clientCertFileMode); err != nil {
		return err
	}
	if len(bundle.CAPEM) > 0 {
		if err := writeAtomic(filepath.Join(s.dir, clientCAName), bundle.CAPEM, clientCertFileMode); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) CAPEM() []byte {
	bundle, err := s.Load()
	if err != nil || bundle == nil {
		return nil
	}
	return bundle.CAPEM
}

func (s *Store) HasCertificate() bool {
	bundle, err := s.Load()
	return err == nil && bundle != nil && !bundle.Expired(time.Now())
}

func (s *Store) GetClientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cert, err := tls.LoadX509KeyPair(filepath.Join(s.dir, clientCertName), filepath.Join(s.dir, clientKeyName))
	if errors.Is(err, os.ErrNotExist) {
		return &tls.Certificate{}, nil
	}
	if err != nil {
		return nil, err
	}
	return &cert, nil
}

type ClientTLSOptions struct {
	CAFile               string
	ExtraCAPEM           []byte
	ServerName           string
	InsecureSkipVerify   bool
	GetClientCertificate func(*tls.CertificateRequestInfo) (*tls.Certificate, error)
}

func NewClientTLSConfig(opts ClientTLSOptions) (*tls.Config, error) {
	roots, err := x509.SystemCertPool()
	if err != nil || roots == nil {
		roots = x509.NewCertPool()
	}
	if len(opts.ExtraCAPEM) > 0 && !roots.AppendCertsFromPEM(opts.ExtraCAPEM) {
		return nil, errors.New("extra CA PEM contains no certificates")
	}
	if opts.CAFile != "" {
		pemBytes, err := os.ReadFile(opts.CAFile) // #nosec G304 -- operator-configured CA file
		if err != nil {
			return nil, fmt.Errorf("read tls.ca_file: %w", err)
		}
		if !roots.AppendCertsFromPEM(pemBytes) {
			return nil, errors.New("tls.ca_file contains no certificates")
		}
	}
	return &tls.Config{
		MinVersion:           tls.VersionTLS12,
		ServerName:           opts.ServerName,
		RootCAs:              roots,
		InsecureSkipVerify:   opts.InsecureSkipVerify, //nolint:gosec
		GetClientCertificate: opts.GetClientCertificate,
	}, nil
}

func IsLegacyPanel(err error) bool {
	return status.Code(err) == codes.Unimplemented
}

type EnrollmentCredential struct {
	ClientSecret string
}

func (e *EnrollmentCredential) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{"client_secret": e.ClientSecret}, nil
}

func (e *EnrollmentCredential) RequireTransportSecurity() bool { return true }

func writeAtomic(path string, data []byte, perm os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".pki-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(perm); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	if runtime.GOOS == "windows" {
		return nil
	}
	return util.SyncDir(filepath.Dir(path))
}

package serverutil

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// testPKI 是测试用自签 PKI：CA（自签）+ 由 CA 签发的服务端与客户端证书。
type testPKI struct {
	caCert     string
	serverCert string
	serverKey  string
	clientCert string
	clientKey  string
}

// newTestPKI 生成测试 PKI（服务端与客户端证书均由 CA 签发，含 127.0.0.1 SAN）。
func newTestPKI(t *testing.T) testPKI {
	t.Helper()
	dir := t.TempDir()
	caKey := newECKey(t)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "atlas-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER := signCert(t, caTmpl, caTmpl, &caKey.PublicKey, caKey)

	serverKey := newECKey(t)
	serverTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "atlas-test-server"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	serverDER := signCert(t, serverTmpl, caTmpl, &serverKey.PublicKey, caKey)

	clientKey := newECKey(t)
	clientTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "atlas-test-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientDER := signCert(t, clientTmpl, caTmpl, &clientKey.PublicKey, caKey)

	pki := testPKI{
		caCert:     filepath.Join(dir, "ca.crt"),
		serverCert: filepath.Join(dir, "server.crt"),
		serverKey:  filepath.Join(dir, "server.key"),
		clientCert: filepath.Join(dir, "client.crt"),
		clientKey:  filepath.Join(dir, "client.key"),
	}
	writePEM(t, pki.caCert, "CERTIFICATE", caDER)
	writePEM(t, pki.serverCert, "CERTIFICATE", serverDER)
	writePEM(t, pki.serverKey, "EC PRIVATE KEY", marshalKey(t, serverKey))
	writePEM(t, pki.clientCert, "CERTIFICATE", clientDER)
	writePEM(t, pki.clientKey, "EC PRIVATE KEY", marshalKey(t, clientKey))
	return pki
}

// newECKey 生成 P-256 私钥。
func newECKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("生成私钥失败: %v", err)
	}
	return key
}

// signCert 用 caKey 签发 tmpl 的 DER 证书。
func signCert(t *testing.T, tmpl, caTmpl *x509.Certificate, pub *ecdsa.PublicKey, caKey *ecdsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caTmpl, pub, caKey)
	if err != nil {
		t.Fatalf("签发证书失败: %v", err)
	}
	return der
}

// marshalKey 编码 EC 私钥。
func marshalKey(t *testing.T, key *ecdsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("编码私钥失败: %v", err)
	}
	return der
}

// caPool 读取 CA 证书构造信任池。
func caPool(t *testing.T, caCert string) *x509.CertPool {
	t.Helper()
	pem, err := os.ReadFile(caCert)
	if err != nil {
		t.Fatalf("读取 CA 失败: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		t.Fatal("CA 证书无效")
	}
	return pool
}

// TestHTTPTLSHandshake 验证启用 TLS 后真实握手成功（自签 CA 信任链）。
func TestHTTPTLSHandshake(t *testing.T) {
	pki := newTestPKI(t)
	srv, err := HTTPServer(&configspb.Server_HTTP{
		Addr: "127.0.0.1:0",
		Tls:  &configspb.Server_TLS{Enabled: true, CertFile: pki.serverCert, KeyFile: pki.serverKey},
	}, nil, nil)
	if err != nil {
		t.Fatalf("构造 HTTPS 服务端失败: %v", err)
	}
	srv.HandleFunc("/health", HealthHandler("game"))
	ep := startHTTP(t, srv)
	if ep.Scheme != "https" {
		t.Fatalf("端点 scheme = %q，期望 https", ep.Scheme)
	}

	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: caPool(t, pki.caCert), MinVersion: tls.VersionTLS12},
	}}
	resp, err := client.Get(ep.String() + "/health")
	if err != nil {
		t.Fatalf("HTTPS 请求失败: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200", resp.StatusCode)
	}
}

// TestHTTPMutualTLS 验证 client_auth 下无客户端证书被拒、带证书通过。
func TestHTTPMutualTLS(t *testing.T) {
	pki := newTestPKI(t)
	srv, err := HTTPServer(&configspb.Server_HTTP{
		Addr: "127.0.0.1:0",
		Tls: &configspb.Server_TLS{
			Enabled: true, CertFile: pki.serverCert, KeyFile: pki.serverKey,
			CaFile: pki.caCert, ClientAuth: true,
		},
	}, nil, nil)
	if err != nil {
		t.Fatalf("构造 mTLS 服务端失败: %v", err)
	}
	srv.HandleFunc("/health", HealthHandler("game"))
	ep := startHTTP(t, srv)

	// 不带客户端证书：握手必须失败。
	noCert := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: caPool(t, pki.caCert), MinVersion: tls.VersionTLS12},
	}}
	if resp, err := noCert.Get(ep.String() + "/health"); err == nil {
		_ = resp.Body.Close()
		t.Fatal("未带客户端证书的请求应握手失败，实际成功")
	}

	// 带 CA 签发的客户端证书：握手通过。
	clientCert, err := tls.LoadX509KeyPair(pki.clientCert, pki.clientKey)
	if err != nil {
		t.Fatalf("加载客户端证书失败: %v", err)
	}
	withCert := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs:      caPool(t, pki.caCert),
			Certificates: []tls.Certificate{clientCert},
			MinVersion:   tls.VersionTLS12,
		},
	}}
	resp, err := withCert.Get(ep.String() + "/health")
	if err != nil {
		t.Fatalf("带客户端证书的请求失败: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200", resp.StatusCode)
	}
}

// TestGRPCTLSHandshake 验证 gRPC 侧 TLS 握手并真实调用 health 服务。
func TestGRPCTLSHandshake(t *testing.T) {
	pki := newTestPKI(t)
	opts, err := GRPCOptions(&configspb.Server_GRPC{
		Addr: "127.0.0.1:0",
		Tls:  &configspb.Server_TLS{Enabled: true, CertFile: pki.serverCert, KeyFile: pki.serverKey},
	})
	if err != nil {
		t.Fatalf("GRPCOptions() 错误 = %v", err)
	}
	srv, err := atlasgrpc.NewServer(opts...)
	if err != nil {
		t.Fatalf("构造 gRPC 服务端失败: %v", err)
	}
	go func() { _ = srv.Start(context.Background()) }()
	ep, err := WaitEndpoint(srv, 5*time.Second)
	if err != nil {
		t.Fatalf("等待 gRPC 服务端就绪失败: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop(context.Background()) })

	conn, err := grpc.NewClient(ep.Host, grpc.WithTransportCredentials(
		credentials.NewTLS(&tls.Config{RootCAs: caPool(t, pki.caCert), MinVersion: tls.VersionTLS12})))
	if err != nil {
		t.Fatalf("建立 gRPC 连接失败: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("TLS 下调用 health 失败: %v", err)
	}
	if resp.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("health 状态 = %v，期望 SERVING", resp.GetStatus())
	}
}

// writePEM 把 DER 数据以指定块类型编码写入 path（测试 PKI 夹具共用）。
func writePEM(t *testing.T, path, blockType string, der []byte) {
	t.Helper()
	buf := pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der})
	if err := os.WriteFile(path, buf, 0o600); err != nil {
		t.Fatalf("写入 %s 失败: %v", path, err)
	}
}

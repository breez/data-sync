package middleware

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/breez/data-sync/config"
	"github.com/breez/data-sync/proto"
	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/ecdsa"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/tv42/zbase32"
	"google.golang.org/grpc/metadata"
)

const (
	USER_PUBKEY_CONTEXT_KEY = "user_pubkey"
)

var ErrInternalError = fmt.Errorf("internal error")
var ErrInvalidSignature = fmt.Errorf("invalid signature")
var SignedMsgPrefix = []byte("realtimesync:")

func checkApiKey(config *config.Config, ctx context.Context, req interface{}) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return fmt.Errorf("Could not read request metadata")
	}

	authHeader := md.Get("Authorization")[0]
	if len(authHeader) <= 7 || !strings.HasPrefix(authHeader, "Bearer ") {
		return fmt.Errorf("Invalid auth header")
	}

	apiKey := authHeader[7:]
	block, err := base64.StdEncoding.DecodeString(apiKey)
	if err != nil {
		return fmt.Errorf("Could not decode auth header: %v", err)
	}

	cert, err := x509.ParseCertificate(block)
	if err != nil {
		return fmt.Errorf("Could not parse certificate: %v", err)
	}

	rootPool := x509.NewCertPool()
	rootPool.AddCert(config.CACert)

	chains, err := cert.Verify(x509.VerifyOptions{
		Roots: rootPool,
	})
	if err != nil {
		return fmt.Errorf("Certificate verification error: %v", err)
	}
	if len(chains) != 1 || len(chains[0]) != 2 || !chains[0][0].Equal(cert) || !chains[0][1].Equal(config.CACert) {
		return fmt.Errorf("Certificate verification error: invalid chain of trust")
	}

	return nil
}

func Authenticate(config *config.Config, ctx context.Context, req interface{}) (context.Context, error) {
	if err := checkApiKey(config, ctx, req); err != nil {
		return nil, err
	}

	var toVerify string
	var signature string
	setRecordReq, ok := req.(*proto.SetRecordRequest)
	if ok {
		toVerify = SignSetRecord(setRecordReq.Record, setRecordReq.RequestTime)
		signature = setRecordReq.Signature
	}

	listChangesReq, ok := req.(*proto.ListChangesRequest)
	if ok {
		toVerify = fmt.Sprintf("%v-%v", listChangesReq.SinceRevision, listChangesReq.RequestTime)
		signature = listChangesReq.Signature
	}

	trackChangesReq, ok := req.(*proto.TrackChangesRequest)
	if ok {
		toVerify = fmt.Sprintf("%v", trackChangesReq.RequestTime)
		signature = trackChangesReq.Signature
	}

	pubkey, err := VerifyMessage([]byte(toVerify), signature)
	if err != nil {
		return nil, err
	}

	pubkeyBytes := pubkey.SerializeCompressed()
	newContext := context.WithValue(ctx, USER_PUBKEY_CONTEXT_KEY, hex.EncodeToString(pubkeyBytes))
	return newContext, nil
}

func SignSetRecord(record *proto.Record, requestTime uint32) string {
	return fmt.Sprintf(
		"%v-%x-%v-%v-%v",
		record.Id,
		record.Data,
		record.Revision,
		record.SchemaVersion,
		requestTime,
	)
}

func SignMessage(key *btcec.PrivateKey, msg []byte) (string, error) {
	message := append(SignedMsgPrefix, msg...)
	digest := chainhash.DoubleHashB(message)
	signture, err := ecdsa.SignCompact(key, digest, true)
	if err != nil {
		return "", fmt.Errorf("failed to sign message: %v", err)
	}
	sig := zbase32.EncodeToString(signture)
	return sig, nil
}

func VerifyMessage(message []byte, signature string) (*btcec.PublicKey, error) {
	// The signature should be zbase32 encoded
	sig, err := zbase32.DecodeString(signature)
	if err != nil {
		return nil, fmt.Errorf("failed to decode signature: %v", err)
	}

	msg := append(SignedMsgPrefix, message...)
	first := sha256.Sum256(msg)
	second := sha256.Sum256(first[:])
	pubkey, wasCompressed, err := ecdsa.RecoverCompact(
		sig,
		second[:],
	)
	if err != nil {
		return nil, ErrInvalidSignature
	}

	if !wasCompressed {
		return nil, ErrInvalidSignature
	}

	return pubkey, nil
}

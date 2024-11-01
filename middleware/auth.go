package middleware

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"

	"github.com/breez/data-sync/config"
	"github.com/breez/data-sync/proto"
	"github.com/breez/data-sync/store"
	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/ecdsa"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/tv42/zbase32"
)

const (
	USER_DB_CONTEXT_KEY     = "user_db"
	USER_PUBKEY_CONTEXT_KEY = "user_pubkey"
)

var ErrInternalError = fmt.Errorf("internal error")
var ErrInvalidSignature = fmt.Errorf("invalid signature")
var SignedMsgPrefix = []byte("Lightning Signed Message:")

func Authenticate(config *config.Config, ctx context.Context, req interface{}) (context.Context, error) {
	var toVerify string
	var signature string
	setRecordReq, ok := req.(*proto.SetRecordRequest)
	if ok {
		toVerify = fmt.Sprintf("%v-%x-%v-%v", setRecordReq.Record.Id, setRecordReq.Record.Data, setRecordReq.Record.Revision, setRecordReq.RequestTime)
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

	dbDir := config.UsersDatabasesDir
	pubkeyBytes := pubkey.SerializeCompressed()
	storeFile := fmt.Sprintf("%v/%v/%v/%v", dbDir,
		hex.EncodeToString(pubkeyBytes[0:1]),
		hex.EncodeToString(pubkeyBytes[1:2]),
		hex.EncodeToString(pubkeyBytes[2:]))
	db, err := store.Connect(storeFile)
	if err != nil {
		log.Printf("failed to connect to database file %v: %v", storeFile, err)
		return nil, ErrInternalError
	}
	newContext := context.WithValue(ctx, USER_DB_CONTEXT_KEY, db)
	newContext = context.WithValue(newContext, USER_PUBKEY_CONTEXT_KEY, hex.EncodeToString(pubkeyBytes))
	return newContext, nil
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

package middleware

import (
	"testing"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/stretchr/testify/require"
)

func TestSignVerify(t *testing.T) {
	privateKey, err := btcec.NewPrivateKey()
	require.NoError(t, err, "failed to create private key")
	pubkey := privateKey.PubKey().SerializeCompressed()
	message := []byte("test message")
	signature, err := SignMessage(privateKey, message)
	require.NoError(t, err, "failed to sign message")
	recoveredKey, err := VerifyMessage(message, signature)
	require.NoError(t, err, "failed to verify message")
	require.Equal(t, recoveredKey.SerializeCompressed(), pubkey)
}

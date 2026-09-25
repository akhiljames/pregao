package vault

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestOpenBaoTransitClient_DecryptSingle(t *testing.T) {
	expectedPlaintext := "my-secret-binance-key"
	encodedPlaintext := base64.StdEncoding.EncodeToString([]byte(expectedPlaintext))

	mockTransport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		assert.Equal(t, http.MethodPost, req.Method)
		assert.Equal(t, "/v1/transit/decrypt/broker-keys", req.URL.Path)
		assert.Equal(t, "test-token", req.Header.Get("X-Vault-Token"))

		var dReq decryptRequest
		err := json.NewDecoder(req.Body).Decode(&dReq)
		assert.NoError(t, err)
		assert.Equal(t, "vault:v1:someciphertext", dReq.Ciphertext)

		respData := decryptResponse{}
		respData.Data.Plaintext = encodedPlaintext
		bodyBytes, _ := json.Marshal(respData)

		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader(bodyBytes)),
			Header:     make(http.Header),
		}, nil
	})

	httpClient := &http.Client{Transport: mockTransport}
	client := NewTransitClientWithHTTPClient("http://openbao:8200", "test-token", "broker-keys", httpClient)

	decryptedBytes, err := client.Decrypt(context.Background(), "vault:v1:someciphertext")
	require.NoError(t, err)
	assert.Equal(t, []byte(expectedPlaintext), decryptedBytes)
}

func TestOpenBaoTransitClient_DecryptCredentials_Batch(t *testing.T) {
	keyPlain := "binance-api-key-12345"
	secPlain := "binance-api-secret-67890"

	mockTransport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		var bReq batchDecryptRequest
		err := json.NewDecoder(req.Body).Decode(&bReq)
		assert.NoError(t, err)
		assert.Len(t, bReq.BatchInput, 2)

		respData := batchDecryptResponse{}
		respData.Data.BatchResults = []batchDecryptResultItem{
			{Plaintext: base64.StdEncoding.EncodeToString([]byte(keyPlain))},
			{Plaintext: base64.StdEncoding.EncodeToString([]byte(secPlain))},
		}
		bodyBytes, _ := json.Marshal(respData)

		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader(bodyBytes)),
			Header:     make(http.Header),
		}, nil
	})

	httpClient := &http.Client{Transport: mockTransport}
	client := NewTransitClientWithHTTPClient("http://openbao:8200", "test-token", "broker-keys", httpClient)

	creds, err := client.DecryptCredentials(context.Background(), "vault:v1:key", "vault:v1:sec")
	require.NoError(t, err)
	require.NotNil(t, creds)

	assert.Equal(t, []byte(keyPlain), creds.APIKey)
	assert.Equal(t, []byte(secPlain), creds.APISecret)

	// Test zeroing
	creds.Zero()
	assert.Nil(t, creds.APIKey)
	assert.Nil(t, creds.APISecret)
}

func TestPlaintextCredentials_ZeroMemory(t *testing.T) {
	key := []byte("secret_api_key_bytes")
	secret := []byte("secret_api_secret_bytes")

	creds := &PlaintextCredentials{
		APIKey:    key,
		APISecret: secret,
	}

	assert.Equal(t, "secret_api_key_bytes", string(creds.APIKey))

	creds.Zero()

	assert.Nil(t, creds.APIKey)
	assert.Nil(t, creds.APISecret)

	for _, b := range key {
		assert.Equal(t, byte(0), b, "Byte was not wiped with 0")
	}
	for _, b := range secret {
		assert.Equal(t, byte(0), b, "Byte was not wiped with 0")
	}
}

func TestPlaintextCredentials_Redaction(t *testing.T) {
	creds := PlaintextCredentials{
		APIKey:    []byte("plain-key"),
		APISecret: []byte("plain-secret"),
	}

	assert.Equal(t, "[REDACTED_BROKER_CREDENTIALS]", creds.String())
	assert.Equal(t, "[REDACTED_BROKER_CREDENTIALS]", creds.GoString())
	assert.Equal(t, "[REDACTED_BROKER_CREDENTIALS]", fmt.Sprintf("%v", creds))
	assert.Equal(t, "[REDACTED_BROKER_CREDENTIALS]", fmt.Sprintf("%#v", creds))
}

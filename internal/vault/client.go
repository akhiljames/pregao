package vault

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

var (
	ErrDecryptionFailed = errors.New("openbao transit decryption failed")
	ErrEmptyCiphertext  = errors.New("ciphertext cannot be empty")
)

// PlaintextCredentials holds decrypted API key and secret as raw byte slices.
// MUST be zeroed immediately after use using Zero().
type PlaintextCredentials struct {
	APIKey    []byte
	APISecret []byte
}

// Zero securely wipes the credentials from memory.
func (p *PlaintextCredentials) Zero() {
	if p == nil {
		return
	}
	if p.APIKey != nil {
		for i := range p.APIKey {
			p.APIKey[i] = 0
		}
		p.APIKey = nil
	}
	if p.APISecret != nil {
		for i := range p.APISecret {
			p.APISecret[i] = 0
		}
		p.APISecret = nil
	}
}

// String masks sensitive credentials to prevent accidental logging.
func (p PlaintextCredentials) String() string {
	return "[REDACTED_BROKER_CREDENTIALS]"
}

// GoString masks sensitive credentials for %#v formatting.
func (p PlaintextCredentials) GoString() string {
	return "[REDACTED_BROKER_CREDENTIALS]"
}

// TransitClient defines the interface for OpenBao Transit cryptographic operations.
type TransitClient interface {
	Decrypt(ctx context.Context, ciphertext string) ([]byte, error)
	DecryptCredentials(ctx context.Context, apiKeyCiphertext, apiSecretCiphertext string) (*PlaintextCredentials, error)
}

type openBaoTransitClient struct {
	baseURL    string
	keyName    string
	token      string
	httpClient *http.Client
}

// NewTransitClient returns a new TransitClient configured for an OpenBao instance.
func NewTransitClient(addr, token, keyName string, timeout time.Duration) TransitClient {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return NewTransitClientWithHTTPClient(addr, token, keyName, &http.Client{Timeout: timeout})
}

// NewTransitClientWithHTTPClient returns a TransitClient using a custom http.Client.
func NewTransitClientWithHTTPClient(addr, token, keyName string, httpClient *http.Client) TransitClient {
	if keyName == "" {
		keyName = "broker-keys"
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 5 * time.Second}
	}
	return &openBaoTransitClient{
		baseURL:    strings.TrimRight(addr, "/"),
		keyName:    keyName,
		token:      token,
		httpClient: httpClient,
	}
}

type decryptRequest struct {
	Ciphertext string `json:"ciphertext"`
}

type decryptResponse struct {
	Data struct {
		Plaintext string `json:"plaintext"`
	} `json:"data"`
	Errors []string `json:"errors"`
}

type batchDecryptItem struct {
	Ciphertext string `json:"ciphertext"`
}

type batchDecryptRequest struct {
	BatchInput []batchDecryptItem `json:"batch_input"`
}

type batchDecryptResultItem struct {
	Plaintext string `json:"plaintext"`
	Error     string `json:"error"`
}

type batchDecryptResponse struct {
	Data struct {
		BatchResults []batchDecryptResultItem `json:"batch_results"`
	} `json:"data"`
	Errors []string `json:"errors"`
}

// Decrypt sends a single ciphertext to the OpenBao Transit API and decodes the base64 plaintext.
func (c *openBaoTransitClient) Decrypt(ctx context.Context, ciphertext string) ([]byte, error) {
	if strings.TrimSpace(ciphertext) == "" {
		return nil, ErrEmptyCiphertext
	}

	url := fmt.Sprintf("%s/v1/transit/decrypt/%s", c.baseURL, c.keyName)
	reqBody, err := json.Marshal(decryptRequest{Ciphertext: ciphertext})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal decrypt request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create http request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("X-Vault-Token", c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("openbao request failed: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read openbao response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: status %d body: %s", ErrDecryptionFailed, resp.StatusCode, string(bodyBytes))
	}

	var dResp decryptResponse
	if err := json.Unmarshal(bodyBytes, &dResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal openbao response: %w", err)
	}

	if len(dResp.Errors) > 0 {
		return nil, fmt.Errorf("%w: %s", ErrDecryptionFailed, strings.Join(dResp.Errors, "; "))
	}

	decoded, err := base64.StdEncoding.DecodeString(dResp.Data.Plaintext)
	if err != nil {
		return nil, fmt.Errorf("failed to base64 decode plaintext from openbao: %w", err)
	}

	return decoded, nil
}

// DecryptCredentials decrypts both API key and secret, using batch request if available,
// falling back to individual calls. Returned struct MUST be zeroed with defer creds.Zero().
func (c *openBaoTransitClient) DecryptCredentials(ctx context.Context, apiKeyCiphertext, apiSecretCiphertext string) (*PlaintextCredentials, error) {
	// Attempt batch decryption first
	url := fmt.Sprintf("%s/v1/transit/decrypt/%s", c.baseURL, c.keyName)
	bReq := batchDecryptRequest{
		BatchInput: []batchDecryptItem{
			{Ciphertext: apiKeyCiphertext},
			{Ciphertext: apiSecretCiphertext},
		},
	}

	reqBody, err := json.Marshal(bReq)
	if err == nil {
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(reqBody))
		if reqErr == nil {
			req.Header.Set("Content-Type", "application/json")
			if c.token != "" {
				req.Header.Set("X-Vault-Token", c.token)
			}

			resp, doErr := c.httpClient.Do(req)
			if doErr == nil {
				defer resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					var bResp batchDecryptResponse
					if decErr := json.NewDecoder(resp.Body).Decode(&bResp); decErr == nil && len(bResp.Data.BatchResults) == 2 {
						rKey := bResp.Data.BatchResults[0]
						rSec := bResp.Data.BatchResults[1]
						if rKey.Error == "" && rSec.Error == "" {
							keyBytes, err1 := base64.StdEncoding.DecodeString(rKey.Plaintext)
							secBytes, err2 := base64.StdEncoding.DecodeString(rSec.Plaintext)
							if err1 == nil && err2 == nil {
								return &PlaintextCredentials{
									APIKey:    keyBytes,
									APISecret: secBytes,
								}, nil
							}
						}
					}
				}
			}
		}
	}

	// Fallback to individual decryptions
	keyBytes, err := c.Decrypt(ctx, apiKeyCiphertext)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt api key: %w", err)
	}

	secBytes, err := c.Decrypt(ctx, apiSecretCiphertext)
	if err != nil {
		// Securely zero out keyBytes if secret decryption failed
		for i := range keyBytes {
			keyBytes[i] = 0
		}
		return nil, fmt.Errorf("failed to decrypt api secret: %w", err)
	}

	return &PlaintextCredentials{
		APIKey:    keyBytes,
		APISecret: secBytes,
	}, nil
}

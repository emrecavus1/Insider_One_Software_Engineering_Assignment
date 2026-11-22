// The task of this file is to generate stable SHA-256 request fingerprints
package notification

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

// FingerprintSingle generates a stable hash for a validated individual notification request.
func FingerprintSingle(request CreateRequest) (string, error) {
	normalized, err := request.NormalizeAndValidate()
	if err != nil {
		return "", err
	}

	payload := struct {
		Kind         string        `json:"kind"`
		Notification CreateRequest `json:"notification"`
	}{
		Kind:         "single",
		Notification: normalized,
	}

	return hashPayload(payload)
}

// FingerprintBatch generates a stable hash for a batch of notification requests
func FingerprintBatch(requests []CreateRequest) (string, error) {
	if len(requests) == 0 || len(requests) > 1000 {
		return "", fmt.Errorf(
			"batch must contain between 1 and 1000 notifications",
		)
	}

	normalized := make([]CreateRequest, len(requests))

	for i, request := range requests {
		result, err := request.NormalizeAndValidate()
		if err != nil {
			return "", fmt.Errorf("notification %d: %w", i+1, err)
		}

		normalized[i] = result
	}

	payload := struct {
		Kind          string          `json:"kind"`
		Notifications []CreateRequest `json:"notifications"`
	}{
		Kind:          "batch",
		Notifications: normalized,
	}

	return hashPayload(payload)
}

func hashPayload(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("marshal request fingerprint: %w", err)
	}

	hash := sha256.Sum256(data)
	return fmt.Sprintf("%x", hash), nil
}

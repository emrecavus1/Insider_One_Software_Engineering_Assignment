package notification

import "testing"

func TestFingerprintSingle(t *testing.T) {
	request := CreateRequest{
		Recipient: "+905551234567",
		Channel:   ChannelSMS,
		Content:   "Your order has shipped!",
		Priority:  PriorityHigh,
	}

	first, err := FingerprintSingle(request)
	if err != nil {
		t.Fatal(err)
	}

	second, err := FingerprintSingle(request)
	if err != nil {
		t.Fatal(err)
	}

	if first != second {
		t.Error("identical requests produced different hashes")
	}

	if len(first) != 64 {
		t.Errorf("expected 64-character hash, got %d", len(first))
	}

	modified := request
	modified.Content = "Your order was cancelled!"

	third, err := FingerprintSingle(modified)
	if err != nil {
		t.Fatal(err)
	}

	if first == third {
		t.Error("different requests produced identical hashes")
	}
}

func TestFingerprintNormalization(t *testing.T) {
	first := CreateRequest{
		Recipient: " user@example.com ",
		Channel:   ChannelEmail,
		Content:   "Hello!",
	}

	second := CreateRequest{
		Recipient: "user@example.com",
		Channel:   ChannelEmail,
		Content:   "Hello!",
		Priority:  PriorityNormal,
	}

	hash1, err := FingerprintSingle(first)
	if err != nil {
		t.Fatal(err)
	}

	hash2, err := FingerprintSingle(second)
	if err != nil {
		t.Fatal(err)
	}

	if hash1 != hash2 {
		t.Error("equivalent normalized requests should have the same hash")
	}
}

func TestFingerprintBatch(t *testing.T) {
	request := CreateRequest{
		Recipient: "+905551234567",
		Channel:   ChannelSMS,
		Content:   "Flash sale!",
	}

	batch := []CreateRequest{request, request}

	first, err := FingerprintBatch(batch)
	if err != nil {
		t.Fatal(err)
	}

	second, err := FingerprintBatch(batch)
	if err != nil {
		t.Fatal(err)
	}

	if first != second {
		t.Error("identical batches produced different hashes")
	}

	single, err := FingerprintSingle(request)
	if err != nil {
		t.Fatal(err)
	}

	if first == single {
		t.Error("single and batch fingerprints must be different")
	}
}

func TestFingerprintInvalidRequests(t *testing.T) {
	_, err := FingerprintSingle(CreateRequest{})
	if err == nil {
		t.Error("expected validation error for empty notification")
	}

	_, err = FingerprintBatch(nil)
	if err == nil {
		t.Error("expected validation error for empty batch")
	}

	_, err = FingerprintBatch(make([]CreateRequest, 1001))
	if err == nil {
		t.Error("expected error for oversized batch")
	}
}

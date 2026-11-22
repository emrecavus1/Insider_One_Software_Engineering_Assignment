// In order to verify our notification system behaves correctly
package notification

import (
	"strings"
	"testing"
)

func TestCreateRequestValidation(t *testing.T) {
	tests := []struct {
		name    string
		request CreateRequest
		wantErr bool
	}{
		{
			name: "valid SMS",
			request: CreateRequest{
				Recipient: "+905551234567",
				Channel:   ChannelSMS,
				Content:   "Your order has shipped",
				Priority:  PriorityHigh,
			},
		},
		{
			name: "valid email",
			request: CreateRequest{
				Recipient: "user@example.com",
				Channel:   ChannelEmail,
				Content:   "Welcome!",
			},
		},
		{
			name: "valid push notification",
			request: CreateRequest{
				Recipient: "device-token-123",
				Channel:   ChannelPush,
				Content:   "You have a new message",
				Priority:  PriorityLow,
			},
		},
		{
			name: "missing recipient",
			request: CreateRequest{
				Channel: ChannelSMS,
				Content: "Hello",
			},
			wantErr: true,
		},
		{
			name: "empty content",
			request: CreateRequest{
				Recipient: "+905551234567",
				Channel:   ChannelSMS,
				Content:   "   ",
			},
			wantErr: true,
		},
		{
			name: "invalid channel",
			request: CreateRequest{
				Recipient: "user@example.com",
				Channel:   "whatsapp",
				Content:   "Hello",
			},
			wantErr: true,
		},
		{
			name: "invalid priority",
			request: CreateRequest{
				Recipient: "+905551234567",
				Channel:   ChannelSMS,
				Content:   "Hello",
				Priority:  "urgent",
			},
			wantErr: true,
		},
		{
			name: "invalid phone number",
			request: CreateRequest{
				Recipient: "5551234567",
				Channel:   ChannelSMS,
				Content:   "Hello",
			},
			wantErr: true,
		},
		{
			name: "invalid email address",
			request: CreateRequest{
				Recipient: "not-an-email",
				Channel:   ChannelEmail,
				Content:   "Hello",
			},
			wantErr: true,
		},
		{
			name: "SMS exceeds character limit",
			request: CreateRequest{
				Recipient: "+905551234567",
				Channel:   ChannelSMS,
				Content:   strings.Repeat("a", 161),
			},
			wantErr: true,
		},
		{
			name: "push exceeds character limit",
			request: CreateRequest{
				Recipient: "device-token-123",
				Channel:   ChannelPush,
				Content:   strings.Repeat("a", 257),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.request.NormalizeAndValidate()

			if tt.wantErr && err == nil {
				t.Error("expected validation error, got nil")
			}

			if !tt.wantErr && err != nil {
				t.Errorf("unexpected validation error: %v", err)
			}
		})
	}
}

func TestDefaultPriority(t *testing.T) {
	request := CreateRequest{
		Recipient: "user@example.com",
		Channel:   ChannelEmail,
		Content:   "Welcome!",
	}

	validated, err := request.NormalizeAndValidate()
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}

	if validated.Priority != PriorityNormal {
		t.Errorf(
			"expected priority normal, got %s",
			validated.Priority,
		)
	}
}

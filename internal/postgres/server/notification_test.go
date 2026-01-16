package server

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNotificationHub_Listen(t *testing.T) {
	hub := NewNotificationHub()

	// Listen on a channel
	hub.Listen(1, "test_channel")

	// Verify subscription
	assert.True(t, hub.IsListening(1, "test_channel"))
	assert.False(t, hub.IsListening(1, "other_channel"))
	assert.False(t, hub.IsListening(2, "test_channel"))

	// Listen on another channel
	hub.Listen(1, "another_channel")
	channels := hub.GetListeningChannels(1)
	assert.Len(t, channels, 2)
	assert.Contains(t, channels, "test_channel")
	assert.Contains(t, channels, "another_channel")
}

func TestNotificationHub_ListenCaseInsensitive(t *testing.T) {
	hub := NewNotificationHub()

	// Listen with mixed case
	hub.Listen(1, "Test_Channel")

	// Should be normalized to lowercase
	assert.True(t, hub.IsListening(1, "test_channel"))
	assert.True(t, hub.IsListening(1, "TEST_CHANNEL"))
	assert.True(t, hub.IsListening(1, "Test_Channel"))
}

func TestNotificationHub_Unlisten(t *testing.T) {
	hub := NewNotificationHub()

	// Setup
	hub.Listen(1, "channel1")
	hub.Listen(1, "channel2")
	hub.Listen(2, "channel1")

	// Unlisten from specific channel
	hub.Unlisten(1, "channel1")
	assert.False(t, hub.IsListening(1, "channel1"))
	assert.True(t, hub.IsListening(1, "channel2"))
	assert.True(t, hub.IsListening(2, "channel1"))

	// Unlisten from all channels
	hub.Unlisten(1, "*")
	assert.False(t, hub.IsListening(1, "channel2"))
	assert.Nil(t, hub.GetListeningChannels(1))

	// Other session should still be listening
	assert.True(t, hub.IsListening(2, "channel1"))
}

func TestNotificationHub_Notify(t *testing.T) {
	hub := NewNotificationHub()

	// Setup listeners
	hub.Listen(1, "events")
	hub.Listen(2, "events")
	hub.Listen(3, "other")

	// Send notification
	count := hub.Notify("events", "test payload", 100)
	assert.Equal(t, 2, count)

	// Check queued notifications for session 1
	notifications := hub.GetPendingNotifications(1)
	require.Len(t, notifications, 1)
	assert.Equal(t, "events", notifications[0].Channel)
	assert.Equal(t, "test payload", notifications[0].Payload)
	assert.Equal(t, int32(100), notifications[0].PID)

	// Check queued notifications for session 2
	notifications = hub.GetPendingNotifications(2)
	require.Len(t, notifications, 1)
	assert.Equal(t, "events", notifications[0].Channel)

	// Session 3 should have no notifications
	notifications = hub.GetPendingNotifications(3)
	assert.Nil(t, notifications)

	// Notifications should be cleared after getting them
	notifications = hub.GetPendingNotifications(1)
	assert.Nil(t, notifications)
}

func TestNotificationHub_NotifyCaseInsensitive(t *testing.T) {
	hub := NewNotificationHub()

	// Listen with lowercase
	hub.Listen(1, "events")

	// Notify with uppercase - should still work
	count := hub.Notify("EVENTS", "payload", 1)
	assert.Equal(t, 1, count)

	notifications := hub.GetPendingNotifications(1)
	require.Len(t, notifications, 1)
	assert.Equal(t, "events", notifications[0].Channel)
}

func TestNotificationHub_MultipleNotifications(t *testing.T) {
	hub := NewNotificationHub()

	hub.Listen(1, "channel")

	// Send multiple notifications
	hub.Notify("channel", "first", 10)
	hub.Notify("channel", "second", 20)
	hub.Notify("channel", "third", 30)

	notifications := hub.GetPendingNotifications(1)
	require.Len(t, notifications, 3)
	assert.Equal(t, "first", notifications[0].Payload)
	assert.Equal(t, "second", notifications[1].Payload)
	assert.Equal(t, "third", notifications[2].Payload)
}

func TestNotificationHub_RemoveSession(t *testing.T) {
	hub := NewNotificationHub()

	hub.Listen(1, "channel1")
	hub.Listen(1, "channel2")
	hub.Notify("channel1", "test", 0)

	// Remove session
	hub.RemoveSession(1)

	// Session should no longer be listening
	assert.False(t, hub.IsListening(1, "channel1"))
	assert.False(t, hub.IsListening(1, "channel2"))

	// Pending notifications should be gone
	assert.Nil(t, hub.GetPendingNotifications(1))

	// New notifications should not be queued
	count := hub.Notify("channel1", "test2", 0)
	assert.Equal(t, 0, count)
}

func TestNotificationQueue(t *testing.T) {
	queue := NewNotificationQueue()

	// Initially empty
	assert.Equal(t, 0, queue.Len())
	assert.Nil(t, queue.PopAll())

	// Add notifications
	queue.Push(&Notification{Channel: "ch1", Payload: "p1", PID: 1})
	queue.Push(&Notification{Channel: "ch2", Payload: "p2", PID: 2})
	assert.Equal(t, 2, queue.Len())

	// Pop all
	items := queue.PopAll()
	assert.Len(t, items, 2)
	assert.Equal(t, 0, queue.Len())
	assert.Nil(t, queue.PopAll())

	// Close queue
	queue.Push(&Notification{Channel: "ch3", Payload: "p3", PID: 3})
	assert.Equal(t, 1, queue.Len())

	queue.Close()

	// After close, push should be no-op
	queue.Push(&Notification{Channel: "ch4", Payload: "p4", PID: 4})
	// Length should still be 0 (items cleared on close)
	assert.Equal(t, 0, queue.Len())
}

func TestWriteNotificationResponse(t *testing.T) {
	var buf bytes.Buffer

	err := WriteNotificationResponse(&buf, 12345, "test_channel", "hello world")
	require.NoError(t, err)

	// Verify the message format
	data := buf.Bytes()

	// Message type should be 'A'
	assert.Equal(t, byte('A'), data[0])

	// Parse message length (big endian int32)
	msgLen := int(data[1])<<24 | int(data[2])<<16 | int(data[3])<<8 | int(data[4])

	// Expected length: 4 (length) + 4 (pid) + 12 (channel) + 1 (null) + 11 (payload) + 1 (null) = 33
	expectedLen := 4 + 4 + len("test_channel") + 1 + len("hello world") + 1
	assert.Equal(t, expectedLen, msgLen)

	// Parse PID (big endian int32)
	pid := int32(data[5])<<24 | int32(data[6])<<16 | int32(data[7])<<8 | int32(data[8])
	assert.Equal(t, int32(12345), pid)

	// Parse channel name (null-terminated string starting at byte 9)
	channelEnd := bytes.IndexByte(data[9:], 0)
	channel := string(data[9 : 9+channelEnd])
	assert.Equal(t, "test_channel", channel)

	// Parse payload (null-terminated string after channel)
	payloadStart := 9 + channelEnd + 1
	payloadEnd := bytes.IndexByte(data[payloadStart:], 0)
	payload := string(data[payloadStart : payloadStart+payloadEnd])
	assert.Equal(t, "hello world", payload)
}

func TestWriteNotificationResponse_EmptyPayload(t *testing.T) {
	var buf bytes.Buffer

	err := WriteNotificationResponse(&buf, 1, "ch", "")
	require.NoError(t, err)

	data := buf.Bytes()
	assert.Equal(t, byte('A'), data[0])

	// Verify the payload is empty (just a null terminator)
	// After message type (1), length (4), pid (4), channel (2), null (1) = byte 12
	payloadStart := 1 + 4 + 4 + len("ch") + 1
	assert.Equal(t, byte(0), data[payloadStart])
}

func TestParseChannelName(t *testing.T) {
	tests := []struct {
		query    string
		prefix   string
		expected string
	}{
		{"LISTEN test_channel", "LISTEN ", "test_channel"},
		{"listen test_channel", "LISTEN ", "test_channel"},
		{"LISTEN test_channel;", "LISTEN ", "test_channel"},
		{"LISTEN \"my_channel\"", "LISTEN ", "my_channel"},
		{"LISTEN 'my_channel'", "LISTEN ", "my_channel"},
		{"UNLISTEN *", "UNLISTEN ", "*"},
		{"UNLISTEN test", "UNLISTEN ", "test"},
		// Note: "UNLISTEN" alone without prefix match returns the original string
		// The handler treats empty as "*" anyway
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			result := parseChannelName(tt.query, tt.prefix)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseNotifyArgs(t *testing.T) {
	tests := []struct {
		query           string
		expectedChannel string
		expectedPayload string
	}{
		{"NOTIFY test_channel", "test_channel", ""},
		{"notify test_channel", "test_channel", ""},
		{"NOTIFY test_channel;", "test_channel", ""},
		{"NOTIFY channel, 'payload'", "channel", "payload"},
		{"NOTIFY channel, \"payload\"", "channel", "payload"},
		{"NOTIFY channel, 'hello world'", "channel", "hello world"},
		{"NOTIFY channel, 'payload with, comma'", "channel", "payload with, comma"},
		{"NOTIFY \"my_channel\", 'test'", "my_channel", "test"},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			channel, payload := parseNotifyArgs(tt.query)
			assert.Equal(t, tt.expectedChannel, channel)
			assert.Equal(t, tt.expectedPayload, payload)
		})
	}
}

func TestFindCommaOutsideQuotes(t *testing.T) {
	tests := []struct {
		input    string
		expected int
	}{
		{"a,b", 1},
		{"'a,b',c", 5},
		{"\"a,b\",c", 5},
		{"abc", -1},
		{"'no comma'", -1},
		{"first, 'second, third'", 5},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := findCommaOutsideQuotes(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Package server provides a PostgreSQL wire protocol server for dukdb-go.
package server

import (
	"encoding/binary"
	"io"
	"strings"
	"sync"
)

// Notification represents a PostgreSQL asynchronous notification.
// Notifications are sent via the NotificationResponse message ('A').
type Notification struct {
	// Channel is the name of the channel this notification was sent on.
	Channel string
	// Payload is the optional payload string sent with the notification.
	Payload string
	// PID is the process ID of the notifying backend (session ID in our case).
	PID int32
}

// NotificationHub manages channel subscriptions and notification delivery.
// It is a server-wide component that tracks all listening sessions.
type NotificationHub struct {
	mu sync.RWMutex

	// subscriptions maps channel names to sets of session IDs.
	subscriptions map[string]map[uint64]struct{}

	// sessionChannels maps session IDs to sets of channel names they're listening to.
	sessionChannels map[uint64]map[string]struct{}

	// sessionQueues maps session IDs to their pending notification queues.
	sessionQueues map[uint64]*NotificationQueue
}

// NewNotificationHub creates a new notification hub.
func NewNotificationHub() *NotificationHub {
	return &NotificationHub{
		subscriptions:   make(map[string]map[uint64]struct{}),
		sessionChannels: make(map[uint64]map[string]struct{}),
		sessionQueues:   make(map[uint64]*NotificationQueue),
	}
}

// Listen registers a session to receive notifications on a channel.
// If the session is already listening on the channel, this is a no-op.
func (h *NotificationHub) Listen(sessionID uint64, channel string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Normalize channel name to lowercase
	channel = strings.ToLower(channel)

	// Add to channel subscriptions
	if h.subscriptions[channel] == nil {
		h.subscriptions[channel] = make(map[uint64]struct{})
	}
	h.subscriptions[channel][sessionID] = struct{}{}

	// Track which channels this session is listening to
	if h.sessionChannels[sessionID] == nil {
		h.sessionChannels[sessionID] = make(map[string]struct{})
	}
	h.sessionChannels[sessionID][channel] = struct{}{}

	// Ensure session has a queue
	if h.sessionQueues[sessionID] == nil {
		h.sessionQueues[sessionID] = NewNotificationQueue()
	}
}

// Unlisten unregisters a session from receiving notifications on a channel.
// If channel is "*", unregisters from all channels.
func (h *NotificationHub) Unlisten(sessionID uint64, channel string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if channel == "*" {
		// Unlisten from all channels
		if channels, ok := h.sessionChannels[sessionID]; ok {
			for ch := range channels {
				if subs, ok := h.subscriptions[ch]; ok {
					delete(subs, sessionID)
					if len(subs) == 0 {
						delete(h.subscriptions, ch)
					}
				}
			}
			delete(h.sessionChannels, sessionID)
		}
		return
	}

	// Normalize channel name to lowercase
	channel = strings.ToLower(channel)

	// Remove from channel subscriptions
	if subs, ok := h.subscriptions[channel]; ok {
		delete(subs, sessionID)
		if len(subs) == 0 {
			delete(h.subscriptions, channel)
		}
	}

	// Remove from session's channel list
	if channels, ok := h.sessionChannels[sessionID]; ok {
		delete(channels, channel)
		if len(channels) == 0 {
			delete(h.sessionChannels, sessionID)
		}
	}
}

// Notify sends a notification to all sessions listening on the given channel.
// The notification is queued for delivery at the next opportunity.
// Returns the number of sessions that will receive the notification.
func (h *NotificationHub) Notify(channel, payload string, senderPID int32) int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	// Normalize channel name to lowercase
	channel = strings.ToLower(channel)

	// Get all subscribers
	subs, ok := h.subscriptions[channel]
	if !ok || len(subs) == 0 {
		return 0
	}

	notification := &Notification{
		Channel: channel,
		Payload: payload,
		PID:     senderPID,
	}

	// Queue notification for all subscribers
	count := 0
	for sessionID := range subs {
		if queue, ok := h.sessionQueues[sessionID]; ok {
			queue.Push(notification)
			count++
		}
	}

	return count
}

// GetPendingNotifications retrieves and clears all pending notifications for a session.
func (h *NotificationHub) GetPendingNotifications(sessionID uint64) []*Notification {
	h.mu.RLock()
	queue, ok := h.sessionQueues[sessionID]
	h.mu.RUnlock()

	if !ok {
		return nil
	}

	return queue.PopAll()
}

// IsListening checks if a session is listening on a specific channel.
func (h *NotificationHub) IsListening(sessionID uint64, channel string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()

	channel = strings.ToLower(channel)
	if channels, ok := h.sessionChannels[sessionID]; ok {
		_, listening := channels[channel]
		return listening
	}
	return false
}

// GetListeningChannels returns all channels a session is listening on.
func (h *NotificationHub) GetListeningChannels(sessionID uint64) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if channels, ok := h.sessionChannels[sessionID]; ok {
		result := make([]string, 0, len(channels))
		for ch := range channels {
			result = append(result, ch)
		}
		return result
	}
	return nil
}

// RemoveSession removes all subscriptions for a session.
// Should be called when a session disconnects.
func (h *NotificationHub) RemoveSession(sessionID uint64) {
	h.Unlisten(sessionID, "*")

	h.mu.Lock()
	delete(h.sessionQueues, sessionID)
	h.mu.Unlock()
}

// NotificationQueue is a thread-safe queue for pending notifications.
type NotificationQueue struct {
	mu     sync.Mutex
	items  []*Notification
	closed bool
}

// NewNotificationQueue creates a new notification queue.
func NewNotificationQueue() *NotificationQueue {
	return &NotificationQueue{
		items: make([]*Notification, 0, 16),
	}
}

// Push adds a notification to the queue.
func (q *NotificationQueue) Push(n *Notification) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return
	}

	q.items = append(q.items, n)
}

// PopAll retrieves and removes all notifications from the queue.
func (q *NotificationQueue) PopAll() []*Notification {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.items) == 0 {
		return nil
	}

	result := q.items
	q.items = make([]*Notification, 0, 16)
	return result
}

// Len returns the number of pending notifications.
func (q *NotificationQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// Close marks the queue as closed. No more notifications will be accepted.
func (q *NotificationQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.items = nil
}

// NotificationResponseMessage type constant.
// PostgreSQL wire protocol message type 'A' for NotificationResponse.
const ServerNotificationResponse byte = 'A'

// WriteNotificationResponse writes a NotificationResponse message to the writer.
// Format:
//   - Byte1('A') - Identifies the message as a notification response.
//   - Int32 - Length of message contents in bytes, including self.
//   - Int32 - The process ID of the notifying backend process.
//   - String - The name of the channel that the notify has been raised on.
//   - String - The "payload" string passed from the notifying process.
func WriteNotificationResponse(w io.Writer, pid int32, channel, payload string) error {
	// Calculate message length
	// 4 (length) + 4 (pid) + len(channel) + 1 (null) + len(payload) + 1 (null)
	msgLen := 4 + 4 + len(channel) + 1 + len(payload) + 1

	// Create message buffer
	buf := make([]byte, 1+msgLen)

	// Message type
	buf[0] = ServerNotificationResponse

	// Message length (excluding type byte)
	binary.BigEndian.PutUint32(buf[1:5], uint32(msgLen))

	// Process ID
	binary.BigEndian.PutUint32(buf[5:9], uint32(pid))

	// Channel name (null-terminated)
	pos := 9
	copy(buf[pos:], channel)
	pos += len(channel)
	buf[pos] = 0
	pos++

	// Payload (null-terminated)
	copy(buf[pos:], payload)
	pos += len(payload)
	buf[pos] = 0

	_, err := w.Write(buf)
	return err
}

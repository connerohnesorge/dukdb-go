// Package server provides a PostgreSQL wire protocol server for dukdb-go.
package server

import (
	"context"
	"io"
	"sync"
)

// NotificationWriter is an interface for writing notification responses.
// It abstracts the underlying connection writer for notification delivery.
type NotificationWriter interface {
	io.Writer
}

// NotificationDeliveryContext holds context for delivering notifications to a session.
// This is used to bridge between the notification hub and the wire protocol writer.
type NotificationDeliveryContext struct {
	mu sync.Mutex

	// sessionID identifies the session.
	sessionID uint64

	// hub is the notification hub that manages subscriptions.
	hub *NotificationHub

	// writer is the connection writer for sending notifications.
	// This is set during query processing when we have access to the writer.
	writer NotificationWriter
}

// NewNotificationDeliveryContext creates a new notification delivery context.
func NewNotificationDeliveryContext(
	sessionID uint64,
	hub *NotificationHub,
) *NotificationDeliveryContext {
	return &NotificationDeliveryContext{
		sessionID: sessionID,
		hub:       hub,
	}
}

// SetWriter sets the current connection writer.
// This should be called when query processing begins.
func (ndc *NotificationDeliveryContext) SetWriter(w NotificationWriter) {
	ndc.mu.Lock()
	defer ndc.mu.Unlock()
	ndc.writer = w
}

// ClearWriter clears the current connection writer.
// This should be called when query processing ends.
func (ndc *NotificationDeliveryContext) ClearWriter() {
	ndc.mu.Lock()
	defer ndc.mu.Unlock()
	ndc.writer = nil
}

// DeliverPendingNotifications delivers all pending notifications for this session.
// Returns the number of notifications delivered.
func (ndc *NotificationDeliveryContext) DeliverPendingNotifications() (int, error) {
	ndc.mu.Lock()
	defer ndc.mu.Unlock()

	if ndc.hub == nil || ndc.writer == nil {
		return 0, nil
	}

	notifications := ndc.hub.GetPendingNotifications(ndc.sessionID)
	if len(notifications) == 0 {
		return 0, nil
	}

	for _, n := range notifications {
		if err := WriteNotificationResponse(ndc.writer, n.PID, n.Channel, n.Payload); err != nil {
			return 0, err
		}
	}

	return len(notifications), nil
}

// HasPendingNotifications checks if there are pending notifications for this session.
func (ndc *NotificationDeliveryContext) HasPendingNotifications() bool {
	if ndc.hub == nil {
		return false
	}
	notifications := ndc.hub.GetPendingNotifications(ndc.sessionID)
	if len(notifications) == 0 {
		return false
	}
	// Put them back since we just want to check
	for _, n := range notifications {
		ndc.hub.sessionQueues[ndc.sessionID].Push(n)
	}
	return true
}

// sessionNotificationContextKey is the context key for notification delivery context.
type sessionNotificationContextKey struct{}

// NotificationDeliveryContextFromContext retrieves the notification delivery context from the context.
func NotificationDeliveryContextFromContext(
	ctx context.Context,
) (*NotificationDeliveryContext, bool) {
	ndc, ok := ctx.Value(sessionNotificationContextKey{}).(*NotificationDeliveryContext)
	return ndc, ok
}

// ContextWithNotificationDelivery adds the notification delivery context to the context.
func ContextWithNotificationDelivery(
	ctx context.Context,
	ndc *NotificationDeliveryContext,
) context.Context {
	return context.WithValue(ctx, sessionNotificationContextKey{}, ndc)
}

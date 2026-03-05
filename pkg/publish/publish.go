package publish

import (
	"context"

	"cloud.google.com/go/pubsub"
)

// TopicPublisher adapts a *pubsub.Topic to the Publisher interface by collapsing
// the two-step Publish(...).Get(ctx) pattern into a single synchronous call.
type TopicPublisher struct {
	topic *pubsub.Topic
}

// Publisher abstracts synchronous message publishing. The processor uses this
// to forward permanent-error messages to a dead letter queue. Tests provide a
// simple in-memory fake; production code wraps *pubsub.Topic via TopicPublisher.
type Publisher interface {
	Publish(ctx context.Context, msg *pubsub.Message) error
}

// NewTopicPublisher creates a Publisher that publishes to the given Pub/Sub topic.
func NewTopicPublisher(t *pubsub.Topic) *TopicPublisher {
	return &TopicPublisher{topic: t}
}

// Publish sends a message to the underlying topic and blocks until the server
// acknowledges receipt (or the context is cancelled).
func (tp *TopicPublisher) Publish(ctx context.Context, msg *pubsub.Message) error {
	_, err := tp.topic.Publish(ctx, msg).Get(ctx)
	return err
}

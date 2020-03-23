package tars

import "context"

type ctxOutgoingKey struct{}

func FromOutgoingContext(ctx context.Context) (md map[string]string, ok bool) {
	md, ok = ctx.Value(ctxOutgoingKey{}).(map[string]string)
	return
}

// NewOutgoingContext creates a new context with outgoing md attached.
func NewOutgoingContext(ctx context.Context, ctxmap map[string]string) context.Context {
	return context.WithValue(ctx, ctxOutgoingKey{}, ctxmap)
}
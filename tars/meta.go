package tars

import "context"

type ctxOutgoingKey struct{}
type MD map[string]string

func FromOutgoingContext(ctx context.Context) (md map[string]string, ok bool) {
	md, ok = ctx.Value(ctxOutgoingKey{}).(map[string]string)
	return
}

// NewOutgoingContext creates a new context with outgoing md attached.
func NewOutgoingContext(ctx context.Context, ctxmap map[string]string) context.Context {
	return context.WithValue(ctx, ctxOutgoingKey{}, ctxmap)
}

func String(ctx context.Context, key string) string {
	md, ok := ctx.Value(ctxOutgoingKey{}).(MD)
	if !ok {
		return ""
	}
	str, _ := md[key]
	return str
}
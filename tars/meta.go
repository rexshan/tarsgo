package tars

import (
	"context"
	"strconv"
)

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

func Int64(ctx context.Context, key string) int64 {
	md, ok := ctx.Value(ctxOutgoingKey{}).(MD)
	if !ok {
		return 0
	}
	i64,err := strconv.ParseInt(md[key], 10, 64)
	if err != nil {
		return 0
	}
	return i64
}
package tars

import (
	"context"
	"github.com/rexshan/tarsgo/tars/util/current"
	"strconv"
)

func String(ctx context.Context, key string) string {
	md, ok := current.GetRequestContext(ctx)
	if !ok {
		return ""
	}
	str, _ := md[key]
	return str
}

func Int64(ctx context.Context, key string) int64 {
	md, ok := current.GetRequestContext(ctx)
	if !ok {
		return 0
	}
	i64,err := strconv.ParseInt(md[key], 10, 64)
	if err != nil {
		return 0
	}
	return i64
}
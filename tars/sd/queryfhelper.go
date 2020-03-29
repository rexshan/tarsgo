package sd

import (
	"errors"
	"github.com/rexshan/tarsgo/tars"
	"github.com/rexshan/tarsgo/tars/protocol/res/endpointf"
	"github.com/rexshan/tarsgo/tars/protocol/res/queryf"
)

var (
	RateLimiterErr = errors.New("rate limiter triggered")
)


type SDHelper interface {
	FindObjectByIdInSameGroup(id string, activeEp *[]endpointf.EndpointF, inactiveEp *[]endpointf.EndpointF, _opt ...map[string]string) (_ret int32, _err error)
}

type QueryFHelper struct {
	qratelimiter *tars.Bucket
	q            *queryf.QueryF
}

func NewQueryFHelper(qratelimiter *tars.Bucket, q *queryf.QueryF) SDHelper {
	return &QueryFHelper{
		qratelimiter: qratelimiter,
		q:            q,
	}
}

func (this *QueryFHelper) FindObjectByIdInSameGroup(id string, activeEp *[]endpointf.EndpointF, inactiveEp *[]endpointf.EndpointF, _opt ...map[string]string) (_ret int32, _err error) {
	return this.q.FindObjectByIdInSameGroup(id, activeEp, inactiveEp)
}

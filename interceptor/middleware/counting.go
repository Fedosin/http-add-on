package middleware

import (
	"net/http"

	"github.com/go-logr/logr"

	"github.com/kedacore/http-add-on/interceptor/metrics"
	"github.com/kedacore/http-add-on/pkg/k8s"
	"github.com/kedacore/http-add-on/pkg/queue"
	"github.com/kedacore/http-add-on/pkg/util"
)

type Counting struct {
	queueCounter    queue.Counter
	upstreamHandler http.Handler
}

func NewCountingMiddleware(queueCounter queue.Counter, upstreamHandler http.Handler) *Counting {
	return &Counting{
		queueCounter:    queueCounter,
		upstreamHandler: upstreamHandler,
	}
}

var _ http.Handler = (*Counting)(nil)

// ServeHTTP increments the queue counter synchronously before forwarding
// the request, and decrements it via defer when the request completes.
// This avoids spawning goroutines per request, which was the previous
// bottleneck under high RPS.
func (cm *Counting) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r = util.RequestWithLoggerWithName(r, "CountingMiddleware")
	ctx := r.Context()
	logger := util.LoggerFromContext(ctx)
	httpso := util.HTTPSOFromContext(ctx)
	key := k8s.NamespacedNameFromObject(httpso).String()

	if !cm.inc(logger, key) {
		cm.upstreamHandler.ServeHTTP(w, r)
		return
	}
	defer cm.dec(logger, key)

	cm.upstreamHandler.ServeHTTP(w, r)
}

func (cm *Counting) inc(logger logr.Logger, key string) bool {
	if err := cm.queueCounter.Increase(key, 1); err != nil {
		logger.Error(err, "error incrementing queue counter", "key", key)

		return false
	}

	metrics.RecordPendingRequestCount(key, int64(1))

	return true
}

func (cm *Counting) dec(logger logr.Logger, key string) bool {
	if err := cm.queueCounter.Decrease(key, 1); err != nil {
		logger.Error(err, "error decrementing queue counter", "key", key)

		return false
	}

	metrics.RecordPendingRequestCount(key, int64(-1))

	return true
}

package logging

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
)

// TranscriptSink is an optional request-local observer. Implementations must
// never wait for persistence, and must account for refused bytes independently.
// Native transport code depends only on this contract, not the store or adapter.
type TranscriptSink interface {
	Capture(kind string, data []byte)
	Drop(chunks, bytes uint64)
	HTTP(kind, method, url string, status int, headers http.Header, body []byte)
	Failure(kind string, err error)
}

const TranscriptSinkContextKey = "__transcript_sink__"

type transcriptContextKey struct{}

func WithTranscriptSink(ctx context.Context, sink TranscriptSink) context.Context {
	return context.WithValue(ctx, transcriptContextKey{}, sink)
}

func TranscriptFromContext(ctx context.Context) TranscriptSink {
	if ctx == nil {
		return nil
	}
	if sink, ok := ctx.Value(transcriptContextKey{}).(TranscriptSink); ok {
		return sink
	}
	// Some native executors retain Gin while replacing their request context.
	if g, ok := ctx.Value("gin").(*gin.Context); ok && g != nil {
		if value, exists := g.Get(TranscriptSinkContextKey); exists {
			sink, _ := value.(TranscriptSink)
			return sink
		}
	}
	return nil
}

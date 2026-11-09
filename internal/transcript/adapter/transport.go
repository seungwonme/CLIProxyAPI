package adapter

import (
	"fmt"
	"io"
	"net/http"
)

// decorate uses the native request-scoped transport seam after provider TLS,
// proxy and pool selection. It neither reads ahead nor changes transport policy.
func (e *exchange) decorate(next http.RoundTripper) http.RoundTripper {
	return &observedTransport{next: next, capture: e}
}

type observedTransport struct {
	next    http.RoundTripper
	capture *exchange
}

func (t *observedTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	id := t.capture.attempt.Add(1)
	kind := fmt.Sprintf("http.upstream.%d", id)
	t.capture.HTTP(kind+".request", r.Method, r.URL.String(), 0, r.Header, nil)
	request := r.Clone(r.Context())
	if r.Body != nil && r.Body != http.NoBody {
		request.Body = &wireReader{ReadCloser: r.Body, capture: t.capture, kind: kind + ".request.body"}
	}
	response, err := t.next.RoundTrip(request)
	if err != nil {
		t.capture.Failure(kind+".error", err)
	}
	if response != nil {
		t.capture.HTTP(kind+".response", "", "", response.StatusCode, response.Header, nil)
		if response.Body != nil {
			response.Body = &wireReader{ReadCloser: response.Body, capture: t.capture, kind: kind + ".response.body"}
		}
	}
	return response, err
}

type wireReader struct {
	io.ReadCloser
	capture *exchange
	kind    string
}

func (r *wireReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if n > 0 {
		r.capture.Capture(r.kind, p[:n])
	}
	if err != nil && err != io.EOF {
		r.capture.Failure(r.kind+".error", err)
	}
	return n, err
}

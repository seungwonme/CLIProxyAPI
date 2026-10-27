package access

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

type observerProvider struct {
	name   string
	result *Result
	err    *AuthError
	calls  int
}

func (p *observerProvider) Identifier() string { return p.name }

func (p *observerProvider) Authenticate(context.Context, *http.Request) (*Result, *AuthError) {
	p.calls++
	return p.result, p.err
}

func TestAuthenticationObserverSeesOnlyFirstSuccessfulProvider(t *testing.T) {
	first := &Result{Provider: "accepted", Principal: "verified-user", Metadata: map[string]string{"source": "authorization"}}
	skipped := &observerProvider{name: "not-handled", err: NewNotHandledError()}
	invalid := &observerProvider{name: "invalid", err: NewInvalidCredentialError()}
	accepted := &observerProvider{name: "accepted", result: first}
	later := &observerProvider{name: "later", result: &Result{Provider: "later", Principal: "other-user"}}
	manager := NewManager()
	manager.SetProviders([]Provider{nil, skipped, invalid, accepted, later})
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	observations := 0
	ctx := WithAuthenticationObserver(context.Background(), func(gotRequest *http.Request, gotResult *Result) {
		observations++
		if gotRequest != req || gotResult != first {
			t.Errorf("observer received request/result (%p, %p), want (%p, %p)", gotRequest, gotResult, req, first)
		}
		if accepted.calls != observations || later.calls != 0 {
			t.Errorf("observer called outside the first successful provider: accepted=%d later=%d observations=%d", accepted.calls, later.calls, observations)
		}
	})

	// A second Authenticate call remains a normal authentication operation.
	for call := 1; call <= 2; call++ {
		result, authErr := manager.Authenticate(ctx, req)
		if result != first || authErr != nil {
			t.Fatalf("Authenticate() = (%p, %v), want original success (%p, nil)", result, authErr, first)
		}
		if observations != call {
			t.Fatalf("observer calls = %d after %d authentications", observations, call)
		}
	}
	if skipped.calls != 2 || invalid.calls != 2 || accepted.calls != 2 || later.calls != 0 {
		t.Fatalf("provider calls = (%d, %d, %d, %d), want (2, 2, 2, 0)", skipped.calls, invalid.calls, accepted.calls, later.calls)
	}
	if first.Principal != "verified-user" || first.Metadata["source"] != "authorization" {
		t.Fatalf("observer changed authentication result: %#v", first)
	}
}

func TestAuthenticationObserverDoesNotObserveRejectedOrNilSuccess(t *testing.T) {
	denied := NewDeniedError("synthetic policy denial", http.StatusTooManyRequests)
	tests := []struct {
		name     string
		results  []*observerProvider
		wantCode AuthErrorCode
		wantErr  *AuthError
	}{
		{name: "no providers"},
		{name: "nil success", results: []*observerProvider{{name: "nil-success"}, {name: "unreached", result: &Result{Principal: "unreached"}}}},
		{name: "missing credentials", results: []*observerProvider{{name: "missing", err: NewNoCredentialsError()}}, wantCode: AuthErrorCodeNoCredentials},
		{name: "not handled", results: []*observerProvider{{name: "skip", err: NewNotHandledError()}}, wantCode: AuthErrorCodeNoCredentials},
		{name: "invalid beats missing", results: []*observerProvider{{name: "invalid", err: NewInvalidCredentialError()}, {name: "missing", err: NewNoCredentialsError()}}, wantCode: AuthErrorCodeInvalidCredential},
		{name: "terminal denial", results: []*observerProvider{{name: "denied", err: denied}, {name: "unreached", result: &Result{Principal: "unreached"}}}, wantCode: AuthErrorCodeDenied, wantErr: denied},
		{name: "result alongside error", results: []*observerProvider{{name: "invalid", result: &Result{Principal: "must-not-observe"}, err: NewInvalidCredentialError()}}, wantCode: AuthErrorCodeInvalidCredential},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewManager()
			providers := make([]Provider, len(tt.results))
			for i, provider := range tt.results {
				providers[i] = provider
			}
			manager.SetProviders(providers)
			observed := 0
			ctx := WithAuthenticationObserver(context.Background(), func(*http.Request, *Result) { observed++ })
			result, authErr := manager.Authenticate(ctx, httptest.NewRequest(http.MethodPost, "/v1/responses", nil))
			if result != nil || observed != 0 {
				t.Fatalf("Authenticate() result=%#v observer calls=%d, want nil and zero", result, observed)
			}
			if tt.wantCode == "" {
				if authErr != nil {
					t.Fatalf("Authenticate() error = %v, want nil", authErr)
				}
			} else if !IsAuthErrorCode(authErr, tt.wantCode) {
				t.Fatalf("Authenticate() error = %v, want %s", authErr, tt.wantCode)
			}
			if tt.wantErr != nil && authErr != tt.wantErr {
				t.Fatalf("terminal authentication error was replaced: got %p, want %p", authErr, tt.wantErr)
			}
			for _, provider := range tt.results {
				if provider.name == "unreached" && provider.calls != 0 {
					t.Fatalf("provider after terminal result called %d times", provider.calls)
				}
			}
		})
	}
}

func TestAuthenticationObserverNilManagerAndAbsentObserverPreserveAuthentication(t *testing.T) {
	observed := 0
	ctx := WithAuthenticationObserver(context.Background(), func(*http.Request, *Result) { observed++ })
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	var manager *Manager
	if result, authErr := manager.Authenticate(ctx, req); result != nil || authErr != nil || observed != 0 {
		t.Fatalf("nil manager = (%#v, %v), observer calls=%d", result, authErr, observed)
	}
	ObserveAuthentication(ctx, req, nil)
	ObserveAuthentication(nil, req, &Result{Principal: "verified-user"})
	ObserveAuthentication(context.Background(), req, &Result{Principal: "verified-user"})
	if observed != 0 {
		t.Fatalf("nil result or absent observer invoked callback %d times", observed)
	}

	want := &Result{Provider: "native", Principal: "verified-user"}
	manager = NewManager()
	manager.SetProviders([]Provider{&observerProvider{name: "native", result: want}})
	if result, authErr := manager.Authenticate(context.Background(), req); result != want || authErr != nil {
		t.Fatalf("authentication without observer = (%#v, %v), want original result", result, authErr)
	}
}

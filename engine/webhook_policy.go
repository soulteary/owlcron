package engine

import (
	"context"
	"errors"
	"net/http"

	"github.com/soulteary/http-kit/v2/outbound"
	"github.com/soulteary/owlcron/types"
)

// webHookPolicy adapts application configuration and diagnostics to the shared
// outbound policy. Address and connection rules belong to http-kit/outbound.
type webHookPolicy struct {
	policy *outbound.Policy
	err    error
}

type webHookPolicyError struct{}

func (*webHookPolicyError) Error() string {
	return "webhook destination is prohibited by security policy"
}
func (*webHookPolicyError) Unwrap() error { return outbound.ErrBlocked }

func newWebHookPolicy(config *types.Config) *webHookPolicy {
	var options outbound.Options
	if config != nil {
		options.AllowedHosts = config.WebHookAllowedHosts
		options.AllowedCIDRs = config.WebHookAllowedCIDRs
	}
	policy, err := outbound.NewPolicy(options)
	return &webHookPolicy{policy: policy, err: err}
}

func validateWebHookPolicy(config *types.Config) error { return newWebHookPolicy(config).err }

func (p *webHookPolicy) validate(ctx context.Context, target string) error {
	if p.err != nil {
		return &webHookPolicyError{}
	}
	err := p.policy.Validate(ctx, target)
	switch {
	case errors.Is(err, outbound.ErrBlocked):
		return &webHookPolicyError{}
	case errors.Is(err, outbound.ErrDNSResolution):
		return errors.New("webhook DNS resolution failed")
	case errors.Is(err, outbound.ErrNoAddresses):
		return errors.New("webhook DNS returned no addresses")
	default:
		return err
	}
}

func (p *webHookPolicy) client() *http.Client {
	if p.err != nil {
		// Engine construction rejects bad configuration. Standalone managers
		// must also fail closed instead of falling back to an unrestricted client.
		return &http.Client{Transport: webHookDeniedTransport{}}
	}
	return p.policy.Client()
}

type webHookDeniedTransport struct{}

func (webHookDeniedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		_ = req.Body.Close()
	}
	return nil, &webHookPolicyError{}
}

func isWebHookPolicyDenied(err error) bool { return errors.Is(err, outbound.ErrBlocked) }

func (whm *WebHookManager) ValidateWebHookURL(ctx context.Context, target string) error {
	return whm.policy.validate(ctx, target)
}

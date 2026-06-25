package types

// VhostConfig represents a virtual host configuration for the WAF proxy.
type VhostConfig struct {
	Domain      string
	Upstream    string
	CertFile    string
	KeyFile     string
	ACME        bool
	HTTPToHTTPS *bool
	HTTPEnabled bool
}

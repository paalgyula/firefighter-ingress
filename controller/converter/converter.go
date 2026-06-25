package converter

import (
	"fmt"
	"strings"

	"github.com/paalgyula/firefighter-ingress/pkg/types"
	"github.com/rs/zerolog/log"
	networkingv1 "k8s.io/api/networking/v1"
)

// ConvertIngress converts a Kubernetes Ingress to VHostConfig
func ConvertIngress(ing *networkingv1.Ingress) []types.VhostConfig {
	var configs []types.VhostConfig

	className := ing.Spec.IngressClassName
	if className == nil || *className != "firefighter" {
		if className != nil {
			return configs
		}
	}

	host := ""
	if len(ing.Spec.Rules) > 0 && ing.Spec.Rules[0].Host != "" {
		host = ing.Spec.Rules[0].Host
	}

	annotations := ing.Annotations
	rateLimit := parseAnnotation(annotations, "firefighter.pilab.dev/rate-limit", "")
	httpToHTTPS := parseAnnotation(annotations, "firefighter.pilab.dev/ssl-redirect", "false") == "true"

	certSecret := ""
	if len(ing.Spec.TLS) > 0 {
		certSecret = ing.Spec.TLS[0].SecretName
	}

	for _, rule := range ing.Spec.Rules {
		if rule.Host != "" {
			host = rule.Host
		}

		upstream := ""
		if len(rule.HTTP.Paths) > 0 {
			svc := rule.HTTP.Paths[0].Backend.Service
			if svc != nil {
				upstream = fmt.Sprintf("http://%s.%s.svc:%d",
					svc.Name,
					ing.Namespace,
					svc.Port.Number)
			}
		}

		vh := types.VhostConfig{
			Domain:      host,
			Upstream:    upstream,
			HTTPEnabled: true,
		}

		if httpToHTTPS {
			vh.HTTPToHTTPS = new(bool)
			*vh.HTTPToHTTPS = true
		}

		if certSecret != "" {
			vh.CertFile = fmt.Sprintf("/etc/kubernetes/secrets/%s/%s.crt", ing.Namespace, certSecret)
			vh.KeyFile = fmt.Sprintf("/etc/kubernetes/secrets/%s/%s.key", ing.Namespace, certSecret)
		}

		if rateLimit != "" {
			log.Debug().Str("rate_limit", rateLimit).Msg("Rate limit annotation found")
		}

		configs = append(configs, vh)
	}

	if ing.Spec.DefaultBackend != nil {
		upstream := fmt.Sprintf("http://%s.%s.svc:%d",
			ing.Spec.DefaultBackend.Service.Name,
			ing.Namespace,
			ing.Spec.DefaultBackend.Service.Port.Number)

		configs = append(configs, types.VhostConfig{
			Domain:      "_",
			Upstream:    upstream,
			HTTPEnabled: true,
		})
	}

	return configs
}

func parseAnnotation(annotations map[string]string, key, defaultValue string) string {
	if val, ok := annotations[key]; ok {
		return val
	}

	val, ok := annotations["nginx.org/"+strings.TrimPrefix(key, "firefighter.pilab.dev/")]
	if ok {
		return val
	}

	return defaultValue
}

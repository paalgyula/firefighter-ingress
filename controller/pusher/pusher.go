package pusher

import (
	"context"
	"fmt"
	"time"

	apiv1 "github.com/paalgyula/firefighter-ingress/gen/proto"
	"github.com/paalgyula/firefighter-ingress/pkg/types"
	"github.com/rs/zerolog/log"
)

type GRPCPusher struct {
	client apiv1.ConfigPushServiceClient
}

func NewGRPC(client apiv1.ConfigPushServiceClient) *GRPCPusher {
	return &GRPCPusher{client: client}
}

func (p *GRPCPusher) PushVHosts(vhosts []types.VhostConfig, admissionToken string) error {
	if p.client == nil {
		log.Warn().Msg("WAF client not connected, skipping push")
		return nil
	}

	req := &apiv1.PushVHostsRequest{
		AdmissionToken: admissionToken,
	}

	for _, vh := range vhosts {
		vhostConfig := &apiv1.VHostConfig{
			Domain:       vh.Domain,
			Upstream:     vh.Upstream,
			CertFile:     vh.CertFile,
			KeyFile:      vh.KeyFile,
			Acme:         vh.ACME,
			HttpToHttps:  vh.HTTPToHTTPS != nil && *vh.HTTPToHTTPS,
			HttpEnabled:  vh.HTTPEnabled,
		}

		req.Vhosts = append(req.Vhosts, vhostConfig)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := p.client.PushVHosts(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to push config: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("WAF rejected config: %s", resp.Message)
	}

	log.Info().Int("vhosts", len(vhosts)).Msg("Config pushed to WAF")
	return nil
}

func (p *GRPCPusher) Close() error {
	return nil
}

package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	apiv1 "github.com/paalgyula/firefighter-ingress/gen/proto"
	"github.com/paalgyula/firefighter-ingress/controller/converter"
	"github.com/paalgyula/firefighter-ingress/controller/election"
	"github.com/paalgyula/firefighter-ingress/controller/pusher"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	networkingv1 "k8s.io/api/networking/v1"
)

type Config struct {
	Clientset     kubernetes.Interface
	WAFAddress    string
	IngressClass string
	LeaderElect bool
	CRDEnabled  bool
}

type Controller struct {
	config     Config
	pusher     *pusher.GRPCPusher
	informerFactory informers.SharedInformerFactory
	stopChan    chan struct{}
	mu         sync.RWMutex
	running    bool
	election  *election.LeaderElector
}

func New(cfg Config) (*Controller, error) {
	conn, err := grpc.Dial(cfg.WAFAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to WAF: %w", err)
	}

	client := apiv1.NewConfigPushServiceClient(conn)
	grpcPusher := pusher.NewGRPC(client)

	return &Controller{
		config:    cfg,
		pusher:   grpcPusher,
		stopChan: make(chan struct{}),
	}, nil
}

func (c *Controller) Start(ctx context.Context) error {
	log.Info().
		Str("ingress_class", c.config.IngressClass).
		Bool("leader_elect", c.config.LeaderElect).
		Bool("crd_enabled", c.config.CRDEnabled).
		Msg("Starting ingress controller")

	c.informerFactory = informers.NewSharedInformerFactory(c.config.Clientset, 0)

	ingInformer := c.informerFactory.Networking().V1().Ingresses().Informer()
	ingInformer.AddEventHandler(&cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.OnAdd(obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			c.OnUpdate(oldObj, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			c.OnDelete(obj)
		},
	})

	ingClassInformer := c.informerFactory.Networking().V1().IngressClasses().Informer()
	ingClassInformer.AddEventHandler(&cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			// IngressClass changes don't trigger config updates
		},
	})

	if c.config.CRDEnabled {
		if err := c.startCRDWatchers(ctx); err != nil {
			log.Error().Err(err).Msg("Failed to start CRD watchers, continuing without CRD support")
		}
	}

	c.informerFactory.Start(c.stopChan)
	c.informerFactory.WaitForCacheSync(c.stopChan)

	if c.config.LeaderElect {
		if err := c.startLeaderElection(ctx); err != nil {
			log.Error().Err(err).Msg("Failed to start leader election, running as non-leader")
		}
	} else {
		c.startPusher(ctx)
	}

	c.mu.Lock()
	c.running = true
	c.mu.Unlock()

	log.Info().Msg("Ingress controller started")
	return nil
}

func (c *Controller) startCRDWatchers(ctx context.Context) error {
	log.Info().Msg("CRD watchers not yet implemented")
	return nil
}

func (c *Controller) startLeaderElection(ctx context.Context) error {
	lock, err := election.New(c.config.Clientset, "firefighter-ingress")
	if err != nil {
		return err
	}

	c.election = lock.WithCallbacks(func() {
		log.Info().Msg("Acquired leadership, starting config pusher")
		c.startPusher(ctx)
	}, func() {
		log.Info().Msg("Lost leadership, stopping config pusher")
	})

	go c.election.Run(ctx)
	return nil
}

func (c *Controller) startPusher(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				log.Debug().Msg("Periodic sync not yet implemented")
			}
		}
	}()
}

func (c *Controller) OnAdd(obj interface{}) {
	ing, ok := obj.(*networkingv1.Ingress)
	if !ok {
		return
	}

	log.Info().Str("name", ing.Name).Str("namespace", ing.Namespace).
		Msg("Ingress added")

	c.pushConfig(ing)
}

func (c *Controller) OnUpdate(oldObj, newObj interface{}) {
	oldIng, ok := oldObj.(*networkingv1.Ingress)
	if !ok {
		return
	}

	log.Info().Str("name", oldIng.Name).Str("namespace", oldIng.Namespace).
		Msg("Ingress updated")

	c.pushConfig(newObj.(*networkingv1.Ingress))
}

func (c *Controller) OnDelete(obj interface{}) {
	ing, ok := obj.(*networkingv1.Ingress)
	if !ok {
		return
	}

	log.Info().Str("name", ing.Name).Str("namespace", ing.Namespace).
		Msg("Ingress deleted")
}

func (c *Controller) pushConfig(ing *networkingv1.Ingress) {
	configs := converter.ConvertIngress(ing)

	for _, cfg := range configs {
		log.Info().
			Str("domain", cfg.Domain).
			Str("upstream", cfg.Upstream).
			Msg("Pushing config to WAF")
	}

	if err := c.pusher.PushVHosts(nil, ""); err != nil {
		log.Error().Err(err).Msg("Failed to push config to WAF")
	}
}

func (c *Controller) Stop() {
	close(c.stopChan)

	if c.election != nil {
		c.election.Shutdown()
	}

	c.mu.Lock()
	c.running = false
	c.mu.Unlock()

	log.Info().Msg("Ingress controller stopped")
}
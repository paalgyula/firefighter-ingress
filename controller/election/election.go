package election

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"
	coordinationv1 "k8s.io/api/coordination/v1"
)

type OnStartedLeading func()
type OnStoppedLeading func()

type LeaderElector struct {
	mu         sync.RWMutex
	clientset  kubernetes.Interface
	lockName   string
	namespace string
	identity  string
	onStarted  OnStartedLeading
	onStopped OnStoppedLeading
	isLeader  bool
	ctx       context.Context
	cancel    context.CancelFunc
}

func New(clientset kubernetes.Interface, name string) (*LeaderElector, error) {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = name
	}

	return &LeaderElector{
		clientset:  clientset,
		lockName:  name,
		namespace: "firefighter",
		identity:  hostname,
	}, nil
}

func (e *LeaderElector) WithCallbacks(onStarted OnStartedLeading, onStopped OnStoppedLeading) *LeaderElector {
	e.onStarted = onStarted
	e.onStopped = onStopped
	return e
}

func (e *LeaderElector) Run(ctx context.Context) {
	e.ctx, e.cancel = context.WithCancel(ctx)

	if err := e.acquireLease(e.ctx); err != nil {
		log.Warn().Err(err).Msg("Not leader, running as non-leader")
		e.isLeader = false
	} else {
		e.isLeader = true
		if e.onStarted != nil {
			e.onStarted()
		}
	}

	e.renewLoop()

	<-e.ctx.Done()

	e.releaseLease()
	if e.onStopped != nil {
		e.onStopped()
	}

	log.Info().Msg("Leader election loop ended")
}

func (e *LeaderElector) renewLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			if e.isLeader {
				_ = e.tryAcquire(e.ctx)
			}
		}
	}
}

func (e *LeaderElector) acquireLease(ctx context.Context) error {
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      e.lockName,
			Namespace: e.namespace,
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:         &e.identity,
			LeaseDurationSeconds:  new(int32),
			RenewTime:           new(metav1.MicroTime),
		},
	}
	*lease.Spec.LeaseDurationSeconds = 15
	t := metav1MicroTimeNow()
	lease.Spec.RenewTime = &t

	_, err := e.clientset.CoordinationV1().
		Leases(e.namespace).
		Create(ctx, lease, metav1.CreateOptions{})

	if err != nil {
		if !errors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to create lease: %w", err)
		}
		err = e.tryAcquire(ctx)
		if err != nil {
			return err
		}
	}

	log.Info().Str("identity", e.identity).Msg("Acquired leader lease")
	return nil
}

func metav1MicroTimeNow() metav1.MicroTime {
	return metav1.NewMicroTime(time.Now())
}

func (e *LeaderElector) tryAcquire(ctx context.Context) error {
	existing, err := e.clientset.CoordinationV1().
		Leases(e.namespace).
		Get(ctx, e.lockName, metav1.GetOptions{})

	if err != nil {
		return err
	}

	if existing.Spec.HolderIdentity != nil && *existing.Spec.HolderIdentity != e.identity {
		log.Debug().Str("holder", *existing.Spec.HolderIdentity).Msg("Another leader exists")
		return fmt.Errorf("another leader exists")
	}

	existing.Spec.HolderIdentity = &e.identity
	t := metav1MicroTimeNow()
	existing.Spec.RenewTime = &t

	_, err = e.clientset.CoordinationV1().
		Leases(e.namespace).
		Update(ctx, existing, metav1.UpdateOptions{})

	return err
}

func (e *LeaderElector) releaseLease() {
	e.mu.Lock()
	e.isLeader = false
	e.mu.Unlock()

	log.Info().Str("identity", e.identity).Msg("Released leader lease")
}

func (e *LeaderElector) IsLeader() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.isLeader
}

func (e *LeaderElector) Shutdown() {
	if e.cancel != nil {
		e.cancel()
	}
}
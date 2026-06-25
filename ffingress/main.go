package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/paalgyula/firefighter-ingress/controller"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	gitSha    string
	gitBranch string
	version   string
)

var (
	rootCmd = &cobra.Command{
		Use:   "ffingress",
		Short: "Firefighter Ingress Controller",
		Long: `Firefighter Ingress Controller watches Kubernetes Ingress and VirtualServer resources
and pushes configuration to the Firefighter WAF proxy.`,
	}
)

var (
	kubeconfig    string
	wafAddress  string
	ingressClass string
	leaderElect bool
	crdEnabled bool
	metricsAddr string
	probeAddr   string
)

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&kubeconfig, "kubeconfig", "",
		"Path to kubeconfig file")
	rootCmd.PersistentFlags().StringVar(&wafAddress, "waf-address", "localhost:50051",
		"WAF gRPC address")
	rootCmd.PersistentFlags().StringVar(&ingressClass, "ingress-class", "firefighter",
		"IngressClass name to watch")
	rootCmd.PersistentFlags().BoolVar(&leaderElect, "leader-elect", true,
		"Enable leader election")
	rootCmd.PersistentFlags().BoolVar(&crdEnabled, "crd-enabled", true,
		"Enable CRD support (VirtualServer, Policy)")
	rootCmd.PersistentFlags().StringVar(&metricsAddr, "metrics-address", ":8081",
		"Metrics server address")
	rootCmd.PersistentFlags().StringVar(&probeAddr, "health-address", ":8082",
		"Health probe server address")
}

func initConfig() {
	zerolog.TimeFieldFormat = time.RFC3339
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	consoleWriter := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	log.Logger = log.Output(consoleWriter)
}

func runIngress(cmd *cobra.Command, args []string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if version != "" {
		log.Info().Str("version", version).Str("git_sha", gitSha).Str("branch", gitBranch).
			Msg("Starting Firefighter Ingress Controller")
	}

	kconfig, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to build kubeconfig")
	}

	clientset, err := kubernetes.NewForConfig(kconfig)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create kubernetes client")
	}

	ctrl, err := controller.New(controller.Config{
		Clientset:     clientset,
		WAFAddress:   wafAddress,
		IngressClass: ingressClass,
		LeaderElect: leaderElect,
		CRDEnabled:  crdEnabled,
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create controller")
	}

	if err := ctrl.Start(ctx); err != nil {
		log.Fatal().Err(err).Msg("Controller failed to start")
	}

	go func() {
		http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
		})
		http.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
		})
		log.Info().Str("listen", probeAddr).Msg("Health server starting")
		if err := http.ListenAndServe(probeAddr, nil); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("Health server error")
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Info().Msg("Shutting down controller...")
	ctrl.Stop()
}

func main() {
	rootCmd.Run = runIngress
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
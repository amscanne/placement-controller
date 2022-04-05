// Package server implements the basic server primitives.
package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/amscanne/placement-controller/pkg/controller"
	"github.com/amscanne/placement-controller/pkg/metrics"
	"github.com/amscanne/placement-controller/pkg/options"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	compute "google.golang.org/api/compute/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	kubeclientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	// Required for GKE authentication.
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
)

const (
	gceComputeAPIEndpoint = "https://www.googleapis.com/compute/v1/"
)

func startMetricsHTTP(port int) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		promhttp.Handler().ServeHTTP(w, r)
	}))
	server := &http.Server{
		Addr:    net.JoinHostPort("", strconv.Itoa(port)),
		Handler: mux,
	}
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("Failed to start metrics HTTP handler: %s", err)
	}
}

func createLeaderElector(
	opts *options.Options,
	kubeconfig *restclient.Config,
	onStartedLeading func(context.Context),
	onStoppedLeading func(),
) (*leaderelection.LeaderElector, error) {
	// Create leader election client.
	clientset, err := kubeclientset.NewForConfig(restclient.AddUserAgent(kubeconfig, "leader-election"))
	if err != nil {
		return nil, err
	}

	// Generate a unique identifier.
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("failed to get hostname used for leader election: %v", err)
	}
	id := hostname + "_" + string(uuid.NewUUID())

	// Generate a resource lock.
	resourceLock, err := resourcelock.New(
		opts.LeaderElectionConfig.ResourceLock,
		opts.LeaseNamespace,
		"placement-controller",
		clientset.CoreV1(),
		clientset.CoordinationV1(),
		resourcelock.ResourceLockConfig{
			Identity: id,
		})
	if err != nil {
		return nil, fmt.Errorf("couldn't create resource lock: %v", err)
	}

	leaderElectionConfig := leaderelection.LeaderElectionConfig{
		Lock:          resourceLock,
		LeaseDuration: opts.LeaderElectionConfig.LeaseDuration.Duration,
		RenewDeadline: opts.LeaderElectionConfig.RenewDeadline.Duration,
		RetryPeriod:   opts.LeaderElectionConfig.RetryPeriod.Duration,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: onStartedLeading,
			OnStoppedLeading: onStoppedLeading,
		},
	}
	return leaderelection.NewLeaderElector(leaderElectionConfig)
}

// wrapComputeService is an implementation of controller.ComputeService.
type wrapComputeService compute.Service

// RemoveResourcePolicies implements controller.ComputeService.RemoveResourcePolicies.
func (cs *wrapComputeService) RemoveResourcePolicies(project string, zone string, instance string, request *compute.InstancesRemoveResourcePoliciesRequest) error {
	s := (*compute.Service)(cs)
	op := s.Instances.RemoveResourcePolicies(project, zone, instance, request)
	_, err := op.Do()
	return err
}

// GetResourcePolicy implements controller.ComputeService.GetResourcePolicy.
func (cs *wrapComputeService) GetResourcePolicy(project string, zone string, instance string) (string, error) {
	s := (*compute.Service)(cs)
	op := s.Instances.Get(project, zone, instance)
	resp, err := op.Do()
	if err != nil {
		return "", err
	}
	if len(resp.ResourcePolicies) == 0 {
		return "", nil
	}
	return resp.ResourcePolicies[0], nil // At most one.
}

// AddResourcePolicies implements controller.ComputeService.AddResourcePolicies.
func (cs *wrapComputeService) AddResourcePolicies(project string, zone string, instance string, request *compute.InstancesAddResourcePoliciesRequest) error {
	s := (*compute.Service)(cs)
	op := s.Instances.AddResourcePolicies(project, zone, instance, request)
	_, err := op.Do()
	return err
}

// SimulateMaintenanceEvent implements controller.ComputeService.SimulateMaintenanceEvent.
func (cs *wrapComputeService) SimulateMaintenanceEvent(project string, zone string, instance string) error {
	s := (*compute.Service)(cs)
	op := s.Instances.SimulateMaintenanceEvent(project, zone, instance)
	_, err := op.Do()
	return err
}

// wallTime is an implementation of controller.Timer.
type wallTime struct{}

// After implements controller.Timer.After.
func (wt wallTime) After(_ string, d time.Duration) <-chan time.Time {
	return time.After(d)
}

// Run runs the placement controller with the given options.
func Run(opts *options.Options) error {
	// Create kubernetes client.
	config, err := restclient.InClusterConfig()
	if err != nil {
		return fmt.Errorf("error constructing config: %w", err)
	}
	kubeClientset, err := kubeclientset.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("error creating kube client: %w", err)
	}

	// Create the GCE client.
	computeClient, err := compute.NewService(context.Background())
	if err != nil {
		return fmt.Errorf("error creating compute client: %w", err)
	}

	// Start the service.
	var (
		wg     sync.WaitGroup
		cancel context.CancelFunc
	)
	startLeading := func(ctx context.Context) {
		// Note that this assumptions cancelation progagation from the
		// context passed here. This just wraps additional cancelation
		// for the leader election semantics.
		ctx, cancel = context.WithCancel(ctx)
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := controller.New(controller.Options{
				KubeClient:     kubeClientset,
				ComputeService: (*wrapComputeService)(computeClient),
				Timer:          wallTime{},
				Metrics: controller.Metrics{
					Requested:   metrics.RequestedPolicies,
					Applied:     metrics.AppliedPolicies,
					AddPolicy:   metrics.AddPolicyDuration,
					ApplyPolicy: metrics.ApplyPolicyDuration,
				},
				ClusterProject: opts.ClusterProject,
			})
			c.Run(ctx)
		}()
	}
	stopLeading := func() {
		cancel()
		wg.Wait()
	}

	// Main context, used below.
	ctx, cancel := context.WithCancel(context.Background())

	// Start peripheral services.
	go startMetricsHTTP(opts.Port)

	// Run with a leader election, or on our own.
	if opts.LeaderElectionConfig.LeaderElect {
		leaderElector, err := createLeaderElector(opts, config, startLeading, stopLeading)
		if err != nil {
			cancel()
			return fmt.Errorf("failed to create leader elector: %v", err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return // Cancelled.
				default:
					leaderElector.Run(ctx)
				}
			}
		}()
	} else {
		go startLeading(ctx)
	}

	// Block until we need to shutdown.
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT)
	<-signalChan // Wait for a signal.
	log.Printf("Signal received, attempting graceful shutdown.")
	signal.Reset() // Allow signals to kill the process.
	stopLeading()  // May be called redundantly.

	return nil
}

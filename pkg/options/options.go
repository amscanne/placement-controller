// Package options provides the placement controller options.
package options

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"golang.org/x/oauth2/google"
	compute "google.golang.org/api/compute/v1"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/component-base/config"
)

// Options is the set of common options.
type Options struct {
	// Port is the port which the metrics server will listen on.
	Port int

	// LeaseNamespace is the namespace to use for leader election.
	LeaseNamespace string

	// ClusterProject is the project used for the cluster, which controls the
	// default project used for placement policies.
	ClusterProject string

	// LeaderElectionConfig is the leader election configuration.
	LeaderElectionConfig config.LeaderElectionConfiguration
}

const defaultPort = 8000

var defaultNamespace = func() string {
	return "placement-controller"
}

var defaultProject = func() string {
	credentials, err := google.FindDefaultCredentials(context.Background(), compute.ComputeScope)
	if err != nil {
		return ""
	}
	return credentials.ProjectID
}

// AddFlags adds flags for a specific TPUOperator to the specified FlagSet.
func (o *Options) AddFlags(fs *flag.FlagSet) {
	// Controller-specific flasg.
	fs.IntVar(&o.Port, "port", defaultPort, "The port that the controller will listen on.")
	fs.StringVar(&o.ClusterProject, "cluster-project", defaultProject(), "The cluster project to use.")
	fs.StringVar(&o.LeaseNamespace, "lease-namespace", defaultNamespace(), "The lease namespace.")

	// Standard leader election flags.
	fs.BoolVar(&o.LeaderElectionConfig.LeaderElect, "leader-elect", false, "Enable leader election.")
	fs.DurationVar(&o.LeaderElectionConfig.LeaseDuration.Duration, "leader-elect-lease-duration", 60*time.Second,
		"The duration that non-leader candidates will wait after observing a leadership renewal until attempting to acquire.")
	fs.DurationVar(&o.LeaderElectionConfig.RenewDeadline.Duration, "leader-elect-renew-deadline", 15*time.Second,
		"The interval between acquisition attempts.")
	fs.DurationVar(&o.LeaderElectionConfig.RetryPeriod.Duration, "leader-elect-retry-period", 5*time.Second,
		"The duration the clients should wait between acquisition attempts.")
	fs.StringVar(&o.LeaderElectionConfig.ResourceLock, "leader-elect-resource-lock", resourcelock.LeasesResourceLock,
		"The type of resource object that is used for locking.")
}

// Validate validates the given options.
func (o *Options) Validate() error {
	if o.Port <= 0 || o.Port > 65535 {
		return fmt.Errorf("invalid port number: %d", o.Port)
	}
	if o.ClusterProject == "" {
		return errors.New("no cluster-project provided")
	}
	return nil
}

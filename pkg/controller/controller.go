// Package controller implements basic placement controller logic.
package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/amscanne/placement-controller/pkg/metrics"
	compute "google.golang.org/api/compute/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

const (
	regionLabel    = "topology.kubernetes.io/region"
	zoneLabel      = "topology.kubernetes.io/zone"
	requestedLabel = "requested-placement-policy"
	appliedLabel   = "applied-placement-policy"
)

var patchOptions = v1.PatchOptions{
	FieldManager: "placement_controller",
}

// ComputeService is the interface used for manipulating instances.
type ComputeService interface {
	GetResourcePolicy(project string, zone string, instance string) (string, error)
	RemoveResourcePolicies(project string, zone string, instance string, request *compute.InstancesRemoveResourcePoliciesRequest) error
	AddResourcePolicies(project string, zone string, instance string, request *compute.InstancesAddResourcePoliciesRequest) error
	SimulateMaintenanceEvent(project string, zone string, instance string) error
}

// Timer is used for the passage of time.
type Timer interface {
	After(instance string, d time.Duration) <-chan time.Time
}

// MetricGauge is a gauge-style metric.
type MetricGauge interface {
	Inc(zone string)
	Dec(zone string)
}

// MetricDistribution is a distribution-style metric.
type MetricDistribution interface {
	Observe(zone string, d time.Duration)
}

// Metrics is a metrics wrapper.
type Metrics struct {
	Requested   MetricGauge
	Applied     MetricGauge
	AddPolicy   MetricDistribution
	ApplyPolicy MetricDistribution
}

// Options are Controller options.
type Options struct {
	KubeClient     kubernetes.Interface
	ComputeService ComputeService
	Timer          Timer
	Metrics        Metrics
	ClusterProject string
}

// labelInfo combines a label with dimensions of interest.
type labelInfo struct {
	zone  string
	value string
}

// cachedMetric is a wrapper around a gauge.
type cachedMetric struct {
	mu    sync.Mutex
	cache map[string]labelInfo
}

// update updates the metric and cache.
//
// True if returned iff there is any change from cached state.
func (c *cachedMetric) update(n *corev1.Node, value string, gauge MetricGauge) {
	c.mu.Lock()
	defer c.mu.Unlock()
	old, ok := c.cache[n.Name]
	zone := n.Labels[zoneLabel]
	if value == "" && ok {
		delete(c.cache, n.Name)
		gauge.Dec(zone)
	} else if value != "" && !ok {
		c.cache[n.Name] = labelInfo{
			value: value,
			zone:  zone,
		}
		gauge.Inc(zone)
	} else if value != "" && ok {
		// There was already an active request. The policy may
		// change, but we don't need to update the zone metrics,
		// since this is not permitted to change for an instance.
		old.value = value
		c.cache[n.Name] = old
	}
}

// Controller implements the main controller loop.
type Controller struct {
	// options is immutable and should not be changed.
	Options

	// outstandingMu protects outstanding, which indicates which nodse currently
	// have any outstanding goroutines trying to synchronize their state.
	//
	// outstandingCond is signaled every time outstanding changes.
	outstandingMu   sync.Mutex
	outstandingCond sync.Cond
	outstanding     map[string]chan struct{}

	// requested is a metric for requested labels.
	requested cachedMetric

	// applied is a metric for applied labels.
	applied cachedMetric
}

// New returns a new Controller.
func New(options Options) *Controller {
	c := &Controller{
		Options:     options,
		outstanding: make(map[string]chan struct{}),
		requested:   cachedMetric{cache: make(map[string]labelInfo)},
		applied:     cachedMetric{cache: make(map[string]labelInfo)},
	}
	c.outstandingCond.L = &c.outstandingMu
	return c
}

// stopAll stops all outstanding requests.
func (c *Controller) stopAll() {
	c.outstandingMu.Lock()
	defer c.outstandingMu.Unlock()
	for name, stopChan := range c.outstanding {
		if stopChan != nil {
			// Clear the channel.
			c.outstanding[name] = nil
			close(stopChan)
		}
	}
	for len(c.outstanding) > 0 {
		c.outstandingCond.Wait()
	}
}

// startOne starts one node update.
func (c *Controller) startOne(n *corev1.Node) {
	c.stopOne(n) // Stop anything running.
	c.outstandingMu.Lock()
	defer c.outstandingMu.Unlock()
	for {
		stopCh, ok := c.outstanding[n.Name]
		if ok {
			if stopCh == nil {
				// If this channel is nil, then it has already been closed, and
				// we are simply waiting for the goroutine to complete. This might
				// happen if something else has triggered a stop also.
				c.outstandingCond.Wait()
				continue
			}
			// The update is already running. Note that we previously kicked
			// the channel to reset the backoff, but since the labels haven't
			// changed, we no longer do this.
			return
		}
		// Note that this channel must be buffered to support the kick logic
		// directly above, without needing to release the lock and avoiding
		// deadlock during goroutine stop.
		stopCh = make(chan struct{}, 1)
		c.outstanding[n.Name] = stopCh
		go c.update(n, stopCh)
		return
	}
}

// stopOne stops one node update.
func (c *Controller) stopOne(n *corev1.Node) {
	c.outstandingMu.Lock()
	defer c.outstandingMu.Unlock()
	for {
		stopCh, ok := c.outstanding[n.Name]
		if ok {
			if stopCh == nil {
				// We are already waiting for this to stop.
				c.outstandingCond.Wait()
				continue
			}
			// Close the channel, and wait for it to finish. See above, where
			// this also may be done to stop an update.
			c.outstanding[n.Name] = nil
			close(stopCh)
			continue
		}
		// The channel is already gone.
		return
	}
}

// finishOne clears the node from outstanding.
func (c *Controller) finishOne(n *corev1.Node) {
	c.outstandingMu.Lock()
	defer c.outstandingMu.Unlock()
	stopCh, ok := c.outstanding[n.Name]
	if !ok {
		// This should still be there when this is called.
		log.Fatalf("Expected entry in c.outstanding, found none.")
	}
	// N.B. The log format here aligns with the updateOne format, so that you can look
	// at the logs and clearly thread information about relevant nodes.
	if stopCh != nil {
		log.Printf("[node %s] Completed gracefully.", n.Name)
	} else {
		log.Printf("[node %s] Stopped forcefully.", n.Name)
	}
	delete(c.outstanding, n.Name)
	c.outstandingCond.Broadcast()
}

// inferFullPolicy infers the full policy name.
func inferFullPolicy(n *corev1.Node, defaultProject string, policy string) (string, error) {
	if policy == "" {
		return "", nil // No policy.
	}
	if strings.HasPrefix(policy, "https://") {
		return policy, nil // Fully-qualified.
	}
	parts := strings.Split(policy, "/")
	switch len(parts) {
	case 1:
		return fmt.Sprintf("projects/%s/regions/%s/resourcePolicies/%s", defaultProject, n.Labels[regionLabel], parts[0]), nil
	case 2:
		return fmt.Sprintf("projects/%s/regions/%s/resourcePolicies/%s", parts[0], n.Labels[regionLabel], parts[1]), nil
	case 3:
		return fmt.Sprintf("projects/%s/regions/%s/resourcePolicies/%s", parts[0], parts[1], parts[2]), nil

	case 6:
		// Check for a full specification.
		if parts[0] == "projects" && parts[2] == "regions" && parts[4] == "resourcePolicies" {
			return policy, nil
		}
		fallthrough
	default:
		return "", fmt.Errorf("invalid policy specification: %s", policy)
	}
}

// setPlacementPolicy sets the placement policy on the given node.
func (c *Controller) setPlacementPolicy(n *corev1.Node, oldPolicy, newPolicy string) error {
	// Convert to the fully-qualified policy.
	fullOldPolicy, err := inferFullPolicy(n, c.Options.ClusterProject, oldPolicy)
	if err != nil {
		return err
	}
	fullNewPolicy, err := inferFullPolicy(n, c.Options.ClusterProject, newPolicy)
	if err != nil {
		return err
	}

	// Attempt to add/set the resource policy.
	startTime := time.Now()
	if fullOldPolicy != "" {
		request := compute.InstancesRemoveResourcePoliciesRequest{}
		request.ResourcePolicies = append(request.ResourcePolicies, fullOldPolicy)
		if err := c.ComputeService.RemoveResourcePolicies(c.ClusterProject, n.Labels[zoneLabel], n.Name, &request); err != nil {
			return fmt.Errorf("unable to set placement policy: %w", err)
		}
	}
	if fullNewPolicy != "" {
		request := compute.InstancesAddResourcePoliciesRequest{}
		request.ResourcePolicies = append(request.ResourcePolicies, fullNewPolicy)
		if err := c.ComputeService.AddResourcePolicies(c.ClusterProject, n.Labels[zoneLabel], n.Name, &request); err != nil {
			return fmt.Errorf("unable to set placement policy: %w", err)
		}
	}
	addTime := time.Since(startTime)
	c.Metrics.AddPolicy.Observe(n.Labels[zoneLabel], addTime)

	// If the policy is non-nil, trigger a maintenance event.
	if fullNewPolicy != "" {
		if err := c.ComputeService.SimulateMaintenanceEvent(c.ClusterProject, n.Labels[zoneLabel], n.Name); err != nil {
			c.setPlacementPolicy(n, "", oldPolicy) // Reset the previous policy.
			return fmt.Errorf("unable to simulate a maintenance event: %w", err)
		}
		applyTime := time.Since(startTime)
		c.Metrics.ApplyPolicy.Observe(n.Labels[zoneLabel], applyTime)
	}

	return nil
}

type jsonPatch struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value string `json:"value,omitempty"`
}

// encodeForPatch encodes a URL for a json patch key.
//
// JSONPatch uses a bizarre custom schema for formatting, described here:
// https://datatracker.ietf.org/doc/html/rfc6901#section-3
func encodeForPatch(v string) string {
	v = strings.Replace(v, "~", "~0", -1)
	v = strings.Replace(v, "/", "~1", -1)
	return v
}

// addNodeLabel adds the given node label.
func (c *Controller) addNodeLabel(n *corev1.Node, label, value string) error {
	patch := []jsonPatch{
		{
			Op:    "add",
			Path:  fmt.Sprintf("/metadata/labels/%s", encodeForPatch(label)),
			Value: value,
		},
	}
	b, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	if _, err := c.KubeClient.CoreV1().Nodes().Patch(context.Background(), n.Name, types.JSONPatchType, b, patchOptions); err != nil {
		return err
	}
	n.Labels[label] = value
	return nil
}

// removeNodeLabel remove the given node label.
func (c *Controller) removeNodeLabel(n *corev1.Node, label string) error {
	patch := []jsonPatch{
		{
			Op:   "remove",
			Path: fmt.Sprintf("/metadata/labels/%s", encodeForPatch(label)),
		},
	}
	b, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	if _, err := c.KubeClient.CoreV1().Nodes().Patch(context.Background(), n.Name, types.JSONPatchType, b, patchOptions); err != nil {
		return err
	}
	delete(n.Labels, label)
	return nil
}

// updateNodeLabel updates the given node label.
func (c *Controller) updateNodeLabel(n *corev1.Node, label, value string) error {
	patch := []jsonPatch{
		{
			Op:    "replace",
			Path:  fmt.Sprintf("/metadata/labels/%s", encodeForPatch(label)),
			Value: value,
		},
	}
	b, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	if _, err := c.KubeClient.CoreV1().Nodes().Patch(context.Background(), n.Name, types.JSONPatchType, b, patchOptions); err != nil {
		return err
	}
	n.Labels[label] = value
	return nil
}

// See update, below.
const (
	minBackoff = time.Second
	maxBackoff = time.Hour * 24 * 7
)

// update is the main node update logic.
func (c *Controller) update(n *corev1.Node, stopCh chan struct{}) {
	log.Printf("[node %s] Starting update.", n.Name)
	defer c.finishOne(n)

	// Start the exponential backoff at one second. Increase it up to one week between attempts, in
	// order to handle a degenerate case where there are tons of nodes with the annotation and none
	// can be applied successfully.
	var backoff = minBackoff

	// Check for a requested placement policy.
	requestedPolicy, requestedPolicyOk := n.Labels[requestedLabel]
	appliedPolicy, appliedPolicyOk := n.Labels[appliedLabel]

	// Ensure metrics are up-to-date.
	c.requested.update(n, n.Labels[requestedLabel], c.Metrics.Requested)
	c.applied.update(n, n.Labels[appliedLabel], c.Metrics.Applied)

	// Is there anything to do?
	if !requestedPolicyOk && !appliedPolicyOk {
		log.Printf("[node %s] No policy requested or applied.", n.Name)
		return // Nothing exists, normal case.
	}
	if requestedPolicyOk && appliedPolicyOk && requestedPolicy == appliedPolicy {
		log.Printf("[node %s] The requested policy is already applied.", n.Name)
		return // Both are valid and applied.
	}
	if requestedPolicyOk && requestedPolicy == "" && !appliedPolicyOk {
		log.Printf("[node %s] The requested policy is empty, and nothing is applied.", n.Name)
		return // Expected outcome is that we delete the applied policy.
	}

	for {
		// Update to the requested policy.
		policyErr := c.setPlacementPolicy(n, appliedPolicy, requestedPolicy)
		if policyErr != nil {
			// Policy set failed.
			log.Printf("[node %s] Failed to set policy: %v", n.Name, policyErr)

			// In this case, the policy set may have failed due to bad information about
			// what policy has been applied to this instance. If necessary, update the
			// node label to reflect what's actually been applied. This will be picked up
			// in a subsequent cycle of this loop, since we update the node object.
			currentPolicy, err := c.ComputeService.GetResourcePolicy(c.ClusterProject, n.Labels[zoneLabel], n.Name)
			if err == nil {
				// Use the requestedPolicy alias if this
				// matches, it likely means that we will have
				// no work left work do.
				if fullPolicy, err := inferFullPolicy(n, c.ClusterProject, requestedPolicy); err == nil && currentPolicy == fullPolicy {
					currentPolicy = requestedPolicy
				}
				if appliedPolicy != "" && currentPolicy == "" {
					labelErr := c.removeNodeLabel(n, appliedLabel)
					if labelErr == nil {
						// Valid label updated, allow the controller to restart.
						log.Printf("[node %s] Policy label removed.", n.Name)
						c.applied.update(n, currentPolicy, metrics.AppliedPolicies)
						return
					}
					// We did not remove the invalid label.
					log.Printf("[node %s] Failed to remove invalid applied label: %v", n.Name, labelErr)
				} else if appliedPolicyOk && appliedPolicy != currentPolicy {
					labelErr := c.updateNodeLabel(n, appliedLabel, currentPolicy)
					if labelErr == nil {
						// Valid label updated, allow the controller to restart.
						log.Printf("[node %s] Policy label updated.", n.Name)
						c.applied.update(n, currentPolicy, metrics.AppliedPolicies)
						return
					}
					// We could not update the invalid label.
					log.Printf("[node %s] Failed to update invalid applied label: %v", n.Name, labelErr)
				} else if !appliedPolicyOk && currentPolicy != "" {
					labelErr := c.addNodeLabel(n, appliedLabel, currentPolicy)
					if labelErr == nil {
						// Valid label updated, allow the controller to restart.
						log.Printf("[node %s] Policy label added.", n.Name)
						c.applied.update(n, currentPolicy, metrics.AppliedPolicies)
						return
					}
					// We could not add the proper label.
					log.Printf("[node %s] Failed to update invalid applied label: %v", n.Name, labelErr)
				}
			}
		} else {
			// Apply the applied applied label, if successful.
			if requestedPolicy == "" && appliedPolicyOk {
				labelErr := c.removeNodeLabel(n, appliedLabel)
				if labelErr == nil {
					// Successfully remove the policy, update our metrics.
					log.Printf("[node %s] Policy successfully removed.", n.Name)
					c.applied.update(n, appliedPolicy, metrics.AppliedPolicies)
					return
				}
				// The policy has applied fully, but we don't have the label.
				log.Printf("[node %s] Failed to remove policy: %v", n.Name, labelErr)
			} else if requestedPolicy != "" && appliedPolicyOk {
				labelErr := c.updateNodeLabel(n, appliedLabel, requestedPolicy)
				if labelErr == nil {
					// Successfully changed the policy, update our metrics (should be no-op).
					log.Printf("[node %s] Policy successfully changed to %s.", n.Name, requestedPolicy)
					c.applied.update(n, requestedPolicy, metrics.AppliedPolicies)
					return
				}
				// See above: the policy is applied.
				log.Printf("[node %s] Failed to change policy: %v", n.Name, labelErr)
			} else if requestedPolicy != "" && !appliedPolicyOk {
				labelErr := c.addNodeLabel(n, appliedLabel, requestedPolicy)
				if labelErr == nil {
					// Successfully added the policy, update our metrics.
					log.Printf("[node %s] Policy successfully added as %s.", n.Name, requestedPolicy)
					c.applied.update(n, requestedPolicy, metrics.AppliedPolicies)
					return
				}
				// See above: the policy is applied.
				log.Printf("[node %s] Failed to add policy: %v", n.Name, labelErr)
			}
		}

		// Wait for the backoff interval or cancelation.
		log.Printf("[node %s] Waiting %v to attempt update again...", n.Name, backoff)
		select {
		case <-c.Timer.After(n.Name, backoff):
			// Double the backoff for next time.
			backoff = backoff * 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		case _, ok := <-stopCh:
			// If the channel is kicked but not closed, then this indicates that we should
			// only reset the expoential backoff for this note.
			if ok {
				backoff = minBackoff
				break
			}
			// If the channel is closed, then we finish.
			return
		}
	}
}

// Run runs the Controller.
func (c *Controller) Run(ctx context.Context) {
	defer c.stopAll()
	informer := cache.NewSharedInformer(
		&cache.ListWatch{
			ListFunc: func(options v1.ListOptions) (runtime.Object, error) {
				return c.KubeClient.CoreV1().Nodes().List(context.Background(), options)
			},
			WatchFunc: func(options v1.ListOptions) (watch.Interface, error) {
				return c.KubeClient.CoreV1().Nodes().Watch(context.Background(), options)
			},
		}, &corev1.Node{}, time.Hour)
	informer.AddEventHandler(c)
	log.Printf("Controller started.")
	informer.Run(ctx.Done())
	log.Printf("Controller stopped.")
}

// OnAdd implements ResourceEventHandler.OnAdd.
func (c *Controller) OnAdd(obj interface{}) {
	c.startOne(obj.(*corev1.Node))
}

// OnUpdate implements ResourceEventHandler.OnUpdate.
func (c *Controller) OnUpdate(oldObj, newObj interface{}) {
	oldN := oldObj.(*corev1.Node)
	newN := newObj.(*corev1.Node)

	// Start an update only if labels changed.
	requestedChanged := oldN.Labels[requestedLabel] != newN.Labels[requestedLabel]
	appliedChanged := newN.Labels[appliedLabel] != newN.Labels[appliedLabel]
	if requestedChanged || appliedChanged {
		c.startOne(newN)
	}
}

// OnDelete implmenets ResourceEventHandler.OnDelete.
func (c *Controller) OnDelete(obj interface{}) {
	c.stopOne(obj.(*corev1.Node))
}

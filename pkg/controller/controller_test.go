package controller

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	compute "google.golang.org/api/compute/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	fakev1 "k8s.io/client-go/kubernetes/typed/core/v1/fake"
)

// fakeInstance represents a single fake compute instance.
type fakeInstance struct {
	mu             sync.Mutex
	project        string
	region         string
	zone           string
	name           string
	policy         string
	policyErr      error
	maintenanceErr error
}

// fakeComputeService represents an aggregate compute service.
type fakeComputeService struct {
	instances []*fakeInstance
	policies  map[string]int // Fully-qualified with project.
}

// GetResourcePolicy implements ComputeService.GetResourcePolicy.
func (f *fakeComputeService) GetResourcePolicy(project string, zone string, name string) (string, error) {
	for _, instance := range f.instances {
		if instance.project == project && instance.zone == zone && instance.name == name {
			instance.mu.Lock()
			defer instance.mu.Unlock()
			return instance.policy, nil
		}
	}
	return "", fmt.Errorf("no instances matching %s for project %q in zone %s", name, project, zone)
}

// RemoveResourcePolicies implements ComputeService.RemoveResourcePolicies.
func (f *fakeComputeService) RemoveResourcePolicies(project string, zone string, name string, request *compute.InstancesRemoveResourcePoliciesRequest) error {
	for _, instance := range f.instances {
		if instance.project == project && instance.zone == zone && instance.name == name {
			instance.mu.Lock()
			defer instance.mu.Unlock()
			if instance.policyErr != nil {
				return instance.policyErr
			}
			if len(request.ResourcePolicies) == 0 {
				return nil
			}
			if len(request.ResourcePolicies) != 1 {
				return fmt.Errorf("invalid remove request: %d policies set.", len(request.ResourcePolicies))
			}
			if request.ResourcePolicies[0] != instance.policy {
				return fmt.Errorf("[node %s] invalid remove request for %s, existing policy is %s.", name, request.ResourcePolicies[0], instance.policy)
			}
			log.Printf("[node %s] clearing fake policy.", name)
			instance.policy = ""
			return nil
		}
	}
	return fmt.Errorf("no instances matching %s for project %q in zone %s", name, project, zone)
}

// AddResourcePolicies implements ComputeService.AddResourcePolicies.
func (f *fakeComputeService) AddResourcePolicies(project string, zone string, name string, request *compute.InstancesAddResourcePoliciesRequest) error {
	for _, instance := range f.instances {
		if instance.project == project && instance.zone == zone && instance.name == name {
			instance.mu.Lock()
			defer instance.mu.Unlock()
			if instance.policyErr != nil {
				return instance.policyErr
			}
			if len(request.ResourcePolicies) == 0 {
				return nil
			}
			if len(request.ResourcePolicies) != 1 {
				return fmt.Errorf("invalid add request: %d policies set.", len(request.ResourcePolicies))
			}
			if instance.policy != "" {
				return fmt.Errorf("[node %s] invalid add request for %s, existing policy is %s.", name, request.ResourcePolicies[0], instance.policy)
			}
			log.Printf("[node %s] setting fake policy to %s.", name, request.ResourcePolicies[0])
			instance.policy = request.ResourcePolicies[0]
			return nil
		}
	}
	return fmt.Errorf("no instances matching %s for project %q in zone %s", name, project, zone)
}

// AddResourcePolicies implements ComputeService.SimulateMaintenanceEvent.
func (f *fakeComputeService) SimulateMaintenanceEvent(project string, zone string, name string) error {
	for _, instance := range f.instances {
		if instance.project == project && instance.zone == zone && instance.name == name {
			instance.mu.Lock()
			defer instance.mu.Unlock()
			return instance.maintenanceErr
		}
	}
	return fmt.Errorf("no instances for project %q", project)
}

// fakeTimer is an implement of timers for testing.
type fakeTimer struct {
	mu      sync.Mutex
	cond    sync.Cond
	kick    map[string]chan time.Time
	started map[string]time.Duration
}

// After implements Timer.After.
func (f *fakeTimer) After(name string, d time.Duration) <-chan time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Record the intended time & register the channel.
	rc := make(chan time.Time, 1)
	f.started[name] = d
	f.kick[name] = rc
	f.cond.Broadcast()

	return rc
}

// Kick kicks the given instance.
func (f *fakeTimer) Kick(name string, fn func(time.Duration)) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Wait for the name.
	for {
		if rc, ok := f.kick[name]; ok {
			d := f.started[name]
			fn(d) // With lock held, caller blocked.
			rc <- time.Now()
			close(rc)
			delete(f.started, name)
			delete(f.kick, name)
			return
		}
		f.cond.Wait()
	}
}

// fakeMetricGauge is a fake gauage.
type fakeGauge struct {
	mu     sync.Mutex
	values map[string]int
}

// Inc implements MetricGauge.Inc.
func (f *fakeGauge) Inc(zone string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.values[zone] = f.values[zone] + 1
}

// Dec implements MetricGauge.Dec.
func (f *fakeGauge) Dec(zone string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	old := f.values[zone]
	if old == 1 {
		delete(f.values, zone)
		return
	}
	f.values[zone] = old - 1
}

// fakeDistribution is a fake distribution.
type fakeDistribution struct {
	mu     sync.Mutex
	values map[string]time.Duration
}

// Observe implements MetricDistribution.Observe.
func (f *fakeDistribution) Observe(zone string, d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.values[zone] = f.values[zone] + d
}

const (
	testProject   = "test-project"
	testRegion    = "zone1"
	testZone1     = "zone1-a"
	testZone2     = "zone1-b"
	testInstance1 = "instance-1"
	testInstance2 = "instance-2"
	testInstance3 = "instance-3"
	testPolicy1   = "test-policy1"
	testPolicy2   = "test-policy2"
)

// newTestController returns a new test controller.
func newTestController() (c *Controller, syncfn func(), stop func()) {
	kubeClient := fake.NewSimpleClientset()
	computeService := &fakeComputeService{
		policies: make(map[string]int),
	}
	timer := &fakeTimer{
		kick:    make(map[string]chan time.Time),
		started: make(map[string]time.Duration),
	}
	timer.cond.L = &timer.mu
	c = New(Options{
		KubeClient:     kubeClient,
		ComputeService: computeService,
		Timer:          timer,
		Metrics: Metrics{
			Requested:   &fakeGauge{values: make(map[string]int)},
			Applied:     &fakeGauge{values: make(map[string]int)},
			AddPolicy:   &fakeDistribution{values: make(map[string]time.Duration)},
			ApplyPolicy: &fakeDistribution{values: make(map[string]time.Duration)},
		},
		ClusterProject: testProject,
	})

	// Start the controller.
	var (
		ctx    context.Context
		cancel func()
		wg     sync.WaitGroup
	)
	start := func() {
		ctx, cancel = context.WithCancel(context.Background())
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Run(ctx)
		}()
	}
	stop = func() {
		cancel()
		wg.Wait()
	}
	syncfn = func() {
		stop()
		start()
	}
	start()
	return c, syncfn, stop
}

// addNode adds a node.
func addNode(t *testing.T, c *Controller, instance *fakeInstance) *corev1.Node {
	t.Helper()
	var localNode corev1.Node
	localNode.SetLabels(map[string]string{
		regionLabel: instance.region,
		zoneLabel:   instance.zone,
	})
	localNode.SetName(instance.name)
	n, err := c.KubeClient.CoreV1().Nodes().(*fakev1.FakeNodes).Create(context.Background(), &localNode, v1.CreateOptions{})
	if err != nil {
		// Likely a duplicate or other test issue?
		t.Fatalf("unable to add a node %#v: %v", localNode, err)
	}
	c.ComputeService.(*fakeComputeService).instances = append(c.ComputeService.(*fakeComputeService).instances, instance)
	return n
}

// checkNoApplied asserts that no policy has been applied.
func checkNoApplied(t *testing.T, c *Controller, n *corev1.Node) {
	t.Helper()
	if _, ok := n.Labels[appliedLabel]; ok {
		t.Errorf("expected no applied label, but one is present")
	}
	for _, instance := range c.ComputeService.(*fakeComputeService).instances {
		if instance.zone == n.Labels[zoneLabel] && instance.name == n.Name {
			if instance.policy != "" {
				t.Errorf("instance has policy %s applied, but want none", instance.policy)
			}
		}
	}
}

// checkApplied asserts that the given policy has been applied.
func checkApplied(t *testing.T, c *Controller, n *corev1.Node, value string) {
	t.Helper()
	n, err := c.KubeClient.CoreV1().Nodes().Get(context.Background(), n.Name, v1.GetOptions{})
	if err != nil {
		t.Errorf("unable to refresh node: %v", err)
		return
	}
	if v, ok := n.Labels[appliedLabel]; !ok {
		t.Errorf("expected applied label, but not present: %#v", n.Labels)
	} else if v != value {
		t.Errorf("wanted applied label with value %s, got value %s", value, v)
	}
	for _, instance := range c.ComputeService.(*fakeComputeService).instances {
		if instance.zone == n.Labels[zoneLabel] && instance.name == n.Name {
			if fullPolicy, _ := inferFullPolicy(n, c.ClusterProject, value); instance.policy != fullPolicy {
				t.Errorf("instance has policy %s applied, but want %s", instance.policy, fullPolicy)
			}
		}
	}
}

func TestSingleInstance(t *testing.T) {
	c, sync, stop := newTestController()
	defer stop()
	n := addNode(t, c, &fakeInstance{
		project: testProject,
		region:  testRegion,
		zone:    testZone1,
		name:    testInstance1,
	})
	n.Labels[requestedLabel] = testPolicy1 // Request this policy for this instance.
	c.KubeClient.CoreV1().Nodes().Update(context.Background(), n, v1.UpdateOptions{})

	// Force one update.
	c.OnAdd(n)

	// Check that the label has been applied.
	sync()
	checkApplied(t, c, n, testPolicy1)
}

func TestNoUpdates(t *testing.T) {
	c, sync, stop := newTestController()
	defer stop()
	n := addNode(t, c, &fakeInstance{
		project: testProject,
		region:  testRegion,
		zone:    testZone1,
		name:    testInstance1,
	})

	// Run one update.
	c.OnAdd(n)

	// Check that no policies are applied.
	sync()
	checkNoApplied(t, c, n)
}

func TestMultipleInstances(t *testing.T) {
	c, sync, stop := newTestController()
	defer stop()
	n1 := addNode(t, c, &fakeInstance{
		project: testProject,
		region:  testRegion,
		zone:    testZone1,
		name:    testInstance1,
	})
	n2 := addNode(t, c, &fakeInstance{
		project: testProject,
		region:  testRegion,
		zone:    testZone1,
		name:    testInstance2,
	})
	n3 := addNode(t, c, &fakeInstance{
		project: testProject,
		region:  testRegion,
		zone:    testZone1,
		name:    testInstance3,
	})

	// Request policies for the instances.
	n1.Labels[requestedLabel] = testPolicy1
	n2.Labels[requestedLabel] = testPolicy1
	n3.Labels[requestedLabel] = testPolicy2
	c.KubeClient.CoreV1().Nodes().Update(context.Background(), n1, v1.UpdateOptions{})
	c.KubeClient.CoreV1().Nodes().Update(context.Background(), n2, v1.UpdateOptions{})
	c.KubeClient.CoreV1().Nodes().Update(context.Background(), n3, v1.UpdateOptions{})

	// Force one update.
	c.OnAdd(n1)
	c.OnAdd(n2)
	c.OnAdd(n3)

	// Check that the label has been applied.
	sync()
	checkApplied(t, c, n1, testPolicy1)
	checkApplied(t, c, n2, testPolicy1)
	checkApplied(t, c, n3, testPolicy2)
}

func TestSpuriousUpdates(t *testing.T) {
	c, sync, stop := newTestController()
	defer stop()
	n := addNode(t, c, &fakeInstance{
		project: testProject,
		region:  testRegion,
		zone:    testZone1,
		name:    testInstance1,
	})
	n.Labels[requestedLabel] = testPolicy1 // Request this policy for this instance.
	c.KubeClient.CoreV1().Nodes().Update(context.Background(), n, v1.UpdateOptions{})

	// These should be handled normally.
	c.OnAdd(n)
	c.OnAdd(n)
	c.OnAdd(n)

	// Check that all is as expected.
	sync()
	checkApplied(t, c, n, testPolicy1)
}

func TestFailure(t *testing.T) {
	for _, tc := range []struct {
		name           string
		policyErr      error
		maintenanceErr error
	}{
		{
			name:      "policy",
			policyErr: errors.New("unlucky!"),
		},
		{
			name:           "maintenance",
			maintenanceErr: errors.New("unlucky!"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, sync, stop := newTestController()
			defer stop()
			f := &fakeInstance{
				project:        testProject,
				region:         testRegion,
				zone:           testZone1,
				name:           testInstance1,
				policyErr:      tc.policyErr,
				maintenanceErr: tc.maintenanceErr,
			}
			n := addNode(t, c, f)
			n.Labels[requestedLabel] = testPolicy1 // Request this policy for this instance.
			c.KubeClient.CoreV1().Nodes().Update(context.Background(), n, v1.UpdateOptions{})

			// Trigger the update.
			c.OnAdd(n)

			// Check that nothing is applied. Note that until we kick the timer,
			// the controller goroutine is guaranteed to be blocked there.
			sync()

			// If the error was not a policy error, then we should have completed.
			if f.policyErr == nil {
				checkApplied(t, c, n, testPolicy1)
				return
			}

			// If the error was a policy error, nothing should be applied.
			checkNoApplied(t, c, n)

			// Allow it to apply 22 times, which should allow the backoff to
			// hit the maximum allowed (after waiting once before). The goroutine
			// is still guaranteed to be kicked in the timer after this.
			for i := 0; i < 22; i++ {
				c.Timer.(*fakeTimer).Kick(n.Name, func(d time.Duration) {
					switch i {
					case 0: // First kick result.
						if d != minBackoff {
							t.Errorf("got first wait time of %v, expected %v", d, minBackoff)
						}
					case 21: // Last kick result.
						if d != maxBackoff {
							t.Errorf("got last wait time of %v, expected %v", d, maxBackoff)
						}
					}
				})
			}

			// Ensure that we succeed, at least once.
			c.Timer.(*fakeTimer).Kick(n.Name, func(time.Duration) {
				f.mu.Lock()
				defer f.mu.Unlock()
				f.policyErr = nil
				f.maintenanceErr = nil
			})

			// Sync and check that it has applied.
			sync()
			checkApplied(t, c, n, testPolicy1)
		})
	}
}

func TestSpuriousDeletes(t *testing.T) {
	c, sync, stop := newTestController()
	defer stop()
	n := addNode(t, c, &fakeInstance{
		project: testProject,
		region:  testRegion,
		zone:    testZone1,
		name:    testInstance1,
	})
	n.Labels[requestedLabel] = testPolicy1 // Request this policy for this instance.
	c.KubeClient.CoreV1().Nodes().Update(context.Background(), n, v1.UpdateOptions{})

	// Trigger the update.
	c.OnAdd(n)

	// These are exercising the synchronization between the controller and
	// the goroutine triggering the update. The OnDelete should block until
	// the goroutine has completely.
	c.OnDelete(n)
	c.OnAdd(n)
	c.OnDelete(n)
	c.OnAdd(n)
	c.OnDelete(n)
	c.OnAdd(n)

	// Check that the policy has been applied.
	sync()
	checkApplied(t, c, n, testPolicy1)
}

func checkMetrics(t *testing.T, got, want map[string]int) {
	t.Helper()
	for len(got) != len(want) {
		// This should catch anything that is not in want below.
		t.Errorf("got results in %d zones, want results in %d zones", len(got), len(want))
		return
	}
	for zone, count := range want {
		other, ok := got[zone]
		if !ok {
			t.Errorf("got no results in zone %s, want value of %d", zone, count)
			return
		}
		if count != other {
			t.Errorf("got count of %d in zone %s, want value of %d", other, zone, count)
			return
		}
	}
}

func TestMetrics(t *testing.T) {
	var (
		n1             corev1.Node
		n2             corev1.Node
		n3             corev1.Node
		requestedCache = cachedMetric{cache: make(map[string]labelInfo)}
		appliedCache   = cachedMetric{cache: make(map[string]labelInfo)}
		requested      = &fakeGauge{values: make(map[string]int)}
		applied        = &fakeGauge{values: make(map[string]int)}
	)

	// Create fake nodes.
	n1.SetName(testInstance1)
	n1.SetLabels(map[string]string{
		zoneLabel: testZone1,
	})
	n2.SetName(testInstance2)
	n2.SetLabels(map[string]string{
		zoneLabel: testZone2,
	})
	n3.SetName(testInstance3)
	n3.SetLabels(map[string]string{
		zoneLabel: testZone1,
	})

	// Attempt a first update & sanity check the metrics.
	requestedCache.update(&n1, testPolicy1, requested)
	checkMetrics(t, requested.values, map[string]int{
		testZone1: 1,
	})

	// Check that it does not change.
	requestedCache.update(&n1, testPolicy1, requested)
	checkMetrics(t, requested.values, map[string]int{
		testZone1: 1,
	})

	// Add the second node.
	requestedCache.update(&n2, testPolicy1, requested)
	checkMetrics(t, requested.values, map[string]int{
		testZone1: 1,
		testZone2: 1,
	})

	// Change the policy on the second node, nothing should change.
	requestedCache.update(&n2, testPolicy2, requested)
	checkMetrics(t, requested.values, map[string]int{
		testZone1: 1,
		testZone2: 1,
	})

	// Add the third node.
	requestedCache.update(&n3, testPolicy1, requested)
	checkMetrics(t, requested.values, map[string]int{
		testZone1: 2,
		testZone2: 1,
	})

	// Update the applied label.
	appliedCache.update(&n1, "", applied)
	checkMetrics(t, applied.values, map[string]int{}) // Nothing applied.

	// Add the second node.
	appliedCache.update(&n2, "", applied)
	checkMetrics(t, applied.values, map[string]int{}) // Nothing applied.

	// Add the third node.
	appliedCache.update(&n3, testPolicy1, applied)
	checkMetrics(t, applied.values, map[string]int{
		testZone1: 1,
	})

	// Check that it does not change.
	appliedCache.update(&n3, testPolicy1, applied)
	checkMetrics(t, applied.values, map[string]int{
		testZone1: 1,
	})

	// Remove the first requested label, check that it does change.
	requestedCache.update(&n3, "", requested)
	appliedCache.update(&n3, "", applied)
	checkMetrics(t, requested.values, map[string]int{
		testZone1: 1,
		testZone2: 1,
	})
	checkMetrics(t, applied.values, map[string]int{}) // Empty again.
}

func TestInferPolicy(t *testing.T) {
	const (
		projectName = "project-name"
		regionName  = "region-name"
		policyName  = "policy-name"
	)
	var (
		n    corev1.Node
		want = fmt.Sprintf("projects/%s/regions/%s/resourcePolicies/%s", projectName, regionName, policyName)
	)
	n.SetLabels(map[string]string{
		regionLabel: regionName,
	})

	// Just a bare name, with no associated project.
	if got, err := inferFullPolicy(&n, projectName, policyName); err != nil || got != want {
		t.Errorf("got (%v, %v), want (%v, nil)", got, err, want)
	}

	// The project/name form.
	if got, err := inferFullPolicy(&n, projectName, fmt.Sprintf("%s/%s", projectName, policyName)); err != nil || got != want {
		t.Errorf("got (%v, %v), want (%v, nil)", got, err, want)
	}

	// The project/region/name form.
	if got, err := inferFullPolicy(&n, projectName, fmt.Sprintf("%s/%s/%s", projectName, regionName, policyName)); err != nil || got != want {
		t.Errorf("got (%v, %v), want (%v, nil)", got, err, want)
	}

	// Some other invalid form.
	if got, err := inferFullPolicy(&n, projectName, fmt.Sprintf("prefix/%s/%s/%s", projectName, regionName, policyName)); err == nil {
		t.Errorf("got (%v, nil), want (_, non-nil)", got)
	}
}

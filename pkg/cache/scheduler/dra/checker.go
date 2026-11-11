/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dra

import (
	"context"
	"errors"
	"iter"
	"sync"

	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/dynamic-resource-allocation/structured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/kueue/pkg/cache/scheduler/simulator"
)

// Checker drops candidate nodes that cannot supply a Pod's ResourceClaims. It
// allocates with the engine kube-scheduler uses, so both reach the same answer.
type Checker struct {
	inner    simulator.SchedulerSimulator
	cl       client.Client
	celCache *CELCache
	devices  lazyDevices

	// podClaims holds the claims CountPodsOnNode resolved, keyed by the Pod template
	// it was asked about: it is asked once per node for the same template.
	podClaimsMu sync.Mutex
	podClaims   map[*corev1.PodTemplateSpec]resourceClaimsForPod

	// deviceTaintRules is whether the cluster serves DeviceTaintRules, decided once at
	// startup: listing a kind the API server lacks costs a discovery request each time.
	deviceTaintRules bool
}

// NewChecker wraps inner with the device check. Pass the scheduler cache's CELCache
// rather than a fresh one: it only pays off by outliving the snapshot.
func NewChecker(inner simulator.SchedulerSimulator, cl client.Client, celCache *CELCache, deviceTaintRules bool) *Checker {
	c := &Checker{
		inner:            inner,
		cl:               cl,
		celCache:         celCache,
		deviceTaintRules: deviceTaintRules,
	}
	c.devices.build = c.buildDevices
	return c
}

// Simulate needs no DRA handling: the device filtering holds no state.
func (c *Checker) Simulate(ctx context.Context, fn func()) error {
	return c.inner.Simulate(ctx, fn)
}

// PreemptWorkload frees the Workload's Pods but not the devices its claims hold: the
// allocator is built once per snapshot from the ResourceClaims as they stand. So
// preemption cannot make a Workload device-feasible, and the check stays restrictive
// rather than over-admitting. Releasing them is Beta work in keps/2941-DRA.
func (c *Checker) PreemptWorkload(ctx context.Context, wlKey client.ObjectKey) (func() error, error) {
	return c.inner.PreemptWorkload(ctx, wlKey)
}

func (c *Checker) FindFeasibleNodes(
	ctx context.Context,
	candidates iter.Seq[simulator.Candidate],
	requirements *simulator.PodRequirements,
	stats *simulator.NodeExclusionStats,
) ([]simulator.MatchedCandidate, error) {
	feasible, err := c.inner.FindFeasibleNodes(ctx, candidates, requirements, stats)
	if err != nil {
		return nil, err
	}

	if requirements.PodTemplate == nil {
		return feasible, nil
	}

	// The claims belong to the Workload rather than the cluster, so unlike the
	// allocator they are resolved on every call.
	claims, err := c.newResourceClaimsForPod(ctx, requirements.PodTemplate)
	if err != nil {
		return nil, err
	}
	if claims.isEmpty() {
		return feasible, nil
	}

	devices, err := c.devices.get(ctx)
	if err != nil {
		return nil, err
	}

	return c.filterByDevices(ctx, feasible, devices.allocator, claims, stats)
}

func (c *Checker) filterByDevices(
	ctx context.Context,
	feasible []simulator.MatchedCandidate,
	allocator structured.Allocator,
	claims resourceClaimsForPod,
	stats *simulator.NodeExclusionStats,
) ([]simulator.MatchedCandidate, error) {
	logger := log.FromContext(ctx)
	var draFeasible []simulator.MatchedCandidate
	for _, candidate := range feasible {
		node := candidate.GetNode()
		if node == nil {
			// A candidate always carries its node, so this is a programming error
			// rather than a placement outcome. Guessing would admit an unchecked node.
			return nil, errors.New("candidate has no node, cannot evaluate DRA claims")
		}

		results, err := allocator.Allocate(ctx, node, claims.forNode(node))
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}
			// A failure usually means broken cluster configuration, such as a
			// DeviceClass whose CEL does not compile. Excluding only this node keeps
			// one bad DeviceClass from stalling all scheduling; the log surfaces it.
			logger.V(2).Info("Excluding node: DRA allocation failed", "node", node.Name, "error", err)
			stats.DRANoFit++
			continue
		}
		if results == nil {
			logger.V(5).Info("Node lacks matching DRA devices", "node", node.Name)
			stats.DRANoFit++
			continue
		}

		draFeasible = append(draFeasible, candidate)
	}
	return draFeasible, nil
}

var _ simulator.DevicePodCounter = (*Checker)(nil)

// CountPodsOnNode places the Pods on node one at a time, each allocating against the
// devices the ones before it took, the way kube-scheduler binds them. Allocating every
// Pod's claims in one call could find a packing kube-scheduler would not, and ruling
// out one Pod more would search every ordering of the identical claims.
func (c *Checker) CountPodsOnNode(
	ctx context.Context,
	node *corev1.Node,
	requirements, leader *simulator.PodRequirements,
	limit int32,
) (int32, error) {
	if limit <= 0 || requirements.PodTemplate == nil {
		return limit, nil
	}
	workers, err := c.claimsForPod(ctx, requirements.PodTemplate)
	if err != nil {
		return 0, err
	}
	workerClaims := workers.forNode(node)
	if len(workerClaims) == 0 {
		return limit, nil
	}
	var leaderClaims []*resourceapi.ResourceClaim
	if leader != nil && leader.PodTemplate != nil {
		claims, err := c.claimsForPod(ctx, leader.PodTemplate)
		if err != nil {
			return 0, err
		}
		leaderClaims = claims.forNode(node)
	}

	devices, err := c.devices.get(ctx)
	if err != nil {
		return 0, err
	}
	placement := newNodePlacement(devices, node)
	if len(leaderClaims) > 0 {
		if placed, err := placement.place(ctx, leaderClaims); err != nil || !placed {
			return 0, err
		}
	}
	var count int32
	for count < limit {
		placed, err := placement.place(ctx, workerClaims)
		if err != nil {
			return 0, err
		}
		if !placed {
			break
		}
		count++
	}
	return count, nil
}

func (c *Checker) claimsForPod(ctx context.Context, podTemplate *corev1.PodTemplateSpec) (resourceClaimsForPod, error) {
	c.podClaimsMu.Lock()
	defer c.podClaimsMu.Unlock()
	if claims, found := c.podClaims[podTemplate]; found {
		return claims, nil
	}
	claims, err := c.newResourceClaimsForPod(ctx, podTemplate)
	if err != nil {
		return resourceClaimsForPod{}, err
	}
	if c.podClaims == nil {
		c.podClaims = make(map[*corev1.PodTemplateSpec]resourceClaimsForPod)
	}
	c.podClaims[podTemplate] = claims
	return claims, nil
}

// nodePlacement tracks the devices the Pods placed on one node so far hold.
type nodePlacement struct {
	devices   *deviceState
	node      *corev1.Node
	slices    []*resourceapi.ResourceSlice
	allocated []resourceapi.DeviceRequestAllocationResult
}

func newNodePlacement(devices *deviceState, node *corev1.Node) *nodePlacement {
	slices, allocated := devices.devicesForNode(node)
	return &nodePlacement{devices: devices, node: node, slices: slices, allocated: allocated}
}

// place allocates one Pod's claims and, when they fit, keeps their devices taken. An
// allocator holds its device state for good, so each Pod gets a fresh one.
func (p *nodePlacement) place(ctx context.Context, claims []*resourceapi.ResourceClaim) (bool, error) {
	allocator, err := p.devices.newAllocator(ctx, p.slices, p.allocated)
	if err != nil {
		return false, err
	}
	results, err := allocator.Allocate(ctx, p.node, claims)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return false, err
		}
		// As in filterByDevices, broken cluster configuration excludes the node rather
		// than stalling scheduling, here by holding no more Pods.
		log.FromContext(ctx).V(2).Info("Counting no more Pods on node: DRA allocation failed", "node", p.node.Name, "error", err)
		return false, nil
	}
	if results == nil {
		return false, nil
	}
	for i := range results {
		p.allocated = appendHeldDevices(p.allocated, results[i].Devices.Results)
	}
	return true, nil
}

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
	"fmt"
	"slices"
	"sync"

	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	dracel "k8s.io/dynamic-resource-allocation/cel"
	"k8s.io/dynamic-resource-allocation/structured"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/dynamicresources"
	schedulerfeature "k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/kueue/pkg/features"
)

// CELCache holds the compiled device selectors, keyed by the expression, so one survives
// the Checker that compiled it and is compiled once for the process rather than once
// per scheduling cycle. The zero value is ready to use and safe to share.
type CELCache struct {
	once  sync.Once
	value *dracel.Cache
}

func (c *CELCache) get() *dracel.Cache {
	c.once.Do(func() {
		// Read on first use, not at construction, so the gates are parsed by now.
		fts := schedulerfeature.NewSchedulerFeaturesFromGates(utilfeature.DefaultFeatureGate)
		// The same size kube-scheduler's dynamicresources plugin uses.
		c.value = dracel.NewCache(10, dracel.Features{
			EnableConsumableCapacity: fts.EnableDRAConsumableCapacity,
			EnableListTypeAttributes: fts.EnableDRAListTypeAttributes,
		})
	})
	return c.value
}

// lazyDevices builds the snapshot's device state on first use. A Pod's claims go to
// Allocate rather than the constructor, so one state serves the whole snapshot and
// cannot shift mid-cycle.
type lazyDevices struct {
	build func(context.Context) (*deviceState, error)
	once  sync.Once
	value *deviceState
	err   error
}

func (l *lazyDevices) get(ctx context.Context) (*deviceState, error) {
	l.once.Do(func() {
		l.value, l.err = l.build(ctx)
	})
	return l.value, l.err
}

// deviceState is the cluster's devices as the snapshot read them.
type deviceState struct {
	// allocator answers whether one Pod's claims fit on any node.
	allocator structured.Allocator

	// The rest builds an allocator for one node that also counts the devices of the
	// Pods already placed there. Scoping it to the node's pools keeps that cheap.
	features    structured.Features
	classLister *deviceClassCache
	celCache    *dracel.Cache
	pools       map[poolKey]*poolDevices
	// nodePools are the pools with a slice bound to each node, and sharedPools the
	// ones with a slice bound to none, which may serve any node.
	nodePools   map[string][]poolKey
	sharedPools []poolKey
}

type poolKey struct {
	driver string
	pool   string
}

// poolDevices is a pool's slices and the devices allocated from it.
type poolDevices struct {
	slices    []*resourceapi.ResourceSlice
	allocated []resourceapi.DeviceRequestAllocationResult
}

func (c *Checker) buildDevices(ctx context.Context) (*deviceState, error) {
	var sliceList resourceapi.ResourceSliceList
	if err := c.cl.List(ctx, &sliceList); err != nil {
		return nil, fmt.Errorf("listing ResourceSlices: %w", err)
	}
	deviceSlices := make([]*resourceapi.ResourceSlice, len(sliceList.Items))
	for i := range sliceList.Items {
		deviceSlices[i] = &sliceList.Items[i]
	}

	deviceSlices, err := applyDeviceTaintRules(ctx, c.cl, c.deviceTaintRules, deviceSlices)
	if err != nil {
		return nil, err
	}

	// Configured from the Kubernetes DRA gates rather than the Kueue ones, by the same
	// call kube-scheduler makes, so the two allocators stay in step. Device taints are the
	// exception: KueueDRAIntegrationDeviceTaints gates them as DRADeviceTaints does in kube-scheduler.
	draFeatures := dynamicresources.AllocatorFeatures(schedulerfeature.NewSchedulerFeaturesFromGates(utilfeature.DefaultFeatureGate))
	draFeatures.DeviceTaints = draFeatures.DeviceTaints && features.Enabled(features.KueueDRAIntegrationDeviceTaints)

	allocated, err := listAllocatedDevices(ctx, c.cl)
	if err != nil {
		return nil, fmt.Errorf("building allocated device state: %w", err)
	}

	classLister, err := newDeviceClassCache(ctx, c.cl)
	if err != nil {
		return nil, fmt.Errorf("listing DeviceClasses: %w", err)
	}

	state := &deviceState{
		features:    draFeatures,
		classLister: classLister,
		celCache:    c.celCache.get(),
	}
	state.allocator, err = state.newAllocator(ctx, deviceSlices, allocated)
	if err != nil {
		return nil, err
	}
	state.indexPools(deviceSlices, allocated)
	return state, nil
}

func (d *deviceState) newAllocator(ctx context.Context, slices []*resourceapi.ResourceSlice, allocated []resourceapi.DeviceRequestAllocationResult) (structured.Allocator, error) {
	// The allocated state is read with the allocator's own setting rather than the gate, so
	// the two cannot disagree on whether a shared device is partly or wholly consumed.
	allocatedState := newAllocatedState(allocated, d.features.ConsumableCapacity)
	allocator, err := structured.NewAllocator(ctx, d.features, allocatedState, d.classLister, slices, d.celCache)
	if err != nil {
		return nil, fmt.Errorf("creating DRA allocator: %w", err)
	}
	return allocator, nil
}

func (d *deviceState) indexPools(deviceSlices []*resourceapi.ResourceSlice, allocated []resourceapi.DeviceRequestAllocationResult) {
	d.pools = make(map[poolKey]*poolDevices)
	d.nodePools = make(map[string][]poolKey)
	shared := sets.New[poolKey]()
	for _, slice := range deviceSlices {
		key := poolKey{driver: slice.Spec.Driver, pool: slice.Spec.Pool.Name}
		pool, found := d.pools[key]
		if !found {
			pool = &poolDevices{}
			d.pools[key] = pool
		}
		pool.slices = append(pool.slices, slice)
		nodeName := ptr.Deref(slice.Spec.NodeName, "")
		switch {
		case nodeName == "":
			if !shared.Has(key) {
				shared.Insert(key)
				d.sharedPools = append(d.sharedPools, key)
			}
		case !slices.Contains(d.nodePools[nodeName], key):
			d.nodePools[nodeName] = append(d.nodePools[nodeName], key)
		}
	}
	for _, result := range allocated {
		// A device in no pool cannot be allocated again, so it bounds nothing.
		if pool, found := d.pools[poolKey{driver: result.Driver, pool: result.Pool}]; found {
			pool.allocated = append(pool.allocated, result)
		}
	}
}

// devicesForNode is every pool that can serve node, whole so that none looks
// incomplete, and the devices already allocated from them.
func (d *deviceState) devicesForNode(node *corev1.Node) ([]*resourceapi.ResourceSlice, []resourceapi.DeviceRequestAllocationResult) {
	var nodeSlices []*resourceapi.ResourceSlice
	var allocated []resourceapi.DeviceRequestAllocationResult
	seen := sets.New[poolKey]()
	for _, key := range slices.Concat(d.nodePools[node.Name], d.sharedPools) {
		if seen.Has(key) {
			continue
		}
		seen.Insert(key)
		pool := d.pools[key]
		nodeSlices = append(nodeSlices, pool.slices...)
		allocated = append(allocated, pool.allocated...)
	}
	return nodeSlices, allocated
}

// listAllocatedDevices returns the devices the cluster's ResourceClaims hold. Admin
// access leaves a device free for others, so those results are left out.
func listAllocatedDevices(ctx context.Context, cl client.Client) ([]resourceapi.DeviceRequestAllocationResult, error) {
	var claims resourceapi.ResourceClaimList
	if err := cl.List(ctx, &claims); err != nil {
		return nil, fmt.Errorf("listing ResourceClaims: %w", err)
	}
	var allocated []resourceapi.DeviceRequestAllocationResult
	for i := range claims.Items {
		claim := &claims.Items[i]
		if claim.Status.Allocation == nil {
			continue
		}
		allocated = appendHeldDevices(allocated, claim.Status.Allocation.Devices.Results)
	}
	return allocated, nil
}

// appendHeldDevices appends the results that keep their device from other claims.
func appendHeldDevices(held []resourceapi.DeviceRequestAllocationResult, results []resourceapi.DeviceRequestAllocationResult) []resourceapi.DeviceRequestAllocationResult {
	for _, result := range results {
		if !ptr.Deref(result.AdminAccess, false) {
			held = append(held, result)
		}
	}
	return held
}

func newAllocatedState(allocated []resourceapi.DeviceRequestAllocationResult, consumableCapacity bool) structured.AllocatedState {
	allocatedDevices := sets.New[structured.DeviceID]()
	allocatedSharedDeviceIDs := sets.New[structured.SharedDeviceID]()
	aggregatedCapacity := structured.NewConsumedCapacityCollection()
	for _, result := range allocated {
		deviceID := structured.MakeDeviceID(result.Driver, result.Pool, result.Device)
		if consumableCapacity && result.ShareID != nil {
			sharedID := structured.MakeSharedDeviceID(deviceID, result.ShareID)
			allocatedSharedDeviceIDs.Insert(sharedID)
			if result.ConsumedCapacity != nil {
				cc := structured.NewDeviceConsumedCapacity(deviceID, result.ConsumedCapacity)
				aggregatedCapacity.Insert(cc)
			}
			continue
		}
		allocatedDevices.Insert(deviceID)
	}
	return structured.AllocatedState{
		AllocatedDevices:         allocatedDevices,
		AllocatedSharedDeviceIDs: allocatedSharedDeviceIDs,
		AggregatedCapacity:       aggregatedCapacity,
	}
}

// deviceClassCache serves the DeviceClasses read when the allocator was built. The
// upstream lister takes no context yet is called during allocation, so every class is
// resolved up front.
type deviceClassCache struct {
	all    []*resourceapi.DeviceClass
	byName map[string]*resourceapi.DeviceClass
}

func newDeviceClassCache(ctx context.Context, cl client.Client) (*deviceClassCache, error) {
	var deviceClasses resourceapi.DeviceClassList
	if err := cl.List(ctx, &deviceClasses); err != nil {
		return nil, err
	}
	cache := &deviceClassCache{
		all:    make([]*resourceapi.DeviceClass, len(deviceClasses.Items)),
		byName: make(map[string]*resourceapi.DeviceClass, len(deviceClasses.Items)),
	}
	for i := range deviceClasses.Items {
		dc := &deviceClasses.Items[i]
		cache.all[i] = dc
		cache.byName[dc.Name] = dc
	}
	return cache, nil
}

func (l *deviceClassCache) List() ([]*resourceapi.DeviceClass, error) {
	return l.all, nil
}

func (l *deviceClassCache) Get(className string) (*resourceapi.DeviceClass, error) {
	dc, found := l.byName[className]
	if !found {
		return nil, apierrors.NewNotFound(resourceapi.Resource("deviceclasses"), className)
	}
	return dc, nil
}

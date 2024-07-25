// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	log2 "github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

// Schedule the Scheduler scans stores and determines whether there is an imbalance and which region it should move.
// This scheduler avoids too many regions in one store
// If it returns null (with several times retry), the Scheduler will use GetNextInterval to increase the interval.
// If it returns an operator, the Scheduler will dispatch these operators as the response of the next heartbeat of the related region.
func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	// First, the Scheduler will select all suitable stores
	// In short, a suitable store should be up and the down time cannot be longer than MaxStoreDownTime of the cluster,
	// which you can get through cluster.GetMaxStoreDownTime()
	allStores := cluster.GetStores()
	suitStores := make([]*core.StoreInfo, 0)
	for _, store := range allStores {
		if store.IsUp() && store.DownTime() < cluster.GetMaxStoreDownTime() {
			suitStores = append(suitStores, store)
		}
	}
	// len must >= 2 for move
	if suitStores == nil || len(suitStores) <= 1 {
		return nil
	}
	log2.Debugf("suitStores: %v", suitStores)
	var selectStore *core.StoreInfo
	var selectRegion *core.RegionInfo
	// the Scheduler will select a store as the target. Actually, the Scheduler will select the store with the smallest region size.
	sort.Slice(suitStores, func(i, j int) bool { return suitStores[i].GetRegionSize() > suitStores[j].GetRegionSize() })
	// Then the Scheduler tries to find regions to move from the store with the biggest region size.
	for _, store := range suitStores {
		log2.Debugf("tryStore: %+v, id: %v, regionsize: %v", store, store.GetID(), store.GetRegionSize())
		var container core.RegionsContainer
		// First, it will try to select a pending region because pending may mean the disk is overloaded.
		cluster.GetPendingRegionsWithLock(store.GetID(), func(regionContainer core.RegionsContainer) {
			container = regionContainer
		})
		selectRegion = container.RandomRegion(nil, nil)
		if selectRegion != nil {
			selectStore = store
			break
		}
		// If there isn’t a pending region, it will try to find a follower region.
		cluster.GetFollowersWithLock(store.GetID(), func(regionContainer core.RegionsContainer) {
			container = regionContainer
		})
		selectRegion = container.RandomRegion(nil, nil)
		if selectRegion != nil {
			selectStore = store
			break
		}
		// If it still cannot pick out one region, it will try to pick leader regions
		cluster.GetLeadersWithLock(store.GetID(), func(regionContainer core.RegionsContainer) {
			container = regionContainer
		})
		selectRegion = container.RandomRegion(nil, nil)
		if selectRegion != nil {
			selectStore = store
			break
		}
		// Finally, it will select out the region to move, or the Scheduler will try the next store
		// which has a smaller region size until all stores will have been tried.
		log2.Debugf("tryStore id: %v failed", store.GetID())
	}
	if selectRegion == nil || selectStore == nil {
		return nil
	}
	allRegionStores := selectRegion.GetStoreIds()
	// 判断目标 region 的 store 数量，如果小于 cluster.GetMaxReplicas()，直接放弃本次操作
	if len(allRegionStores) < cluster.GetMaxReplicas() {
		return nil
	}
	// After you pick up one region to move, the Scheduler will select a store as the target
	// Actually, the Scheduler will select the store with the smallest region size.
	var targetStore *core.StoreInfo
	// 选出一个目标 store，目标 store 不能在原来的 region 里面
	for i := len(suitStores) - 1; i >= 0; i-- {
		if _, exist := allRegionStores[suitStores[i].GetID()]; !exist {
			targetStore = suitStores[i]
			break
		}
	}
	if targetStore == nil {
		return nil
	}
	log2.Debugf("selectStore %v, id: %v targetStore: %v, id: %v", selectStore, selectStore.GetID(), targetStore, targetStore.GetID())
	// Then the Scheduler will judge whether this movement is valuable, by checking the difference between
	// region sizes of the original store and the target store.
	// If the difference is big enough(make sure that the difference has to be bigger than two times the approximate size of the region)
	// the Scheduler should allocate a new peer on the target store and create a move peer operator.
	if selectStore.GetRegionSize()-targetStore.GetRegionSize() > 2*selectRegion.GetApproximateSize() {
		log2.Debugf("the difference is big enough")
		allocPeer, _ := cluster.AllocPeer(targetStore.GetID())
		op, _ := operator.CreateMovePeerOperator(s.name, cluster, selectRegion, operator.OpBalance, selectStore.GetID(), targetStore.GetID(), allocPeer.Id)
		return op
	}
	return nil
}

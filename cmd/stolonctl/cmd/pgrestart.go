// Copyright 2018 Sorint.lab
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"fmt"
	"time"

	cmdcommon "github.com/sorintlab/stolon/cmd"
	"github.com/sorintlab/stolon/internal/cluster"
	"github.com/sorintlab/stolon/internal/common"
	slog "github.com/sorintlab/stolon/internal/log"
	"github.com/sorintlab/stolon/internal/store"
	"github.com/spf13/cobra"
)

var log = slog.S()

var pgRestartCmd = &cobra.Command{
	Use:   "pgrestart",
	Short: `Restarts the underlying postgres cluster with minimal downtime`,
	Long:  `Restarts the underlying postgres cluster with minimal downtime`,
	Run:   pgRestart,
}

func init() {
	CmdStolonCtl.AddCommand(pgRestartCmd)
}

func pgRestart(cmd *cobra.Command, args []string) {
	store, err := cmdcommon.NewStore(&cfg.CommonConfig)
	if err != nil {
		die("%v", err)
	}

	cd, pair := fatalGetClusterData(store)

	newCd := cd.DeepCopy()

	log.Infow("Restarting slave postgres!")
	for _, db := range newCd.DBs {
		if db.Spec.Role == common.RoleStandby {
			k, ok := newCd.Keepers[db.Spec.KeeperUID]

			if !ok {
				die("Keeper data was empty for the DB")
			}

			k.Status.Healthy = false
			k.Status.SchedulePgRestart = true
		}
	}

	_, err = store.AtomicPutClusterData(context.TODO(), newCd, pair)
	if err != nil {
		die("cannot update cluster data: %v", err)
	}

	numTries := 10
	err = retry(numTries, time.Second, func() error {
		cd, _ := fatalGetClusterData(store)
		if isAnySlaveHealthy(cd) {
			return restartMasterPostgres(store)
		}
		return fmt.Errorf("No healthy slave found")
	})

	if err != nil {
		die(err.Error())
	}
}

func fatalGetClusterData(e store.Store) (*cluster.ClusterData, *store.KVPair) {
	cd, pair, err := getClusterData(e)
	if err != nil {
		die("cannot get cluster data: %v", err)
	}
	if cd.Cluster == nil {
		die("no cluster spec available")
	}
	if cd.Cluster.Spec == nil {
		die("no cluster spec available")
	}

	return cd, pair
}

func isAnySlaveHealthy(cd *cluster.ClusterData) bool {
	for _, db := range cd.DBs {
		if db.Spec.Role == common.RoleStandby {
			k := cd.Keepers[db.Spec.KeeperUID]

			if k.Status.Healthy && db.Status.Healthy {
				return true
			}
		}
	}
	return false
}

func restartMasterPostgres(e store.Store) error {
	log.Infow("Restarting master")

	oldMasterKeeperUID, err := forceFailMaster(e)
	if err != nil {
		return fmt.Errorf("restartMasterPostgres failed with error: %v", err)
	}

	// Wait till a new master is elected
	err = retry(10, time.Second, func() error {
		cd, _ := fatalGetClusterData(e)
		newMasterKeeper, err := findMasterKeeper(cd)

		if err != nil {
			return err
		}

		if !newMasterKeeper.Status.Healthy {
			return fmt.Errorf("Master is not healthy")
		}

		if newMasterKeeper.UID == oldMasterKeeperUID {
			log.Warnw("Relected master is same as the old master. This will cause downtime.")
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("Error when waiting for new master to be elected: %v", err)
	}

	cd, pair := fatalGetClusterData(e)
	oldMaster, ok := cd.Keepers[oldMasterKeeperUID]
	if !ok {
		return fmt.Errorf("Old master not found in cluster data")
	}

	oldMaster.Status.Healthy = false
	oldMaster.Status.SchedulePgRestart = true

	_, err = e.AtomicPutClusterData(context.TODO(), cd, pair)
	if err != nil {
		return fmt.Errorf("restartMasterPostgres failed on update of cluster data: %v", err)
	}

	return nil
}

func findMasterKeeper(cd *cluster.ClusterData) (*cluster.Keeper, error) {
	for _, db := range cd.DBs {
		if db.Spec.Role == common.RoleMaster {
			masterKeeper, ok := cd.Keepers[db.Spec.KeeperUID]
			if !ok {
				return nil, fmt.Errorf("Unable to find master")
			}

			return masterKeeper, nil
		}
	}
	return nil, fmt.Errorf("Unable to find master")
}

func forceFailMaster(e store.Store) (string, error) {
	cd, pair := fatalGetClusterData(e)

	currentMasterKeeper, err := findMasterKeeper(cd)
	if err != nil {
		return "", err
	}

	currentMasterKeeper.Status.Healthy = false
	currentMasterKeeper.Status.ForceFail = true
	_, err = e.AtomicPutClusterData(context.TODO(), cd, pair)
	if err != nil {
		return "", err
	}

	return currentMasterKeeper.UID, nil
}

func retry(attempts int, sleep time.Duration, fn func() error) error {
	if err := fn(); err != nil {
		if attempts--; attempts > 0 {
			time.Sleep(sleep)
			return retry(attempts, sleep, fn)
		}

		return err
	}
	return nil
}

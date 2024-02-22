package update

import (
	"errors"
	"fmt"

	"github.com/bazelgo/dynamodb-manager/client"
)

var (
	SwitchToOnDemandCapacityClient  = client.SwitchToOnDemandCapacity
	UpdateProvisionedCapacityClient = client.UpdateProvisionedCapacity
	GetCurrentBillingModeClient     = client.GetCurrentBillingMode
)

// ExecuteUpdate updates the capacity mode and provisioned capacity of a DynamoDB table.
// It takes a DynamoDBManager, table name, parameters for Read Capacity Units (RCU), Write Capacity Units (WCU),
// and flags to switch to on-demand or provisioned capacity as input.
// It returns an error if the update operation fails.
func ExecuteUpdate(dbmgr *client.DynamoDBManager, tableName string, paramRcu string, paramWcu string, switchToOnDemand bool, switchToProvisioned bool) error {
	billingMode, rcu, wcu, err := GetCurrentBillingModeClient(dbmgr, tableName)
	if err != nil {
		dbmgr.Logger.Errorf("Failed to get the billing mode info of table:%s : as current billing mode due to error:%v", tableName, err)
		return errors.New("Failed to update the table!")
	}

	if switchToOnDemand {
		if billingMode != "PAY_PER_REQUEST" {
			return SwitchToOnDemandCapacityClient(dbmgr, tableName)
		} else {
			dbmgr.Logger.Warn("No need to switch, as it already is on demand mode!")
			return nil
		}
	} else {
		if billingMode != "PROVISIONED" && !switchToProvisioned {
			dbmgr.Logger.Errorf("Failed to update table:%s : as current billing mode:%s - does not support modification of rcu or wcu", tableName, billingMode)
			return errors.New("Failed to update the table!")
		}

		if paramRcu == "" && paramWcu == "" {
			return UpdateProvisionedCapacityClient(dbmgr, switchToProvisioned, tableName, "", "")
		}

		if paramRcu == "" {
			paramRcu = fmt.Sprintf("%d", client.DefaultRcu)
		}

		if paramWcu == "" {
			paramWcu = fmt.Sprintf("%d", client.DefaultWcu)
		}

		if paramRcu != rcu || paramWcu != wcu {
			return UpdateProvisionedCapacityClient(dbmgr, switchToProvisioned, tableName, paramRcu, paramWcu)
		} else {
			dbmgr.Logger.Warn("No need to update, as it already is provisioned mode or remain the same rcu and wcu!")
			return nil
		}
	}
}

package client

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/bazelgo/dynamodb-manager/logging"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	DefaultRcu = 5
	DefaultWcu = 5
)

// DynamoDBManager represents the DynamoDB manager in Go.
type DynamoDBManager struct {
	DynamoDBClient *dynamodb.Client // Add DynamoDB client
	Logger         *logging.Logger
}

var LoadConfig = config.LoadDefaultConfig
var DBNewFromConfig = dynamodb.NewFromConfig
var NewListTablesPageIt = dynamodb.NewListTablesPaginator

// CreateNewDynamoDBManager creates a new DynamoDBManager instance based on the provided AWS profile name.
// It returns a DynamoDBManager and an error.
func CreateNewDynamoDBManager(profileName string) (*DynamoDBManager, error) {
	var configToUse aws.Config
	var err error

	if profileName != "" {
		// For local test purpose
		configToUse, err = LoadConfig(context.TODO(), config.WithSharedConfigProfile(profileName))
	} else {
		configToUse, err = LoadConfig(context.Background())
	}

	if err != nil {
		fmt.Printf("CreateNewDynamoDBManager-config.LoadDefaultConfig:%s", err)
		return nil, errors.New("Failed to instantiate aws config!")
	}

	return NewDynamoDBManager(configToUse)
}

// NewDynamoDBManager creates a new DynamoDBManager instance with the given AWS config.
// It returns a DynamoDBManager and an error.
func NewDynamoDBManager(cfg ...aws.Config) (*DynamoDBManager, error) {
	if len(cfg) == 0 {
		return nil, errors.New("expected a DynamoDB config, but got nothing")
	}
	return &DynamoDBManager{
		DynamoDBClient: DBNewFromConfig(cfg[0]),
		Logger:         nil,
	}, nil
}

// SetupLogger initializes the logger for the DynamoDBManager with the specified log level.
// It returns an error if logger setup fails.
func SetupLogger(dbmgr *DynamoDBManager, level string) error {
	loggerObj, err := logging.NewLogger(level)
	if err != nil {
		fmt.Printf("failed to create new logger: %v", err)
		return err
	}
	dbmgr.Logger = loggerObj
	return nil
}

// GetTableList retrieves a list of DynamoDB table names using the provided DynamoDBManager.
// It returns a slice of table names and an error.
func GetTableList(dbmgr *DynamoDBManager) ([]string, error) {
	var tableNames []string
	var output *dynamodb.ListTablesOutput
	var err error
	tablePaginator := NewListTablesPageIt(dbmgr.DynamoDBClient, &dynamodb.ListTablesInput{})
	for tablePaginator.HasMorePages() {
		output, err = tablePaginator.NextPage(context.Background())
		if err != nil {
			dbmgr.Logger.Errorf("Couldn't list tables. Here's why: %v\n", err)
			break
		}
		tableNames = append(tableNames, output.TableNames...)
	}
	return tableNames, err
}

// GetTableArn retrieves the ARN of a DynamoDB table with the given name using the provided DynamoDBManager.
// It returns the table ARN and an error.
func GetTableArn(dbmgr *DynamoDBManager, tableName string) (string, error) {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}

	output, err := dbmgr.DynamoDBClient.DescribeTable(context.Background(), input)
	if err != nil {
		dbmgr.Logger.Errorf("Failed to get Table Arn, Here's why: %v\n", err)
		return "", err
	}
	return *output.Table.TableArn, nil

}

// GetTableTags retrieves the tags of a DynamoDB table with the given ARN using the provided DynamoDBManager.
// It returns a slice of tags and an error.
func GetTableTags(dbmgr *DynamoDBManager, tableArn string) ([]types.Tag, error) {
	listTagsInput := &dynamodb.ListTagsOfResourceInput{
		ResourceArn: aws.String(tableArn),
	}

	result, err := dbmgr.DynamoDBClient.ListTagsOfResource(context.Background(), listTagsInput)
	if err != nil {
		dbmgr.Logger.Errorf("Error calling ListTagsOfResource:%v", err)
		return nil, err
	}

	return result.Tags, nil
}

// GetCurrentBillingMode retrieves the billing mode, read capacity units, and write capacity units of a DynamoDB table.
// It returns the billing mode, RCU, WCU, and an error.
func GetCurrentBillingMode(dbmgr *DynamoDBManager, tableName string) (string, string, string, error) {
	input := &dynamodb.DescribeTableInput{
		TableName: &tableName,
	}

	var rcu, wcu, billingMode string

	output, err := dbmgr.DynamoDBClient.DescribeTable(context.Background(), input)
	if err != nil {
		return "", "", "", err
	}

	if output.Table.BillingModeSummary != nil {
		billingMode = fmt.Sprintf("%v", output.Table.BillingModeSummary.BillingMode)
	}

	if billingMode == "PROVISIONED" {
		rcu = fmt.Sprintf("%d", aws.ToInt64(output.Table.ProvisionedThroughput.ReadCapacityUnits))
		wcu = fmt.Sprintf("%d", aws.ToInt64(output.Table.ProvisionedThroughput.WriteCapacityUnits))
	}

	return billingMode, rcu, wcu, nil
}

// UpdateProvisionedCapacity updates the provisioned capacity of a DynamoDB table.
// It returns an error if the update fails.
func UpdateProvisionedCapacity(dbmgr *DynamoDBManager, switchToProvisioned bool, tableName string, rcuStr string, wcuStr string) error {
	var input *dynamodb.UpdateTableInput
	var rcuVal int64
	var wcuVal int64

	if rcuStr != "" {
		rcuVal, _ = strconv.ParseInt(rcuStr, 10, 64)
	}

	if wcuStr != "" {
		wcuVal, _ = strconv.ParseInt(wcuStr, 10, 64)
	}

	if switchToProvisioned {
		if rcuStr == "" {
			rcuVal = int64(DefaultRcu)
		}

		if wcuStr == "" {
			wcuVal = int64(DefaultWcu)
		}

		input = &dynamodb.UpdateTableInput{
			TableName:   &tableName,
			BillingMode: types.BillingModeProvisioned,
			ProvisionedThroughput: &types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(rcuVal),
				WriteCapacityUnits: aws.Int64(wcuVal),
			},
		}
	} else {
		input = &dynamodb.UpdateTableInput{
			TableName: &tableName,
			ProvisionedThroughput: &types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(rcuVal),
				WriteCapacityUnits: aws.Int64(wcuVal),
			},
		}
	}

	_, err := dbmgr.DynamoDBClient.UpdateTable(context.Background(), input)
	if err != nil {
		dbmgr.Logger.Errorf("Error updating provisioned capacity: %v", err)
	} else {
		dbmgr.Logger.Infof("Provisioned capacity updated for table:%s - RCU: %d, WCU: %d", tableName, rcuVal, wcuVal)
	}

	return err
}

// SwitchToOnDemandCapacity switches a DynamoDB table to on-demand capacity mode.
// It returns an error if the switch fails.
func SwitchToOnDemandCapacity(dbmgr *DynamoDBManager, tableName string) error {
	input := &dynamodb.UpdateTableInput{
		TableName:   &tableName,
		BillingMode: types.BillingModePayPerRequest,
	}

	_, err := dbmgr.DynamoDBClient.UpdateTable(context.TODO(), input)
	if err != nil {
		dbmgr.Logger.Errorf("error switching to on-demand capacity: %v", err)
	} else {
		dbmgr.Logger.Infof("Switched to on-demand capacity for table: %s\n", tableName)
	}

	return err
}

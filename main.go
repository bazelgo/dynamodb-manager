package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/bazelgo/dynamodb-manager/client"
	"github.com/bazelgo/dynamodb-manager/search"
	"github.com/bazelgo/dynamodb-manager/update"
)

// Actions the program can take
const (
	Search string = "search"
	Update string = "update"
)

var ExecuteSearchTask = search.ExecuteSearch
var ExecuteUpdateTask = update.ExecuteUpdate

var searchTerm string
var tagValue string
var updateTable string
var rcuValueStr string
var wcuValueStr string
var provisioned bool
var onDemand bool

var usageStr string = `./dynamodb-manager [--help]
./dynamodb-manager [--level LOG_LVL] [--profile NAME] search TABLE [--tag TAG]
./dynamodb-manager [--level LOG_LVL] [--profile NAME] update TABLE [--ondemand|--provisioned] [--rcu READ_CAP] [--wcu WRITE_CAP]`

var rootCmd = &cobra.Command{
	Use:   usageStr,
	Short: "Manage DynamoDB tables with fuzzy search and update capabilities",
	Long:  "Manage DynamoDB tables with fuzzy search and update capabilities",
	RunE: func(cmd *cobra.Command, args []string) error {
		searchTerm = viper.GetString("search")
		tagValue = viper.GetString("tag")
		updateTable = viper.GetString("update")
		rcuValueStr = viper.GetString("rcu")
		wcuValueStr = viper.GetString("wcu")
		provisioned = viper.GetBool("provisioned")
		onDemand = viper.GetBool("ondemand")

		return checkCommand()
	},
}

// isValidSearchCommand checks if the command is valid for searching.
func isValidSearchCommand() bool {
	return (searchTerm != "" || tagValue != "") && (updateTable == "")
}

// isValidUpdateCommand checks if the command is valid for updating.
func isValidUpdateCommand() bool {
	return updateTable != "" && searchTerm == "" && tagValue == ""
}

// checkCommand checks the validity of the command line arguments.
// It returns an error if the arguments are not valid.
func checkCommand() error {

	if !isValidSearchCommand() && !isValidUpdateCommand() {
		return errors.New("Invalid Search or Valid Command!")
	} 

	if (searchTerm != "" || tagValue != "") && (rcuValueStr != "" || wcuValueStr != "" || provisioned || onDemand) {
		return errors.New("Invalid command line arguments: search or tag cannot be used together with rcu, wcu, provisioned, ondemand!")
	}

	if updateTable != "" && (searchTerm != "" || tagValue != "") {
		return errors.New("Invalid command line arguments: update can't be used together with search or tag!")
	}

	if updateTable != "" && rcuValueStr == "" && wcuValueStr == "" && !provisioned && !onDemand {
		return errors.New("Invalid command line arguments: no rcu or wcu or provisioned or onDemand is provided!")
	}

	if updateTable != "" && onDemand && (rcuValueStr != "" || wcuValueStr != "") {
		return errors.New("Invalid command line arguments: ondemand model does not support rcu or wcu!")
	}

	if rcuValueStr != "" {
		_, err := strconv.ParseInt(rcuValueStr, 10, 64)
		if err != nil {
			return errors.New(fmt.Sprintf("Invalid command line arguments: rcuValue:%s - error:%v", rcuValueStr, err))
		}
	}

	if wcuValueStr != "" {
		_, err := strconv.ParseInt(wcuValueStr, 10, 64)
		if err != nil {
			return errors.New(fmt.Sprintf("Invalid command line arguments: wcuValue:%s - error:%v", wcuValueStr, err))
		}
	}
	return nil
}

// dumpParams logs the passed arguments to the logger in debug mode.
func dumpParams(dbmgr *client.DynamoDBManager) {
	dbmgr.Logger.Debugf("Debug info - passed args listed here:")
	dbmgr.Logger.Debugf("Search Term: %s\n", searchTerm)
	dbmgr.Logger.Debugf("Tag Value: %s\n", tagValue)
	dbmgr.Logger.Debugf("Update Table: %s\n", updateTable)
	dbmgr.Logger.Debugf("RCU Value: %s\n", rcuValueStr)
	dbmgr.Logger.Debugf("WCU Value: %s\n", wcuValueStr)
	dbmgr.Logger.Debugf("Provisioned: %t\n", provisioned)
	dbmgr.Logger.Debugf("On-Demand: %t\n", onDemand)
}

// initCommand initializes the command-line flags, parses them, and binds them to viper.
// It returns an error if there's any issue with the command line arguments.
func initCommand() error {
	rootCmd.PersistentFlags().StringP("level", "", "Info", "Setup the log level")
	rootCmd.PersistentFlags().StringP("search", "", "", "Search term for DynamoDB table names")
	rootCmd.PersistentFlags().StringP("tag", "", "", "Value of the tag for DynamoDB table search")
	rootCmd.PersistentFlags().StringP("update", "", "", "Name of the DynamoDB table to update")
	rootCmd.PersistentFlags().StringP("rcu", "", "", "Read Capacity Units")
	rootCmd.PersistentFlags().StringP("wcu", "", "", "Write Capacity Units")
	rootCmd.PersistentFlags().Bool("provisioned", false, "Provisioned capacity mode")
	rootCmd.PersistentFlags().Bool("ondemand", false, "On-Demand capacity mode")

	viper.BindPFlags(rootCmd.PersistentFlags())

	cobra.EnableCommandSorting = false
	rootCmd.SetFlagErrorFunc(func(cmd *cobra.Command, err error) error {
		return errors.New(fmt.Sprintf("Failed to parse command line args:%v", err))
	})

	if err := rootCmd.Execute(); err != nil {
		return errors.New(fmt.Sprintf("Failed to parse command line args:%v", err))
	}
	return nil
}

// run configures and executes the program's workflow based on the specified action.
//
// It takes a DynamoDB manager, 'dbmgr', and an action string as parameters.
// The action string determines the specific workflow to be executed, either 'Search' or 'Update'.
//
// If the action is 'Search', it calls ExecuteSearchTask with the search term and tag retrieved from command-line flags.
// If the action is 'Update', it calls ExecuteUpdateTask with the update table name, read and write capacity units,
// on-demand and provisioned flags retrieved from command-line flags.
//
// Returns an error if the action is unrecognized or if there's an error during execution.
func run(dbmgr *client.DynamoDBManager, action string) error {
	switch action {
	case Search:
		ExecuteSearchTask(dbmgr, viper.GetString("search"), viper.GetString("tag"))
	case Update:
		ExecuteUpdateTask(dbmgr, viper.GetString("update"), viper.GetString("rcu"), viper.GetString("wcu"), viper.GetBool("ondemand"), viper.GetBool("provisioned"))
	default:
		return errors.New(fmt.Sprintf("unrecognized action provided:%s", action))
	}
	return nil
}

// main invokes the program's workflow and handles errors by returning an exit status of 1.
func main() {
	err_cmd := initCommand()
	if err_cmd != nil {
		fmt.Printf("%v", err_cmd)
		os.Exit(1)
	}

	dbmgr, err := client.CreateNewDynamoDBManager(viper.GetString("profile"))
	if err != nil {
		fmt.Printf("Failed to create DynamoDB client due to: %v", err)
		os.Exit(1)
	}

	err = client.SetupLogger(dbmgr, viper.GetString("level"))
	if err != nil {
		fmt.Printf("SetupLogger failed due to:%v", err)
		os.Exit(1)
	}

	dumpParams(dbmgr)

	if viper.GetString("update") != "" {
		err := run(dbmgr, "update")
		if err != nil {
			dbmgr.Logger.Errorf("Failed to update the dynamodb table:%s , due to: %v", viper.GetString("update"), err)
		}
	} else {
		err := run(dbmgr, "search")
		if err != nil {
			dbmgr.Logger.Errorf("Failed to search dynamodb table due to: %v", err)
		}
	}
	os.Exit(1)
}

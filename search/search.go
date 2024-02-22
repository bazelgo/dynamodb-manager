package search

import (
	"strings"

	"github.com/bazelgo/dynamodb-manager/client"
	"github.com/texttheater/golang-levenshtein/levenshtein"
)

const FuzzyRatio = 80

var (
	GetTableListClient = client.GetTableList
	GetTableArnClient  = client.GetTableArn
	GetTableTagsClient = client.GetTableTags
)

// NormalizeRatio normalizes the fuzzy ratio to be between 0 and 100.
// It takes an integer ratio as input and returns the normalized ratio.
func NormalizeRatio(ratio int) int {
	if ratio < 0 {
		return 0
	} else if ratio > 100 {
		return 100
	}
	return ratio
}

// FuzzyMatchRatio calculates the fuzzy match ratio between two strings.
// It takes two strings as input and returns the fuzzy match ratio as an integer.
func FuzzyMatchRatio(str1 string, str2 string) int {
	distance := levenshtein.DistanceForStrings([]rune(str1), []rune(str2), levenshtein.DefaultOptions)
	maxLen := len(str1)
	if len(str2) > maxLen {
		maxLen = len(str2)
	}

	if maxLen == 0 {
		return 100
	}

	return ((maxLen - distance) * 100) / maxLen
}

// searchTablesByFuzzyName searches DynamoDB tables by fuzzy name using the provided DynamoDBManager.
// It takes a DynamoDBManager and a fuzzy name as input and returns a slice of matching tables.
func searchTablesByFuzzyName(dbmgr *client.DynamoDBManager, fuzzyName string) []map[string]string {
	// Get the list of table names
	tableList, err := GetTableListClient(dbmgr)
	if err != nil {
		dbmgr.Logger.Errorf("Error finding DynamoDB tables: %v", err)
		return nil
	}

	// Perform fuzzy search and filter matching tables
	dbmgr.Logger.Info("searchTablesByFuzzyName before")
	matchingTables := make([]map[string]string, 0)
	for _, tableName := range tableList {
		if !strings.Contains(tableName, fuzzyName) {
			fuzzyRatio := FuzzyMatchRatio(strings.ToLower(fuzzyName), strings.ToLower(tableName))
			similarityScore := NormalizeRatio(fuzzyRatio)
			dbmgr.Logger.Debugf("Calculating: fuzzyname:%s - tablename:%s - similarityScore: %d\n", strings.ToLower(fuzzyName), strings.ToLower(tableName), similarityScore)
			if similarityScore < FuzzyRatio {
				continue
			}
		}
		tableArn, err := GetTableArnClient(dbmgr, tableName)
		if err != nil {
			dbmgr.Logger.Warnf("Error getting table ARN: %v", err)
			continue
		}
		dbmgr.Logger.Infof("searchTablesByFuzzyName: fuzzyname:%s - tablename:%s - tableArn: %s\n", strings.ToLower(fuzzyName), strings.ToLower(tableName), tableArn)
		matchingTables = append(matchingTables, map[string]string{"Name": tableName, "ARN": tableArn})

	}
	return matchingTables
}

// searchTablesByTagValue searches DynamoDB tables by tag value using the provided DynamoDBManager and a list of table names.
// It takes a DynamoDBManager, a tag value, and a slice of table names as input and returns a slice of matching tables.
func searchTablesByTagValue(dbmgr *client.DynamoDBManager, tagValue string, tableList []string) []map[string]string {
	var matchingTables []map[string]string
	var tableListTag []string
	var errGetTable error

	if tableList != nil {
		tableListTag = make([]string, len(tableList))
		copy(tableListTag, tableList)
	} else {
		tableListTag, errGetTable = GetTableListClient(dbmgr)
		if errGetTable != nil {
			dbmgr.Logger.Errorf("Error finding DynamoDB tables: %v", errGetTable)
			return nil
		}
	}

	// Iterate over the tableList and check tags
	for _, tableName := range tableListTag {
		dbmgr.Logger.Infof("Check the tags of table name: %s\n", tableName)
		tableArn, err := GetTableArnClient(dbmgr, tableName)
		if err != nil {
			dbmgr.Logger.Warnf("Error getting table ARN: %v", err)
			continue
		}

		tags, errTag := GetTableTagsClient(dbmgr, tableArn)
		if errTag != nil {
			dbmgr.Logger.Warnf("Get tags for arn:%s, failed due to:%v", tableArn, errTag)
			continue
		}
		// Check if tagValue matches any tag in the list
		for _, tag := range tags {
			dbmgr.Logger.Debugf("table_name: %s - tableArn: %s, Key: %s, Value: %s\n", tableName, tableArn, *tag.Key, *tag.Value)
			if *tag.Value == tagValue {
				matchingTables = append(matchingTables, map[string]string{"Name": tableName, "ARN": tableArn})
				break
			}
		}
	}

	return matchingTables
}

// ExecuteSearch performs a search operation based on the provided conditions such as fuzzy table name and tag value.
// It takes a DynamoDBManager, a fuzzy table name, and a tag value as input and returns a slice of matching tables.
func ExecuteSearch(dbmgr *client.DynamoDBManager, tableFuzzyName string, tagValue string) []map[string]string {
	var matchingTables []map[string]string
	if tableFuzzyName != "" && tagValue != "" {
		dbmgr.Logger.Infof("Begin to search the matched tables via fuzzy name:%s, tag:%s, ...", tableFuzzyName, tagValue)
		fuzzyMatchingTables := []map[string]string{}
		fuzzyMatchingTables = searchTablesByFuzzyName(dbmgr, tableFuzzyName)
		var tableList []string
		for _, entry := range fuzzyMatchingTables {
			name, exists := entry["Name"]
			if exists {
				tableList = append(tableList, name)
			}
		}
		matchingTables = searchTablesByTagValue(dbmgr, tagValue, tableList)
	} else if tableFuzzyName != "" {
		dbmgr.Logger.Infof("Begin to search the matched tables via fuzzy name:%s, ...", tableFuzzyName)
		matchingTables = searchTablesByFuzzyName(dbmgr, tableFuzzyName)
	} else if tagValue != "" {
		dbmgr.Logger.Infof("Begin to search the matched tables via tag:%s, ...", tagValue)
		matchingTables = searchTablesByTagValue(dbmgr, tagValue, nil)
	} else {
		dbmgr.Logger.Error("Invalid search conditions: search table name or tag value should not be empty!")
		return nil
	}

	if matchingTables == nil || len(matchingTables) == 0 {
		dbmgr.Logger.Warnf("Empty search results - please check the search conditions, tableFuzzyName:%s - tagValue:%s", tableFuzzyName, tagValue)
	}

	dbmgr.Logger.Info("Search results:")
	for _, table := range matchingTables {
		dbmgr.Logger.Infof("Table Name: %s, ARN: %s\n", table["Name"], table["ARN"])
	}

	return matchingTables
}

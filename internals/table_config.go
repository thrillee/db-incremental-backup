package internals

import (
	"encoding/json"
	"log"
	"os"
)

type TableConfig struct {
	TableName string
	DateField string
}

var RegisteredTables map[string]TableConfig

func SetupTable(tableConfigDir string) {
	jsonData, err := os.ReadFile(tableConfigDir)
	if err != nil {
		log.Fatal(err)
	}

	configs := []TableConfig{}
	err = json.Unmarshal(jsonData, &configs)
	errCheck(err)

	tableDateMap := make(map[string]TableConfig)
	for _, tableConfig := range configs {
		tableDateMap[tableConfig.TableName] = tableConfig
	}

	RegisteredTables = tableDateMap
}

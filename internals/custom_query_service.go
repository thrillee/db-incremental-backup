package internals

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
)

type QuerySetup struct {
	Query          string
	RequiredParams []string
}

type CustomQuerySetup struct {
	QueryName string
	Setup     QuerySetup
}

var customQueryStore map[string]QuerySetup

func LoadCustomerQuery(custom_query_path string) {
	jsonData, err := os.ReadFile(custom_query_path)
	if err != nil {
		log.Fatal(fmt.Sprintf("Load Custom Query Config Failed => %v", err))
	}

	configs := []CustomQuerySetup{}
	err = json.Unmarshal(jsonData, &configs)
	errCheck(err)

	cqs := make(map[string]QuerySetup)
	for _, s := range configs {
		setup := s.Setup
		sort.Strings(setup.RequiredParams)
		cqs[s.QueryName] = setup
	}

	customQueryStore = cqs
}

func ProcessQuery(query_name string, requestParams []string) {
	log.Println(fmt.Sprintf("<<<<<<<<<<<<<<<<<Running Custom Query For %s>>>>>>>>>>>>>>>>>", query_name))
	defer log.Printf("<<<<<<<<<<<<<<<<<Custom Query Ended>>>>>>>>>>>>>>>>>\n")

	qs, ok := customQueryStore[query_name]
	if !ok {
		log.Fatal("Invalid query")
	}

	log.Println(requestParams)

	parsedParams, paramKeys := handleParams(requestParams)

	if !validateParams(qs.RequiredParams, paramKeys) {
		log.Fatal(fmt.Sprintf("Invalid Params Passed: Required Params=%v", qs.RequiredParams))
	}

	query := qs.Query
	for _, requiredKey := range paramKeys {
		p := fmt.Sprintf(":%s", requiredKey)
		query = strings.ReplaceAll(query, p, parsedParams[requiredKey])
	}
	log.Println("Running query. Please wait...")

	_, err := db.Exec(query)
	errCheck(err)
}

func parseRequestParam(reqParam string) (key, value string) {
	splitedValue := strings.Split(reqParam, "=")
	if len(splitedValue) != 2 {
		log.Fatal(fmt.Sprintf("Unable to parse params => %s", reqParam))
	}

	return splitedValue[0], splitedValue[1]
}

func validateParams(required_params, params []string) bool {
	sort.Strings(params)

	total_required_params := len(required_params)
	if total_required_params != len(params) {
		return false
	}

	for i := 0; i < total_required_params; i++ {
		if required_params[i] != params[i] {
			return false
		}
	}

	return true
}

func handleParams(requestParams []string) (map[string]string, []string) {
	params := make(map[string]string)

	reqParamKeys := []string{}

	for _, rp := range requestParams {
		key, value := parseRequestParam(rp)
		reqParamKeys = append(reqParamKeys, key)

		params[key] = value
	}

	return params, reqParamKeys
}

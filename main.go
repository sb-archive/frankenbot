// Package frankenbot provides Springbot data extraction,
// transformation, and transferring from a source
// database to a destination database
package main

import (
	"io/ioutil"
	"log"
	"os"
	"sync"

	"gopkg.in/yaml.v2"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Frankenbot needs a config file to lurch to life")
	}

	configFile := os.Args[1]
	configData, err := ioutil.ReadFile(configFile)

	if err != nil {
		log.Fatal("Line 26: ", err)
	}

	var config Config
	yaml.Unmarshal(configData, &config)

	wg := &sync.WaitGroup{}
	wg.Add(2)
	go ExtractPostgres(config, wg)
	go ExtractMongo(config, wg)
	wg.Wait()
}

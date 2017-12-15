package main

import (
	"io/ioutil"
	"log"
	"os"

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

	var progress int
	done := make(chan bool)

	go ExtractPostgres(config, done)
	go ExtractMongo(config, done)

	for {
		select {
		case <-done:
			progress++

			if progress >= 2 {
				return
			}
		}
	}
}

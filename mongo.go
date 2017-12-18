package main

import (
	"fmt"
	"log"
	"sync"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// ExtractMongo is the entry point for MongoDB extraction.
// It uses the fan out pattern to create indexes for all of
// the target collections, and again for extraction each
// set of documents for each store in each collection
func ExtractMongo(config Config, parentGroup *sync.WaitGroup) {
	defer parentGroup.Done()
	sourceConnStr := fmt.Sprintf(`mongodb://%s:%s@`, config.MSource["username"], config.MSource["password"])
	sourceConnStr += config.MSourceHosts[0]
	for _, host := range config.MSourceHosts[1:] {
		sourceConnStr += fmt.Sprintf(",%s", host)
	}
	sourceConnStr += fmt.Sprintf("/%s", config.MSource["name"])

	destinationConnStr := fmt.Sprintf("mongodb://%s:%s@", config.MDestination["username"], config.MDestination["password"])
	destinationConnStr += config.MDestinationHosts[0]
	for _, host := range config.MDestinationHosts[1:] {
		destinationConnStr += fmt.Sprintf(",%s", host)
	}
	destinationConnStr += fmt.Sprintf("/%s", config.MDestination["name"])

	sourceSession, err := mgo.Dial(sourceConnStr)
	if err != nil {
		log.Fatal("mongo.go:33: ", err)
	}
	defer sourceSession.Close()

	destinationSession, err := mgo.Dial(destinationConnStr)
	if err != nil {
		log.Fatal("mongo.go:39: ", err)
	}
	defer destinationSession.Close()

	sourceDB := sourceSession.DB(config.MSource["name"])
	destinationDB := destinationSession.DB(config.MDestination["name"])

	for _, collection := range config.TargetCollections {
		log.Printf("Ensuring indexes for %s", collection)

		go EnsureIndex(collection, sourceDB, destinationDB)
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(config.MatchingIds) * len(config.TargetCollections))
	for _, id := range config.MatchingIds {
		log.Printf("*** Extracting collections for store %v ***", id)

		for _, collection := range config.TargetCollections {
			log.Printf("Extracting %s for store %v", collection, id)

			go ExtractCollection(collection, id, config.Filters, config.Since, sourceDB, destinationDB, wg)
		}
	}
	wg.Wait()

	log.Println("*** Finished extracting MongoDB ***")
}

// ExtractCollection extracts mongodb collections
func ExtractCollection(collection string, id int, collectionFilters map[string]map[string]string, since map[string]map[string]string, source, destination *mgo.Database, wg *sync.WaitGroup) {
	defer wg.Done()
	sourceCollection := source.C(collection)
	destinationCollection := destination.C(collection)
	doc := &bson.D{}

	query := bson.M{"store_id": id}
	if sinceMap, ok := since[collection]; ok {
		for field, date := range sinceMap {
			query[field] = bson.M{"gte": date}
		}
	}

	iter := sourceCollection.Find(query).Iter()

	for iter.Next(doc) {
		if filters, ok := collectionFilters[collection]; ok {
			filtered := doc.Map()

			for filter, replacement := range filters {
				filtered[filter] = replacement
			}

			destinationCollection.Insert(filtered)
		} else {
			destinationCollection.Insert(doc)
		}
	}

	log.Printf("*** Finished extracting %s for store %v ***", collection, id)
}

// EnsureIndex creates indexes for mongo collections
func EnsureIndex(collection string, source, destination *mgo.Database) {
	sCollection := source.C(collection)
	dCollection := destination.C(collection)

	indexes, err := sCollection.Indexes()
	if err != nil {
		log.Println("mongo.go:105: ", err)
	}
	for _, index := range indexes {
		err = dCollection.EnsureIndex(index)
		if err != nil {
			log.Println("mongo.go:110: ", err)
		}
	}
}

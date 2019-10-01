package main

import (
	"log"

	"github.com/yhyddr/kv/buntdb"
)

func main() {
	// Will Create if not exists.
	db, err := buntdb.Open("data.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	memory, err := buntdb.Open(":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer memory.Close()
}

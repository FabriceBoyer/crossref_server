package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"common_go_utils/utils"
	"crossref_server/crossref"

	"github.com/gorilla/mux"
)

var root_path = utils.GetEnv("DUMP_PATH", "E:/data/crossref_dump/2023")

var mgr = crossref.CrossrefMetadataManager{Root_path: root_path}

func main() {
	err := mgr.InitializeManager()
	if err != nil {
		panic(err)
	}

	fmt.Print("Serving requests\n")
	handleRequests()
}

func handleRequests() {
	router := mux.NewRouter().StrictSlash(true)
	router.Handle("/", http.FileServer(http.Dir("./static")))
	router.HandleFunc("/id", utils.ErrorHandler(handlePage))

	log.Fatal(http.ListenAndServe(":9098", router))
}

func handlePage(w http.ResponseWriter, r *http.Request) error {
	doi := r.URL.Query().Get("doi")

	elm, err := mgr.GetIndexedCrossrefMetadata(doi)
	if err != nil {
		return err
	}

	val, err := json.MarshalIndent(elm, "", " ")
	if err != nil {
		return err
	}

	_, err = w.Write([]byte(string(val)))
	if err != nil {
		return err
	}

	return nil
}

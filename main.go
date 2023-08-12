package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/fabriceboyer/common_go_utils/utils"
	"github.com/fabriceboyer/crossref_server/crossref"
	"github.com/gorilla/mux"
	"github.com/spf13/viper"
)

var mgr = crossref.CrossrefMetadataManager{}

func main() {
	err := utils.SetupConfig()
	if err != nil {
		panic(err)
	}

	rootPath := viper.GetString("DUMP_PATH")
	mgr.Root_path = rootPath
	fmt.Printf("Root path: %s\n", rootPath)

	err = mgr.InitializeManager()
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

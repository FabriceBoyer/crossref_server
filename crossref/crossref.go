package crossref

import (
	"bufio"
	"compress/gzip"
	"core/utils"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

const indexFileName = "crossref-metadata-index.txt"
const indexSeparator = "#"

type CrossrefAuthor struct {
	Given  string `json:"given"`
	Family string `json:"family"`
}
type CrossrefReference struct {
	Key  string `json:"key"`
	DOI  string `json:"doi"`
	ISSN string `json:"ISSN"`
}
type CrossrefMetadata struct {
	Member         string              `json:"member"` // not int
	ISSN           []string            `json:"ISSN"`
	ContainerTitle []string            `json:"container-title"`
	Author         []CrossrefAuthor    `json:"author"`
	DOI            string              `json:"DOI"`
	Subject        []string            `json:"subject"`
	Reference      []CrossrefReference `json:"reference"`
	Title          []string            `json:"title"`
	Source         string              `json:"source"`
	Type           string              `json:"type"`
	Publisher      string              `json:"publisher"`
	Language       string              `json:"language"`
}
type CrossrefMetadataList struct {
	Items []CrossrefMetadata `json:"items"`
}

func (m *CrossrefMetadata) String() string {
	jsonBytes, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Sprint("Error:", err)
	}

	return string(jsonBytes)
}

type CrossrefMetadataIndex struct {
	doi string
	pos CrossrefPos
}

type CrossrefPos struct {
	fileId int64
	seek   int64
}

type CrossrefMetadataManager struct {
	Root_path string
	index     map[string]CrossrefPos
}

func (mgr *CrossrefMetadataManager) InitializeManager() error {
	regenerate := !utils.FileExists(mgr.getIndexFileName())
	//TODO check if all files in data folder are older than index
	if regenerate {
		fmt.Print("Index file missing or out of date, regenerating...\n")
		if err := mgr.generateCrossrefMetadataIndex(); err != nil {
			return err
		}
	} else {
		fmt.Print("Reusing existing file index\n")
	}
	if err := mgr.readCrossrefMetadataIndex(); err != nil {
		return err
	}
	return nil
}

func (mgr *CrossrefMetadataManager) generateCrossrefMetadataIndex() error {
	fmt.Print("Generating index file\n")
	index := []CrossrefMetadataIndex{}

	files, err := os.ReadDir(mgr.Root_path)
	if err != nil {
		return err
	}

	filesToBeProcessed := []string{}
	for _, file := range files {
		fileName := file.Name()
		if strings.HasSuffix(fileName, ".json.gz") {
			filesToBeProcessed = append(filesToBeProcessed, fileName)
		}
	}

	results := make(chan *[]CrossrefMetadataIndex)
	routineCount := runtime.NumCPU()
	fileCount := len(filesToBeProcessed)
	fileBlockSize := fileCount / routineCount
	var wg sync.WaitGroup
	wg.Add(routineCount)

	// parallelize work
	fmt.Printf("%d routines processing %d files in blocks of %d\n", routineCount, fileCount, fileBlockSize)
	for i := 0; i < routineCount; i++ {
		go mgr.generatePartialCrossrefMetadataIndex(filesToBeProcessed[i*fileBlockSize:utils.Min(fileCount, (i+1)*fileBlockSize)], results)
	}
	wg.Wait()
	close(results)

	for result := range results {
		index = append(index, *result...)
	}

	fmt.Print("Writing index file\n")
	index_file, err := os.Create(mgr.getIndexFileName())
	if err != nil {
		return err
	}
	defer index_file.Close()

	for _, elm := range index {
		index_file.WriteString(fmt.Sprintf("%s%s%d\n",
			elm.doi, indexSeparator,
			elm.pos.fileId))
	}

	return nil
}

func (mgr *CrossrefMetadataManager) generatePartialCrossrefMetadataIndex(files []string, results chan<- *[]CrossrefMetadataIndex) error {
	index := []CrossrefMetadataIndex{}
	for _, fileName := range files {

		fileNameWithoutExt := strings.ReplaceAll(fileName, ".json.gz", "")

		fileId, err := strconv.ParseInt(fileNameWithoutExt, 10, 64)
		if err != nil {
			return err
		}

		f, err := os.Open(path.Join(mgr.Root_path, fileName))
		if err != nil {
			return err
		}
		defer f.Close()

		gzipReader, err := gzip.NewReader(f)
		if err != nil {
			return err
		}
		defer gzipReader.Close()

		d := json.NewDecoder(gzipReader)
		var metaDataList CrossrefMetadataList

		err = d.Decode(&metaDataList)
		if err != nil {
			return err
		}

		for _, elm := range metaDataList.Items {
			if elm.DOI != "" {
				index = append(index, CrossrefMetadataIndex{
					doi: elm.DOI,
					pos: CrossrefPos{fileId: fileId, seek: 0}, // seek not available in gzip
				})
			}
		}

		// fmt.Printf("%s elements parsed, current file is %d\n", humanize.SI(float64(len(index)), ""), fileId)
	}

	results <- &index
	return nil
}

func (mgr *CrossrefMetadataManager) GetIndexedCrossrefMetadata(doi string) (*CrossrefMetadata, error) {
	pos, found := mgr.index[doi]
	if !found {
		return nil, fmt.Errorf("index %s not found", doi)
	}

	res, err := mgr.getCrossrefMetadaFromPos(pos, doi)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (mgr *CrossrefMetadataManager) getCrossrefMetadaFromPos(pos CrossrefPos, doi string) (*CrossrefMetadata, error) {
	fileName := fmt.Sprintf("%d.json.gz", pos.fileId)

	f, err := os.Open(path.Join(mgr.Root_path, fileName))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	gzipReader, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gzipReader.Close()

	//gzip seek not available
	d := json.NewDecoder(gzipReader)
	metaDataList := &CrossrefMetadataList{}
	err = d.Decode(metaDataList)
	if err != nil {
		return nil, err
	}

	for _, elm := range metaDataList.Items {
		if elm.DOI == doi {
			return &elm, nil
		}
	}

	return nil, fmt.Errorf("doi %s not found in file %d", doi, pos.fileId)
}

func (mgr *CrossrefMetadataManager) getIndexFileName() string {
	return path.Join(mgr.Root_path, indexFileName)
}

func (mgr *CrossrefMetadataManager) readCrossrefMetadataIndex() error {
	fmt.Print("Reading index file\n")
	mgr.index = make(map[string]CrossrefPos)

	readFile, err := os.Open(mgr.getIndexFileName())
	if err != nil {
		return err
	}
	defer readFile.Close()

	fileScanner := bufio.NewScanner(readFile)
	fileScanner.Split(bufio.ScanLines)

	for fileScanner.Scan() {
		line := fileScanner.Text()
		parts := strings.Split(line, indexSeparator)
		if len(parts) != 2 {
			return fmt.Errorf("expected 2 parts in line %s, got %s", line, parts)
		}
		doi := parts[0]
		fileId, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return err
		}
		mgr.index[doi] = CrossrefPos{fileId: fileId, seek: 0} //seek not available in gzip
	}

	return nil
}

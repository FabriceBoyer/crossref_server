package crossref

import (
	"common_go_utils/utils"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/dustin/go-humanize"
	"github.com/steveyen/gkvlite"
)

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

func (l *CrossrefMetadataList) GetDoisList() (*[]string, error) {
	var dois []string
	for _, item := range l.Items {
		if item.DOI != "" {
			dois = append(dois, item.DOI)
		} else {
			return nil, fmt.Errorf("empty DOI found in %v", item)
		}

	}
	return &dois, nil
}

func (m *CrossrefMetadata) String() string {
	jsonBytes, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Sprint("Error:", err)
	}

	return string(jsonBytes)
}

type CrossrefMetadataIndex struct {
	Doi    string
	FileId int
}

type CrossrefMetadataManager struct {
	Root_path  string
	Store      *gkvlite.Store
	File       *os.File
	Collection *gkvlite.Collection
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

	filesToBeProcessed, err := mgr.getFilesList()
	if err != nil {
		return err
	}

	f, err := os.Create(mgr.getIndexFileName())
	if err != nil {
		return err
	}
	s, err := gkvlite.NewStore(f)
	if err != nil {
		return err
	}
	c := s.SetCollection("crossref", nil)

	routineCount := runtime.NumCPU()
	results := make(chan *CrossrefMetadataIndex, 1e6*routineCount)
	errors := make(chan error)
	finish := make(chan bool)
	fileCount := len(*filesToBeProcessed)
	fileBlockSize := fileCount/routineCount + 1

	var wg sync.WaitGroup
	wg.Add(routineCount)

	// parallelize work
	fmt.Printf("%d routines processing %d files in blocks of %d\n", routineCount, fileCount, fileBlockSize)
	for i := 0; i < routineCount; i++ {
		start := i * fileBlockSize
		end := utils.Min(fileCount, (i+1)*fileBlockSize-1)
		fmt.Printf("Starting go routine %d from %d to %d\n", i, start, end)
		go mgr.generatePartialCrossrefMetadataIndex(i, &wg, (*filesToBeProcessed)[start:end], results, errors)
	}
	go func() {
		wg.Wait()
		close(results)
		close(errors)
		fmt.Print("All go routines finished\n")
		finish <- true
	}()

	var counter int = 0
	var isDone bool = false
	for {
		var result *CrossrefMetadataIndex
		select {
		case result = <-results:
			fileIdStr := strconv.Itoa(result.FileId)
			c.Set([]byte(result.Doi), []byte(fileIdStr))

			//follow progress
			counter++
			if (counter % 1e6) == 0 {
				fmt.Printf("%v entries processed\n", humanize.SI(float64(counter), ""))
				s.Flush() // Persist all the changes to disk.
			}
		case err = <-errors:
			return err
		case <-finish:
			isDone = true
		}
		if isDone {
			break
		}
	}

	fmt.Print("Start writing index file\n")
	s.Flush()
	f.Sync()
	f.Close()
	fmt.Print("Finished writing index file\n")

	return nil
}

func (mgr *CrossrefMetadataManager) getFilesList() (*[]string, error) {
	files, err := os.ReadDir(mgr.Root_path)
	if err != nil {
		return nil, err
	}

	filesToBeProcessed := []string{}
	for _, file := range files {
		fileName := file.Name()
		if strings.HasSuffix(fileName, ".json.gz") {
			filesToBeProcessed = append(filesToBeProcessed, fileName)
		}
	}

	return &filesToBeProcessed, nil
}

func (mgr *CrossrefMetadataManager) generatePartialCrossrefMetadataIndex(routineId int, wg *sync.WaitGroup,
	files []string,
	results chan<- *CrossrefMetadataIndex, errors chan<- error) {

	defer wg.Done()

	var counter int = 0

	for _, fileName := range files {

		fileId, err := mgr.getFileIdFromFileName(fileName)
		if err != nil {
			errors <- err
		}

		f, err := os.Open(path.Join(mgr.Root_path, fileName))
		if err != nil {
			errors <- err
		}
		defer f.Close()

		gzipReader, err := gzip.NewReader(f)
		if err != nil {
			errors <- err
		}
		defer gzipReader.Close()

		d := json.NewDecoder(gzipReader)
		var metaDataList CrossrefMetadataList

		err = d.Decode(&metaDataList)
		if err != nil {
			errors <- err
		}

		for _, elm := range metaDataList.Items {
			if elm.DOI != "" {
				item := CrossrefMetadataIndex{
					Doi:    elm.DOI,
					FileId: fileId, // seek not available in gzip
				}
				results <- &item
				counter++
			}
		}
	}

	fmt.Printf("Routine %d finished with %s elements parsed\n", routineId, humanize.SI(float64(counter), ""))
}

func (*CrossrefMetadataManager) getFileIdFromFileName(fileName string) (int, error) {
	fileNameWithoutExt := strings.ReplaceAll(fileName, ".json.gz", "")

	fileId, err := strconv.Atoi(fileNameWithoutExt)
	if err != nil {
		return -1, err
	}
	return fileId, nil
}

func (mgr *CrossrefMetadataManager) getFileIdFromDoi(doi string) (int, error) {
	if mgr.Collection == nil {
		return -1, fmt.Errorf("store is not initialized")
	}

	fileIdStr, err := mgr.Collection.Get([]byte(doi))
	if err != nil {
		return -1, err
	}

	fileId, err := strconv.Atoi(string(fileIdStr))
	if err != nil {
		return -0, err
	}
	return fileId, nil
}

func (mgr *CrossrefMetadataManager) GetIndexedCrossrefMetadata(doi string) (*CrossrefMetadata, error) {
	fileId, err := mgr.getFileIdFromDoi(doi)
	if err != nil {
		return nil, err
	}

	res, err := mgr.getCrossrefMetadaFromFileIdAndDoi(fileId, doi)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (mgr *CrossrefMetadataManager) getCrossrefMetadaListFromFileId(fileId int) (*CrossrefMetadataList, error) {
	fileName := fmt.Sprintf("%d.json.gz", fileId)

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

	//seek not available in gzip

	d := json.NewDecoder(gzipReader)
	metaDataList := &CrossrefMetadataList{}
	err = d.Decode(metaDataList)
	if err != nil {
		return nil, err
	}

	return metaDataList, nil
}

func (mgr *CrossrefMetadataManager) getCrossrefMetadaFromFileIdAndDoi(fileId int, doi string) (*CrossrefMetadata, error) {

	metaDataList, err := mgr.getCrossrefMetadaListFromFileId(fileId)
	if err != nil {
		return nil, err
	}

	for _, elm := range metaDataList.Items {
		if elm.DOI == doi {
			return &elm, nil
		}
	}

	return nil, fmt.Errorf("DOI %s not found in file %d", doi, fileId)
}

func (mgr *CrossrefMetadataManager) getIndexFileName() string {
	const indexFileName = "crossref-metadata-index.gkvlite"
	return path.Join(mgr.Root_path, indexFileName)
}

func (mgr *CrossrefMetadataManager) readCrossrefMetadataIndex() error {
	fmt.Print("Reading index file\n")
	var err error
	mgr.File, err = os.Open(mgr.getIndexFileName())
	if err != nil {
		return nil
	}
	mgr.Store, err = gkvlite.NewStore(mgr.File)
	if err != nil {
		return nil
	}
	mgr.Collection = mgr.Store.GetCollection("crossref")
	if mgr.Collection == nil {
		return fmt.Errorf("collection crossref not found")
	}

	return nil
}

func (mgr *CrossrefMetadataManager) GetRandomDOIList(archiveFileCount int, doisPerArchive int) (*[]string, error) {

	files, err := mgr.getFilesList()
	if err != nil {
		return nil, err
	}

	randFiles := utils.GetRandomSample(*files, archiveFileCount)

	res := []string{}
	for _, file := range randFiles {
		fileId, err := mgr.getFileIdFromFileName(file)
		if err != nil {
			return nil, err
		}

		list, err := mgr.getCrossrefMetadaListFromFileId(fileId)
		if err != nil {
			return nil, err
		}

		dois, err := list.GetDoisList()
		if err != nil {
			return nil, err
		}

		res = append(res, utils.GetRandomSample(*dois, doisPerArchive)...)
	}

	return &res, nil
}

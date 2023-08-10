package crossref

import (
	"crossref_server/utils"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func newCrossrefManager() CrossrefMetadataManager {
	return CrossrefMetadataManager{Root_path: utils.GetEnv("DUMP_PATH", "E:/data/crossref_dump/2023")}
}

func BenchmarkGenerateCrossrefMetadataIndex(b *testing.B) {
	mgr := newCrossrefManager()
	err := mgr.generateCrossrefMetadataIndex()
	if err != nil {
		b.Error(err)
	}
}

func TestGetIndexedCrossrefMetadata(t *testing.T) {
	mgr := newCrossrefManager()
	err := mgr.InitializeManager()
	if err != nil {
		t.Error(err)
	}
	elm, err := mgr.GetIndexedCrossrefMetadata("10.1051/e3sconf/202337605013")
	if err != nil {
		t.Error(err)
	}
	val, err := json.Marshal(elm)
	if err != nil {
		t.Error(err)
	}
	assert.Contains(t, elm.Author[0].Family, "Kartsan")
	fmt.Printf("%v\n", string(val))
}

func TestGetCrossrefMetadaListFromFileId(t *testing.T) {
	mgr := newCrossrefManager()
	err := mgr.InitializeManager()
	if err != nil {
		t.Error(err)
	}

	list, err := mgr.getCrossrefMetadaListFromFileId(10)
	if err != nil {
		t.Error(err)
	}

	fmt.Print(list.Items)

	assert.Greater(t, len(list.Items), 0)
}

func BenchmarkGetIndexedCrossrefMetadata(b *testing.B) {
	mgr := newCrossrefManager()
	err := mgr.InitializeManager()
	if err != nil {
		b.Error(err)
	}

	dois, err := mgr.GetRandomDOIList(5, 100)
	if err != nil {
		b.Error(err)
	}

	b.Logf("Generated %d random dois\n", len(*dois))

	for _, doi := range *dois {
		elm, err := mgr.GetIndexedCrossrefMetadata(doi)
		if err != nil {
			b.Error(err)
		}
		b.Logf("%s\n", doi)
		assert.Equal(b, elm.DOI, doi)
	}
}

func TestGetFileIdFromDoi(t *testing.T) {
	mgr := newCrossrefManager()
	err := mgr.InitializeManager()
	if err != nil {
		t.Error(err)
	}

	dois, err := mgr.GetRandomDOIList(5, 500)
	if err != nil {
		t.Error(err)
	}

	t.Logf("Generated %d random dois\n", len(*dois))

	for _, doi := range *dois {
		fileId, err := mgr.getFileIdFromDoi(doi)
		if err != nil {
			t.Error(err)
		}
		t.Logf("%d\n", fileId)
	}

}

func TestGetFilesList(t *testing.T) {
	mgr := newCrossrefManager()
	//no need to initialize

	files, err := mgr.getFilesList()
	if err != nil {
		t.Error(err)
	}

	for _, file := range (*files)[:100] {
		fmt.Println(file)
	}

	assert.Greater(t, len(*files), 0)
}

func TestGetRandomDOIList(t *testing.T) {
	mgr := newCrossrefManager()
	// no need to initialize

	list, err := mgr.GetRandomDOIList(5, 100)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, len(*list), 500)

	for _, doi := range (*list)[:50] {
		fmt.Println(doi)
	}
}

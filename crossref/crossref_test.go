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

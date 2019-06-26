package taskdebug

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dpb587/bosh-log-tracer/log"
)

type InstanceAspectChangedMessage struct {
	RawMessage

	InstanceGroup string
	InstanceID    string
	InstanceIndex string // TODO int64?
	Aspect        string
	ChangedFrom   string
	ChangedTo     string
}

var _ log.Line = &InstanceAspectChangedMessage{}

type packageChangeSet map[string]struct {
	Name        string `json:"name"`
	Version     string `json:"version"`
	SHA1        string `json:"sha1"`
	BlobstoreID string `json:"blobstore_id"`
}

func (m InstanceAspectChangedMessage) GetChangedFromTags() map[string]interface{} {
	return m.getChangedTags(m.ChangedFrom)
}

func (m InstanceAspectChangedMessage) GetChangedToTags() map[string]interface{} {
	return m.getChangedTags(m.ChangedTo)
}

func (m InstanceAspectChangedMessage) getChangedTags(data string) map[string]interface{} {
	res := map[string]interface{}{}

	switch m.Aspect {
	case "stemcell":
		res["version"] = strings.TrimPrefix(data, "version: ")
	case "packages":
		var v packageChangeSet

		// older versions dumped with ruby
		data = strings.Replace(data, `"=>`, `":`, -1)

		err := json.Unmarshal([]byte(data), &v)
		if err != nil {
			panic(err)
		}

		for _, pkg := range v {
			res[fmt.Sprintf("%s.version", pkg.Name)] = pkg.Version
			res[fmt.Sprintf("%s.sha1", pkg.Name)] = pkg.SHA1
			res[fmt.Sprintf("%s.blobstore_id", pkg.Name)] = pkg.BlobstoreID
		}
	}

	return res
}

func (m InstanceAspectChangedMessage) GetChangedPackages() []string {
	var res []string

	// TODO

	return res
}

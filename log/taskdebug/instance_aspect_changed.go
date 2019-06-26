package taskdebug

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/dpb587/bosh-log-tracer/log"
)

var InstanceAspectChangedParser = instanceAspectChangedParser{}

type instanceAspectChangedParser struct{}

type InstanceAspectChangedMessage struct {
	RawMessage

	InstanceGroup string
	InstanceID    string
	InstanceIndex string // TODO int64?
	Aspect        string
	ChangedFrom   string
	ChangedTo     string
}

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

var _ log.Line = &InstanceAspectChangedMessage{}

// stemcell_changed? changed FROM: version: 315.36 TO: version: 315.41 on instance concourse/6318b9e7-8c72-4c4e-8769-e59abaa32297 (0)
var instanceAspectChangedOneRE = regexp.MustCompile(`^(.+)_changed\? changed FROM: (.+) TO: (.+) on instance ([^/]+)/([^ ]+) \((\d+)\)$`)

func (p instanceAspectChangedParser) Parse(inU log.Line) (log.Line, error) {
	in, ok := inU.(RawMessage)
	if !ok {
		return inU, nil
	}

	if in.Component != "DirectorJobRunner" {
		return inU, nil
	}

	if m := instanceAspectChangedOneRE.FindStringSubmatch(in.Message); len(m) > 0 {
		msg := InstanceAspectChangedMessage{
			RawMessage:    in,
			InstanceGroup: m[4],
			InstanceID:    m[5],
			InstanceIndex: m[6],
			Aspect:        m[1],
			ChangedFrom:   m[2],
			ChangedTo:     m[3],
		}

		return msg, nil
	}

	return inU, nil
}

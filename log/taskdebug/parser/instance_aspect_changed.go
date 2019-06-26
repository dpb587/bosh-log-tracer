package parser

import (
	"regexp"

	"github.com/dpb587/bosh-log-tracer/log"
	"github.com/dpb587/bosh-log-tracer/log/taskdebug"
)

var InstanceAspectChangedParser = instanceAspectChangedParser{}

type instanceAspectChangedParser struct{}

// stemcell_changed? changed FROM: version: 315.36 TO: version: 315.41 on instance concourse/6318b9e7-8c72-4c4e-8769-e59abaa32297 (0)
var instanceAspectChangedOneRE = regexp.MustCompile(`^(.+)_changed\? changed FROM: (.+) TO: (.+) on instance ([^/]+)/([^ ]+) \((\d+)\)$`)

func (p instanceAspectChangedParser) Parse(inU log.Line) (log.Line, error) {
	in, ok := inU.(taskdebug.RawMessage)
	if !ok {
		return inU, nil
	}

	if in.Component != "DirectorJobRunner" {
		return inU, nil
	}

	if m := instanceAspectChangedOneRE.FindStringSubmatch(in.Message); len(m) > 0 {
		msg := taskdebug.InstanceAspectChangedMessage{
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

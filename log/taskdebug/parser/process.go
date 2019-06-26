package parser

import (
	"regexp"

	"github.com/dpb587/bosh-log-tracer/log"
	"github.com/dpb587/bosh-log-tracer/log/taskdebug"
)

var ProcessParser = processParser{}

type processParser struct{}

// Running from worker 'worker_4' on director/e522142e-d0e2-4605-7c57-2cab3e749003 (127.0.0.1)
var processOneRE = regexp.MustCompile(`^Running from worker '([^']+)' on ([^/]+)/([^ ]+) \(([^\)]+)\)$`)

func (p processParser) Parse(inU log.Line) (log.Line, error) {
	in, ok := inU.(taskdebug.RawMessage)
	if !ok {
		return inU, nil
	}

	if in.Component != "DirectorJobRunner" {
		return inU, nil
	}

	if m := processOneRE.FindStringSubmatch(in.Message); len(m) > 0 {
		out := taskdebug.ProcessMessage{
			RawMessage:   in,
			WorkerName:   m[1],
			InstanceName: m[2],
			InstanceID:   m[3],
			IP:           m[4],
		}

		return out, nil
	}

	return inU, nil
}

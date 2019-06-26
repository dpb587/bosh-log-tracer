package taskdebug

import (
	"regexp"

	"github.com/dpb587/bosh-log-tracer/log"
)

var ProcessParser = processParser{}

type processParser struct{}

type ProcessMessage struct {
	RawMessage

	WorkerName   string
	InstanceName string
	InstanceID   string
	IP           string
}

var _ log.Line = &ProcessMessage{}

// Running from worker 'worker_4' on director/e522142e-d0e2-4605-7c57-2cab3e749003 (127.0.0.1)
var processOneRE = regexp.MustCompile(`^Running from worker '([^']+)' on ([^/]+)/([^ ]+) \(([^\)]+)\)$`)

func (p processParser) Parse(inU log.Line) (log.Line, error) {
	in, ok := inU.(RawMessage)
	if !ok {
		return inU, nil
	}

	if in.Component != "DirectorJobRunner" {
		return inU, nil
	}

	if m := processOneRE.FindStringSubmatch(in.Message); len(m) > 0 {
		out := ProcessMessage{
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

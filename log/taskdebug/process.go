package taskdebug

import (
	"github.com/dpb587/bosh-log-tracer/log"
)

type ProcessMessage struct {
	RawMessage

	WorkerName   string
	InstanceName string
	InstanceID   string
	IP           string
}

var _ log.Line = &ProcessMessage{}

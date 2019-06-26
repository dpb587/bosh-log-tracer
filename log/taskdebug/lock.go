package taskdebug

import (
	"github.com/dpb587/bosh-log-tracer/log"
)

type LockMessage struct {
	RawMessage

	Event string
	Name  string
	UID   string
}

var _ log.Line = &LockMessage{}

package taskdebug

import (
	"time"

	"github.com/dpb587/bosh-log-tracer/log"
)

type RawMessage struct {
	log.RawLine

	LogTime   time.Time
	LogLevel  string
	Process   string
	Tags      map[string]string
	Component string
	Message   string
}

var _ log.Line = &RawMessage{}

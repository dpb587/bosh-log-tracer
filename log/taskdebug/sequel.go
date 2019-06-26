package taskdebug

import (
	"time"

	"github.com/dpb587/bosh-log-tracer/log"
)

type SequelMessage struct {
	RawMessage

	Duration   time.Duration
	Connection string
	Query      string
}

var _ log.Line = &SequelMessage{}

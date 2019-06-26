package taskdebug

import (
	"time"

	"github.com/dpb587/bosh-log-tracer/log"
)

type CPIAWSRPCMessage struct {
	RawMessage

	Correlation   string
	Duration      time.Duration
	StatusCode    int
	Retries       int
	Payload       string
	PayloadMethod string
}

var _ log.Line = &CPIAWSRPCMessage{}

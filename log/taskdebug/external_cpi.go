package taskdebug

import "github.com/dpb587/bosh-log-tracer/log"

type ExternalCPIMessage struct {
	RawMessage

	Correlation string
	Event       string
	Remaining   string
}

var _ log.Line = &ExternalCPIMessage{}

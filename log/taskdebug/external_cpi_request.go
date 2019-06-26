package taskdebug

import "github.com/dpb587/bosh-log-tracer/log"

type ExternalCPIRequestMessage struct {
	ExternalCPIMessage

	Payload       string
	PayloadMethod string
	Command       string
}

var _ log.Line = &ExternalCPIRequestMessage{}

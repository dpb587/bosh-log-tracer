package taskdebug

import (
	"regexp"

	"github.com/dpb587/boshdebugtracer/log"
)

var ExternalCPIParser = externalCPIParser{}

type externalCPIParser struct{}

type ExternalCPIMessage struct {
	RawMessage

	Correlation string
	Event       string
	Remaining   string
}

// [external-cpi] [cpi-308955] request: {"method":"create_vm","arguments":[...
var externalCPIOneRE = regexp.MustCompile(`^\[external-cpi\] \[(cpi-\d+)\] (request|response): (.+)$`)

func (p externalCPIParser) Parse(inU log.Line) (log.Line, error) {
	in, ok := inU.(RawMessage)
	if !ok {
		return inU, nil
	}

	if in.Component != "DirectorJobRunner" {
		return inU, nil
	}

	if m := externalCPIOneRE.FindStringSubmatch(in.Message); len(m) > 0 {
		out := ExternalCPIMessage{
			RawMessage:  in,
			Correlation: m[1],
			Event:       m[2],
			Remaining:   m[3],
		}

		return out, nil
	}

	return inU, nil
}

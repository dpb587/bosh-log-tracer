package parser

import (
	"encoding/json"
	"regexp"

	"github.com/dpb587/bosh-log-tracer/log/taskdebug"
	"github.com/dpb587/bosh-log-tracer/log"
)

var ExternalCPIRequestParser = externalCPIRequestParser{}

type externalCPIRequestParser struct{}

// [external-cpi] [cpi-308955] request: {"method":"create_vm","arguments":[...
var externalCPIRequestOneRE = regexp.MustCompile(`(\{.+\}) with command: (.+)$`)

func (p externalCPIRequestParser) Parse(inU log.Line) (log.Line, error) {
	inU, err := ExternalCPIParser.Parse(inU)
	if inU == nil || err != nil {
		return inU, err
	}

	in, ok := inU.(taskdebug.ExternalCPIMessage)
	if !ok {
		return inU, nil
	}

	upstreamU, err := ExternalCPIParser.Parse(in)
	if upstreamU == nil || err != nil {
		return upstreamU, err
	}

	upstream := upstreamU.(taskdebug.ExternalCPIMessage)

	if upstream.Event != "request" {
		return upstream, nil
	}

	if m := externalCPIRequestOneRE.FindStringSubmatch(upstream.Remaining); len(m) > 0 {
		out := taskdebug.ExternalCPIRequestMessage{
			ExternalCPIMessage: upstream,
			Payload: m[1],
			Command:            m[2],
		}

		var payload struct {
			Method string `json:"method"`
		}

		err = json.Unmarshal([]byte(out.Payload), &payload)
		if err != nil {
			panic(err)
		}

		out.PayloadMethod = payload.Method

		return out, nil
	}

	return upstreamU, nil
}

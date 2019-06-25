package taskdebug

import (
	"encoding/json"
	"regexp"

	"github.com/dpb587/boshdebugtracer/log"
)

var ExternalCPIRequestParser = externalCPIRequestParser{}

type externalCPIRequestParser struct{}

type ExternalCPIRequestMessage struct {
	ExternalCPIMessage

	Payload       string
	PayloadMethod string
	Command       string
}

// [external-cpi] [cpi-308955] request: {"method":"create_vm","arguments":[...
var externalCPIRequestOneRE = regexp.MustCompile(`(\{.+\}) with command: (.+)$`)

func (p externalCPIRequestParser) Parse(inU log.Line) (log.Line, error) {
	inU, err := ExternalCPIParser.Parse(inU)
	if inU == nil || err != nil {
		return inU, err
	}

	in, ok := inU.(ExternalCPIMessage)
	if !ok {
		return inU, nil
	}

	upstreamU, err := ExternalCPIParser.Parse(in)
	if upstreamU == nil || err != nil {
		return upstreamU, err
	}

	upstream := upstreamU.(ExternalCPIMessage)

	if upstream.Event != "request" {
		return upstream, nil
	}

	if m := externalCPIRequestOneRE.FindStringSubmatch(upstream.Remaining); len(m) > 0 {
		out := ExternalCPIRequestMessage{
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

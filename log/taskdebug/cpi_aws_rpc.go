package taskdebug

import (
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/dpb587/boshdebugtracer/log"
)

var CPIAWSRPCParser = cpiAWSRPCParser{}

type cpiAWSRPCParser struct{}

type CPIAWSRPCMessage struct {
	RawMessage

	Correlation   string
	Duration      time.Duration
	Payload       string
	PayloadMethod string
}

// [Aws::EC2::Client 200 1.069542 0 retries] run_instances(...
var cpiAWSRPCOneRE = regexp.MustCompile(`^\[Aws::EC2::Client (\d+) ([\d\.]+) (\d) retries\] (.+)$`)

func (p cpiAWSRPCParser) Parse(inU log.Line) (log.Line, error) {
	in, ok := inU.(RawMessage)
	if !ok {
		return inU, nil
	}

	if in.Component != "ExternalCpiLog" {
		return inU, nil
	}

	if m := cpiAWSRPCOneRE.FindStringSubmatch(in.Message); len(m) > 0 {
		out := CPIAWSRPCMessage{
			RawMessage:  in,
			Correlation: in.Tags["req_id"],
			Payload:     m[4],
		}

		out.PayloadMethod = strings.SplitN(out.Payload, "(", 2)[0]

		if res, err := strconv.ParseFloat(m[2], 64); err == nil {
			out.Duration = time.Duration(int64(res * 1000000))
		}

		return out, nil
	}

	return inU, nil
}

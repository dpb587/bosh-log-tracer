package parser

import (
	"fmt"
	"regexp"
	"time"

	"github.com/dpb587/bosh-log-tracer/log"
	"github.com/dpb587/bosh-log-tracer/log/taskdebug"
)

var RawParser = rawParser{}

type rawParser struct{}

// I, [2019-06-19T01:44:52.546138 #26587] []  INFO -- DirectorJobRunner: ...
var rawOneRE = regexp.MustCompile(`^(\w), \[([^ ]+) #(\d+)\] \[([^\]]*)\]\s+(\w+) -- ([^:]+): (.+)$`)

// I, [2019-06-19T01:47:50.061354 #26935]  INFO -- [req_id cpi-354031]: ...
var rawTwoRE = regexp.MustCompile(`^(\w), \[([^ ]+) #(\d+)\]\s+(\w+) -- \[req_id cpi-(\d+)\]: (.+)$`)

func (p rawParser) Parse(inU log.Line) (log.Line, error) {
	in, ok := inU.(log.RawLine)
	if !ok {
		return inU, nil
	}

	out := taskdebug.RawMessage{
		RawLine: in,
		Message: in.RawLineData,
	}

	if m := rawOneRE.FindStringSubmatch(out.Message); len(m) > 0 {
		out.Process = m[3]
		out.Tags = p.parseTags(m[4])
		out.LogLevel = m[5]
		out.Component = m[6]
		out.Message = m[7]

		if t, err := time.Parse("2006-01-02T15:04:05", m[2]); err == nil {
			out.LogTime = t
		}
	} else if m := rawTwoRE.FindStringSubmatch(out.Message); len(m) > 0 {
		out.Process = m[3]
		out.Tags = map[string]string{"req_id": "cpi-" + m[5]}
		out.LogLevel = m[4]
		out.Component = "ExternalCpiLog"
		out.Message = m[6]

		if t, err := time.Parse("2006-01-02T15:04:05", m[2]); err == nil {
			out.LogTime = t
		}
	}

	return out, nil
}

// task:80528
var rawTagsOneRE = regexp.MustCompile(`^task:(\d+)$`)

// compile_package(legacy/e8d0a259ffde97201489d0b5f47822026cdfebf1, bosh-aws-xen-hvm-ubuntu-trusty-go_agent/3586.40)
var rawTagsTwoRE = regexp.MustCompile(`^compile_package\(([^/]+)/([^,]+), ([^/]+)/([^\)]+)\)$`)

// create_missing_vm(compilation-1b6dfd75-028e-469c-9512-bcce3b0a5504/6056c8c0-2bad-40cf-bcec-4c56f66f12de (0)/1)
var rawTagsThreeRE = regexp.MustCompile(`^create_missing_vm\(([^/]+)/([^ ]+) \((\d+)\)/(\d+)\)$`)

// canary_update(appsrv/a3cc41b0-e2f2-4722-89a8-b4d31a1e60d7 (0))
// instance_update(appsrv/ccd2905c-0e7e-45bd-9f9a-9455fad0fafc (0))
var rawTagsFourRE = regexp.MustCompile(`^(canary_update|instance_update)\(([^/]+)/([^ ]+) \((\d+)\)\)$`)

func (p rawParser) parseTags(raw string) map[string]string {
	tags := map[string]string{}

	if m := rawTagsOneRE.FindStringSubmatch(raw); len(m) > 0 {
		tags["task"] = m[1]
	} else if m := rawTagsTwoRE.FindStringSubmatch(raw); len(m) > 0 {
		tags["action"] = "compile_package"
		tags["package"] = fmt.Sprintf("%s/%s", m[1], m[2])
		tags["package_name"] = m[1]
		tags["package_fingerprint"] = m[2]
		tags["stemcell"] = fmt.Sprintf("%s/%s", m[3], m[4])
		tags["stemcell_os"] = m[3]
		tags["stemcell_version"] = m[4]
	} else if m := rawTagsThreeRE.FindStringSubmatch(raw); len(m) > 0 {
		tags["action"] = "create_missing_vm"
		tags["instance_group"] = m[1]
		tags["instance_id"] = m[2]
		tags["instance_index"] = m[3]
	} else if m := rawTagsFourRE.FindStringSubmatch(raw); len(m) > 0 {
		tags["action"] = m[1]
		tags["instance_group"] = m[2]
		tags["instance_id"] = m[3]
		tags["instance_index"] = m[4]
	}

	return tags
}

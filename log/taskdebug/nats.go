package taskdebug

import (
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/dpb587/bosh-log-tracer/log"
)

var NATSMessageParser = natsMessageParser{}

type natsMessageParser struct{}

type NATSMessageMessage struct {
	RawMessage

	Event   string
	Channel string
	Payload string
}

var _ log.Line = &NATSMessageMessage{}

func (m NATSMessageMessage) GetReceivedTaskID() string {
	var payload struct {
		Value struct {
			AgentTaskID string `json:"agent_task_id"`
		} `json:"value"`
	}

	err := json.Unmarshal([]byte(m.Payload), &payload)
	if err != nil {
		fmt.Println(m.Payload)
		panic(err)
	}

	return payload.Value.AgentTaskID
}

func (m NATSMessageMessage) GetReceivedState() string {
	var payload struct {
		Value struct {
			State string `json:"state"`
		} `json:"value"`
	}

	err := json.Unmarshal([]byte(m.Payload), &payload)
	if err != nil {
		var payload1 struct {
			Value interface{} `json:"value"`
		}

		err := json.Unmarshal([]byte(m.Payload), &payload1)
		if err != nil {
			panic(err)
		}

		// if it unserialized, assume it finished its task and this was the value
		return "done"
	}

	return payload.Value.State
}

func (m NATSMessageMessage) GetReceivedDrainValue() int64 {
	var payload struct {
		Value int64 `json:"value"`
	}

	err := json.Unmarshal([]byte(m.Payload), &payload)
	if err != nil {
		fmt.Println(m.Payload)
		panic(err)
	}

	return payload.Value
}

// SENT: agent.0e2a1093-0ace-4685-a361-a6f40a11f7ed {"protocol":3,"method":"get_state",...
var natsMessageOneRE = regexp.MustCompile(`^(SENT|RECEIVED): ([^ ]+) (.+)$`)

func (p natsMessageParser) Parse(inU log.Line) (log.Line, error) {
	in, ok := inU.(RawMessage)
	if !ok {
		return inU, nil
	}

	if in.Component != "DirectorJobRunner" {
		return inU, nil
	}

	if m := natsMessageOneRE.FindStringSubmatch(in.Message); len(m) > 0 {
		out := NATSMessageMessage{
			RawMessage: in,
			Event:      m[1],
			Channel:    m[2],
			Payload:    m[3],
		}

		return out, nil
	}

	return inU, nil
}

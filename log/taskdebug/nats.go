package taskdebug

import (
	"encoding/json"
	"fmt"

	"github.com/dpb587/bosh-log-tracer/log"
)

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

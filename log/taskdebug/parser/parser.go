package parser

import "github.com/dpb587/bosh-log-tracer/log"

var Parser = log.NewMultiParser(
	RawParser,

	ProcessParser,
	SequelParser,
	LockParser,
	InstanceAspectChangedParser,

	NATSMessageSentAgentParser,
	NATSMessageParser,

	ExternalCPIRequestParser,
	ExternalCPIParser,

	CPIAWSRPCParser,
)

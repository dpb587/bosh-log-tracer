package taskdebug

import "github.com/dpb587/bosh-log-tracer/log"

var Parser = []log.LineParser{
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
}

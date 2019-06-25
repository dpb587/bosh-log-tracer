package taskdebug

import "github.com/dpb587/boshdebugtracer/log"

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

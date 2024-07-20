package constants

const (
	FileOpenErrMessage    = "error during file opening: %v"
	RecordsReadErrMessage = "error during reading records: %v"
	UsersFile             = "users_million.csv"
	Separator             = '|'

	// main kafka
	KafkaBootstrapServers = "localhost:9092"
	KafkaTopic            = "oneMillionGO-v0.0.3"
	NumWorkers            = 3
	JSONFileName          = "resources/files/generated/users.json"
	AvroFileName          = "resources/files/generated/avro_users.json"
	BatchSize             = 100000

	// logs
	LOGFILE = "resources/files/logs/oneMillion.log"
	PREFIX  = "oneMillionKafkaApp: "
	WARNING = "WARNING: "
	INFO    = "INFO: "
	DEBUG   = "DEBUG: "
	ERROR   = "ERROR: "
	WTF     = "???"
)

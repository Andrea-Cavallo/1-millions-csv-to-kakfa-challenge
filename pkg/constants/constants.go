package constants

const (
	FileOpenErrMessage    = "error during file opening: %v"
	RecordsReadErrMessage = "error during reading records: %v"
	UsersFile             = "users_million.csv"
	Separator             = '|'

	// main kafka
	KafkaBootstrapServers = "localhost:9092"
	KafkaTopic            = "oneMillionGO"
	NumWorkers            = 3
	JSONFileName          = "users1.json"
	AvroFileName          = "avro_users1.json"
	BatchSize             = 100000

	// logs
	LOGFILE = "oneMillion.log"
	PREFIX  = "oneMillionKafkaApp: "
	WARNING = "WARNING: "
	INFO    = "INFO: "
	DEBUG   = "DEBUG: "
	ERROR   = "ERROR: "
	WTF     = "???"
)

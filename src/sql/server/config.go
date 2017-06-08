package server

type Config struct {
	Addr string

	StorageType string

	//redis config
	RedisAddr string

	//distkv config
	DistSysAddr  string
	DistUserAddr string
}

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/castermode/Nesoi/src/sql/server"
	"github.com/golang/glog"
)

var (
	shost = flag.String("shost", "0.0.0.0", "nesoi server host")
	sport = flag.String("sport", "3306", "nesoi server port")

	//storage type
	stype = flag.String("store_type", "Redis", "storage type")

	//redis flag
	rhost = flag.String("rhost", "0.0.0.0", "redis server host")
	rport = flag.String("rport", "6379", "redis server port")

	//distkv flag
	dshost = flag.String("dshost", "0.0.0.0", "distkv server sys host")
	dsport = flag.String("dsport", "6379", "distkv server sys port")
	duhost = flag.String("duhost", "0.0.0.0", "distkv server user host")
	duport = flag.String("duport", "6379", "distkv server user port")
)

func init() {
	flag.Parse()
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	cfg := &server.Config{
		Addr:         fmt.Sprintf("%s:%s", *shost, *sport),
		RedisAddr:    fmt.Sprintf("%s:%s", *rhost, *rport),
		StorageType:  *stype,
		DistSysAddr:  fmt.Sprintf("%s:%s", *dshost, *dsport),
		DistUserAddr: fmt.Sprintf("%s:%s", *duhost, *duport),
	}

	svr, err := server.NewServer(cfg)
	if err != nil {
		glog.Fatalf("Start server error: %s", err.Error())
		return
	}

	err = svr.RegisterDriver()
	if err != nil {
		glog.Fatalf("Register storage driver error: %s", err.Error())
		return
	}

	err = svr.InitNesoiDB()
	if err != nil {
		glog.Fatalf("Init nesoi database error: %s", err.Error())
		return
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sg := <-sc
		glog.Info("Received signal [", sg, "] to exit...")
		svr.Stop()
		glog.Flush()
		os.Exit(0)
	}()

	svr.Start()
}

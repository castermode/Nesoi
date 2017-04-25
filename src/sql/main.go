package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/castermode/Nesoi/src/sql/server"
)

var (
	host = flag.String("host", "0.0.0.0", "nesoi server host")
	port = flag.String("P", "3306", "nesoi server port")
)

func init() {
	flag.Parse()
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	cfg := &server.Config{
		Addr: fmt.Sprintf("%s:%s", *host, *port),
	}

	svr, err := server.NewServer(cfg)
	if err != nil {
		return
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		<-sc
		svr.Stop()
		os.Exit(0)
	}()

	svr.Start()
}

package main

import (
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/bisegni/sql-proxy/server"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log := xlog.NewStdLog(xlog.Level(xlog.DEBUG))

	// build := build.GetInfo()
	fmt.Printf("sql-proxy:[%+v]\n", "0.0.1")

	// config
	// flag.Usage = func() { usage() }
	// flag.Parse()
	// if flagConf == "" {
	// 	usage()
	// 	os.Exit(0)
	// }

	proxy := server.NewServer(log)
	proxy.Start()

	// Handle SIGINT and SIGTERM.
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	log.Info("sql-proxy.signal:%+v", <-ch)

	// Stop the proxy and httpserver.
	proxy.Stop()
}

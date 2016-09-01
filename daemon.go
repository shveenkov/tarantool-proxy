package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	"github.com/tarantool/go-tarantool"
	"time"

	"gopkg.in/yaml.v2"
	"io/ioutil"
	"runtime/pprof"

	"net/http"
	_ "net/http/pprof"
)

func tarantool_listen(netLoc string, listenNum int, tntPool [][]*tarantool.Connection, schema *Schema) {
	listener, err := net.Listen("tcp", netLoc)
	if err != nil {
		log.Fatalf("ERROR: Listen - %s", err)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatalf("ERROR: Accept - %s", err)
		}

		//run tarantool15 connection communicate
		go func() {
			//log.Printf("Accept ok\n")
			defer conn.Close()

			proxy := newProxyConnection(conn, listenNum, tntPool, schema)
			proxy.processIproto()
		}()
	}
}

func main() {
	var (
		config_file string
		cpuprofile  string
		memprofile  string
		netprofile  string
	)

	flag.StringVar(&cpuprofile, "cpuprofile", "", "write cpu profile to file")
	flag.StringVar(&memprofile, "memprofile", "", "write mem profile to file")
	flag.StringVar(&netprofile, "netprofile", "", "write cpu profile to http://host:port")
	flag.StringVar(&config_file, "config", "", "config file for tarantool proxy")
	flag.Parse()

	if config_file == "" {
		log.Fatalf("config file for tarantool proxy must be not empty")
	}

	if netprofile != "" {
		go http.ListenAndServe(netprofile, nil)
	}

	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	yamlData, err := ioutil.ReadFile(config_file)
	if err != nil {
		log.Fatalf("ERROR read yaml config: %s", err)
	}

	var ProxyConfig ProxyConfigStruct
	if err := yaml.Unmarshal(yamlData, &ProxyConfig); err != nil {
		log.Fatalf("ERROR parse yaml config %s: %s", config_file, err)
	}

	// check listen config
	if len(ProxyConfig.Listen15) == 0 {
		log.Fatalf("ERROR: config param 'listen' is required")
	}
	for _, netLoc := range ProxyConfig.Listen15 {
		netLocSplit := strings.SplitN(netLoc, ":", 2)
		if len(netLocSplit) < 2 {
			log.Fatalf("ERROR: incorrect param 'listen' value, use host:port")
		}
		_, err := strconv.Atoi(netLocSplit[1])
		if err != nil {
			log.Fatalf("ERROR: incorrect param 'listen' value, use host:port")
		}
	}

	// check tarantool config
	if len(ProxyConfig.Pass16) == 0 {
		log.Fatalf("ERROR: config param tarantool is required")
	}
	for _, shard := range ProxyConfig.Pass16 {
		if len(shard) == 0 {
			log.Fatalf("ERROR: bad config param tarantool value, use list ['host:port', ...]")
		}
		for _, netLoc := range shard {
			netLocSplit := strings.SplitN(netLoc, ":", 2)
			if len(netLocSplit) < 2 {
				log.Fatalf("ERROR: bad config param tarantool value, use list ['host:port', ...]")
			}
			_, err := strconv.Atoi(netLocSplit[1])
			if err != nil {
				log.Fatalf("ERROR: bad config param tarantool value, use list ['host:port', ...]")
			}
		}
	}

	if !ProxyConfig.Sharding && len(ProxyConfig.Pass16) != len(ProxyConfig.Listen15) {
		log.Fatalf("ERROR: incorrect config count nodes for tarantool and listen param, use the same count or set sharding_enabled=true")
	}

	signal_chan := make(chan os.Signal, 1)
	signal.Notify(signal_chan, os.Interrupt, syscall.SIGTERM)

	log.Printf("tarantool-proxy v%s (built w/%s)\n", Version, runtime.Version())
	if ProxyConfig.Sharding {
		log.Printf("sharding enabled\n")
	} else {
		log.Printf("sharding disabled\n")
	}

	var tntConnectionPool [][]*tarantool.Connection
	tntOpts := tarantool.Opts{
		Timeout:       5 * time.Second,
		Reconnect:     1 * time.Second,
		MaxReconnects: 0, // endlessly
		User:          ProxyConfig.User,
		Pass:          ProxyConfig.Password,
	}

	for _, shard := range ProxyConfig.Pass16 {
		// make several connections for CPU usage
		tntShard := make([]*tarantool.Connection, Tnt16PoolSize*len(shard))

		for _, netLoc := range shard {
			log.Printf("connect_16 %s\n", netLoc)
		}

		for i := 0; i < Tnt16PoolSize; i += 1 {
			for j := 0; j < len(shard); j += 1 {
				tnt16, err := tarantool.Connect(shard[j], tntOpts)
				if err != nil {
					log.Fatalf("Failed to connect: %s", err.Error())
				}
				tntShard[i*len(shard)+j] = tnt16
			} //end for
		} //end for

		tntConnectionPool = append(tntConnectionPool, tntShard)
	} //end for

	// close connections for tntConnectionPool
	defer func() {
		for _, shard := range tntConnectionPool {
			for _, tnt := range shard {
				tnt.Close()
			}
		}
	}()

	for listenNum, netLoc := range ProxyConfig.Listen15 {
		log.Printf("listen_15 %s\n", netLoc)
		go tarantool_listen(netLoc, listenNum, tntConnectionPool, NewSchema(&ProxyConfig))
	}

	wait_sig := <-signal_chan
	log.Printf("Caught signal %v... shutting down\n", wait_sig)

	if memprofile != "" {
		f, err := os.Create(memprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
	} //end if
}

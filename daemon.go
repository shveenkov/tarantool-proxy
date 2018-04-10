package main

import (
	"flag"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/quipo/statsd"
	"github.com/tarantool/go-tarantool"
	"gopkg.in/yaml.v2"

	"net/http"
	_ "net/http/pprof"
	"runtime/pprof"
)

func tarantoolListen(netLoc string, listenNum int, tntPool [][]*tarantool.Connection, schema *Schema, statsdClient statsd.Statsd) {
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
			defer conn.Close()

			proxy := newProxyConnection(conn, listenNum, tntPool, schema, statsdClient)
			proxy.processIproto()
		}()
	}
}

func createStatsdClient(addr, prefix string) (client statsd.Statsd) {
	if addr == "" {
		client = statsd.NoopClient{}
		return
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	} else {
		hostname = strings.SplitN(hostname, ".", 2)[0]
	}

	hostPrefix := strings.Replace(prefix, "%HOST%", hostname, 1)
	if hostPrefix != "" && !strings.HasSuffix(hostPrefix, ".") {
		hostPrefix = hostPrefix + "."
	}

	statsdSock := statsd.NewStatsdClient(addr, hostPrefix)
	err = statsdSock.CreateSocket()
	if err != nil {
		log.Printf("error create statsd socket: %s\n", err)
		client = statsd.NoopClient{}
		return
	}
	statsd.UDPPayloadSize = 8 * 1024
	interval := time.Second * 2 // aggregate stats and flush every 2 seconds
	client = statsd.NewStatsdBuffer(interval, statsdSock)

	log.Printf("enable send metrics to statsd: %s; %s\n", addr, prefix)

	return
}

func main() {
	var (
		configFile string
		cpuprofile string
		memprofile string
		netprofile string
		logFile    string
		logFD      *os.File
	)

	flag.StringVar(&cpuprofile, "cpuprofile", "", "write cpu profile to file")
	flag.StringVar(&memprofile, "memprofile", "", "write mem profile to file")
	flag.StringVar(&netprofile, "netprofile", "", "write cpu profile to http://host:port")
	flag.StringVar(&configFile, "config", "", "config file for tarantool proxy")
	flag.StringVar(&logFile, "log", "", "log file for tarantool proxy")
	flag.Parse()

	if logFile != "" {
		f, err := os.OpenFile(logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0664)
		if err != nil {
			log.Fatalf("error open log file: %v\n", err)
		}
		logFD = f
		log.SetOutput(logFD)
	}

	if configFile == "" {
		log.Fatalf("config file for tarantool proxy must be not empty")
	}

	if netprofile != "" {
		log.Printf("netprofile enabled %s\n", netprofile)

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

	yamlData, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Fatalf("ERROR read yaml config: %s", err)
	}

	var ProxyConfig ProxyConfigStruct
	if err := yaml.Unmarshal(yamlData, &ProxyConfig); err != nil {
		log.Fatalf("ERROR parse yaml config %s: %s", configFile, err)
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

	log.Printf("tarantool-proxy v%s (built w/%s)\n", Version, runtime.Version())
	if ProxyConfig.Sharding {
		log.Printf("sharding enabled\n")
	} else {
		log.Printf("sharding disabled\n")
	}

	var tntConnectionPool [][]*tarantool.Connection
	tntOpts := tarantool.Opts{
		Timeout:       7 * time.Second,
		Reconnect:     2 * time.Second,
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

		for i := 0; i < Tnt16PoolSize; i++ {
			for j := 0; j < len(shard); j++ {
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

	statsdClient := createStatsdClient(ProxyConfig.Statsd.Server, ProxyConfig.Statsd.Prefix)
	defer statsdClient.Close()

	for listenNum, netLoc := range ProxyConfig.Listen15 {
		log.Printf("listen_15 %s\n", netLoc)
		go tarantoolListen(netLoc, listenNum, tntConnectionPool, NewSchema(&ProxyConfig), statsdClient)
	}

	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

WAIT_SIGNAL:
	for {
		waitSig := <-chSignal
		switch waitSig {
		case syscall.SIGHUP:
			log.Printf("Caught signal %v... log reopen\n", waitSig)
			if logFD == nil {
				continue WAIT_SIGNAL
			}
			// log reopen
			f, err := os.OpenFile(logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0664)
			if err != nil {
				log.Printf("error open log file: %v\n", err)
			}
			log.SetOutput(f)
			logFD.Close()
			logFD = f
		default:
			log.Printf("Caught signal %v... shutting down\n", waitSig)
			break WAIT_SIGNAL
		} //end switch
	} //end for

	if logFD != nil {
		logFD.Close()
	}

	if memprofile != "" {
		f, err := os.Create(memprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
	} //end if
}

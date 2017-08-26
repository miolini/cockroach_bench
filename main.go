package main

import (
	"fmt"
	"flag"
	"log"
	"database/sql"
	"strings"
	"net"
	"sync"
	"time"
	"sync/atomic"
	
	_ "github.com/lib/pq"
)

func main() {
	flConc := flag.Int("c", 4, "concurrency level")
	flNum := flag.Int("n", 10000, "number")
	flAddrs := flag.String("addrs", "localhost:26257", "addrs list host:port")
	flDbUser := flag.String("dbuser", "root", "db user")
	flDbName := flag.String("dbname", "testdb", "test db name")
	flBatchSize := flag.Int("b", 1, "batch size")
	flReportInterval := flag.Int("ri", 5, "report interval in secs")
	flag.Parse()
	log.Printf("c=%d, n=%d, b=%d", *flConc, *flNum, *flBatchSize)
	log.Printf("dbuser=%s, dbname=%s, addrs=%s", *flDbUser, *flDbName, *flAddrs)
	addrs := strings.Split(*flAddrs, ",")
	var wg sync.WaitGroup
	wg.Add(*flConc * len(addrs))
	var qps int64
	query := fmt.Sprintf("INSERT INTO test (value) VALUES %s", strings.TrimRight(strings.Repeat("(0),", *flBatchSize), ","))
	log.Printf("query: %s", query)
	batchSize := int64(*flBatchSize)
	go func() {
		var lastQps int64
		var currentQps int64
		sleep := int64(*flReportInterval)
		sleepDuration := time.Second * time.Duration(sleep)
		var i int
		t := time.Now()
		for {
			realSleep := t.Add(sleepDuration * time.Duration(i)).Sub(time.Now())
			time.Sleep(realSleep)
			i += 1
			currentQps = atomic.LoadInt64(&qps)
			log.Printf("qps %d/%d, rps %d/%d", (currentQps - lastQps) / sleep, currentQps, (currentQps - lastQps) / sleep * batchSize, currentQps * batchSize)
			lastQps = currentQps
		}
	}()
	for i, addr := range addrs {
		host, port, err := net.SplitHostPort(addr)
		checkErr(err)
		connectUrl := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable", host, port, *flDbUser, *flDbName)
		log.Printf("connect url: %s", connectUrl)
		if i == 0 {
			log.Printf("init db")
			initDb(connectUrl)
		}
		log.Printf("run workers for: %s", addr)
		for i:=0;i<*flConc;i++ {
			go newConn(connectUrl, query, *flNum, *flBatchSize, &qps, &wg)
		}
	}
	wg.Wait()
}

func initDb(connectUrl string) {
	db, err := sql.Open("postgres", connectUrl)
	checkErr(err)
	defer db.Close()
	_, err = db.Exec("DROP TABLE IF EXISTS test")
	checkErr(err)
	_, err = db.Exec("CREATE TABLE test (id INT PRIMARY KEY DEFAULT unique_rowid(), value INT)")
	checkErr(err)
}

func newConn(connectUrl string, query string, num int, batchSize int, qps *int64, wg *sync.WaitGroup) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("%s err: %s", connectUrl, err)
		}
	}()
	defer wg.Done()
	db, err := sql.Open("postgres", connectUrl)
	checkErr(err)
	defer db.Close()
	for i:=0; i<num; i++ {
		_, err = db.Exec(query)
		checkErr(err)
		atomic.AddInt64(qps, 1)
	}
}

func checkErr(err interface{}) {
	if err != nil {
		panic(err)
	}
}
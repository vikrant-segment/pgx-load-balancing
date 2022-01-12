package main

import (
	"fmt"
	"net"
	"sync"
	"time"

	"gopkg.in/jackc/pgx.v2"
)

type config struct {
	PostgresAddr             string
	PostgresReadReplicaAddr  string
	PostgresMaxConnections   uint
	PostgresStatementTimeout time.Duration
}

type Actor struct {
	ActorId   uint
	FirstName string
	LastName  string
	LastMod   time.Time
}

var wg sync.WaitGroup

func main() {
	cfg := config{
		PostgresAddr:             "postgres://postgres:Awspostgres1@schema-test-primary.cmc59mvfggh6.us-east-2.rds.amazonaws.com:5432/segment",
		PostgresReadReplicaAddr:  "postgres://postgres:Awspostgres1@3.21.56.60:5432/segment",
		PostgresMaxConnections:   0,
		PostgresStatementTimeout: 0,
	}

	for i := 0; i < 1000; i++ {
		go process(cfg)
		time.Sleep(1 * time.Second)

	}

	wg.Wait()
}

func process(cfg config) {
	wg.Add(1)
	connConfig, err := pgx.ParseURI(cfg.PostgresAddr)
	if err != nil {
		fmt.Println(fmt.Errorf("failed to parse postgresql connection string %v", err))
	}
	connConfig.Dial = func(network, addr string) (net.Conn, error) {
		return net.DialTimeout(network, addr, time.Minute)
	}
	fmt.Println("creating postgres connection pool...")
	dbx, err := pgx.NewConnPool(pgx.ConnPoolConfig{
		ConnConfig:     connConfig,
		MaxConnections: int(cfg.PostgresMaxConnections),
		AfterConnect: func(conn *pgx.Conn) error {
			_, queryErr := conn.Exec(fmt.Sprintf("SET statement_timeout=%d", int64(cfg.PostgresStatementTimeout.Seconds()*1000)))
			return queryErr
		},
	})
	if err != nil {
		fmt.Println(fmt.Errorf("failed to connect to postgresql %v", err))
	}

	connConfigReplica, err := pgx.ParseURI(cfg.PostgresReadReplicaAddr)
	if err != nil {
		fmt.Println(fmt.Errorf("failed to parse postgresql read replica connection string %v", err))
	}
	connConfigReplica.Dial = func(network, addr string) (net.Conn, error) {
		return net.DialTimeout(network, addr, time.Minute)
	}
	fmt.Println("creating postgres read replica connection pool...")
	dbxReplica, err := pgx.NewConnPool(pgx.ConnPoolConfig{
		ConnConfig:     connConfigReplica,
		MaxConnections: int(cfg.PostgresMaxConnections),
		AfterConnect: func(conn *pgx.Conn) error {
			_, queryErr := conn.Exec(fmt.Sprintf("SET statement_timeout=%d", int64(cfg.PostgresStatementTimeout.Seconds()*1000)))
			return queryErr
		},
	})
	if err != nil {
		fmt.Println(fmt.Errorf("failed to connect to postgresql read replica %v", err))
	}

	fmt.Println(dbx.Stat())

	rows, err := dbxReplica.Query("select * from actor")
	if err != nil {
		fmt.Println("error", err)
	} else {
		for rows.Next() {
			var p Actor
			err = rows.Scan(&p.ActorId, &p.FirstName, &p.LastName, &p.LastMod)
			fmt.Println(p)
		}

	}
	time.Sleep(2 * time.Minute)
	wg.Done()
}

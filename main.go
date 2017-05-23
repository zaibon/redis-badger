package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/zaibon/badger/badger"
	"github.com/zaibon/resp"
)

type BadgerKV struct {
	kv *badger.KV
}

func NewKV(dir string) *BadgerKV {
	opts := badger.DefaultOptions
	opts.Dir = dir
	kv := badger.NewKV(&opts)

	return &BadgerKV{
		kv: kv,
	}
}
func (b *BadgerKV) Close() {
	b.kv.Close()
}

func (b *BadgerKV) Get(conn *resp.Conn, args []resp.Value) bool {
	if len(args) != 2 {
		conn.WriteError(errors.New("ERR wrong number of arguments for 'get' command"))
	} else {
		key := args[1].Bytes()
		val, _ := b.kv.Get(key)
		if val == nil {
			conn.WriteNull()
		} else {
			conn.WriteBytes(val)
		}
	}
	return true
}

func (b *BadgerKV) Set(conn *resp.Conn, args []resp.Value) bool {
	if len(args) != 3 {
		conn.WriteError(errors.New("ERR wrong number of arguments for 'set' command"))
	} else {
		key := args[1].Bytes()
		val := args[2].Bytes()
		b.kv.Set(key, val)
		conn.WriteSimpleString("OK")
	}
	return true
}

func (b *BadgerKV) Ping(conn *resp.Conn, args []resp.Value) bool {
	if len(args) == 1 {
		conn.WriteSimpleString("PONG")
	} else if len(args) == 2 {
		conn.WriteSimpleString(fmt.Sprintf("PONG %s", args[1].String()))
	} else {
		conn.WriteError(errors.New("ERR wrong number of arguments for 'ping' command"))
	}
	return true
}

var addr = flag.String("addr", ":16379", "listening address")
var dir = flag.String("dir", "db", "directory for kvs storage")

func main() {
	flag.Parse()

	kv := NewKV(*dir)

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT)

	defer func() {
		log.Println("closing kvs")
		kv.Close()
		os.Exit(0)
	}()

	s := resp.NewServer()

	s.HandleFunc("set", kv.Set)
	s.HandleFunc("get", kv.Get)
	s.HandleFunc("ping", kv.Ping)

	go func() {
		<-c
		log.Println("closing kvs")
		kv.Close()
		os.Exit(0)
	}()

	log.Printf("server listenting on %v\n", *addr)
	if err := s.ListenAndServe(*addr); err != nil {
		log.Fatal(err)
	}
}

// Copyright 2022 Peter Bigot Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-test/deep"
	"github.com/gomodule/redigo/redis"
	"github.com/kr/pretty"
	"gopkg.in/yaml.v2"

	lw "github.com/pabigot/logwrap"
	svcInflux "github.com/pabigot/svcutil/influxdb"
	svcInfluxCfg "github.com/pabigot/svcutil/influxdb/config"
	svcRedis "github.com/pabigot/svcutil/redis"
)

// logMaker constructs a log.Logger and increases the timestamp precision to
// milliseconds.
func logMaker(owner interface{}) lw.Logger {
	logger := lw.LogLogMaker(owner)
	switch owner.(type) {
	case redis.Conn:
		logger.SetPriority(lw.Info)
	case *svcRedis.KeyMonitorBase:
		logger.SetPriority(lw.Info)
	default:
		logger.SetPriority(lw.Debug)
	}
	lgr := logger.(*lw.LogLogger).Instance()
	lgr.SetFlags(lgr.Flags() | log.Lmicroseconds)
	return logger
}

func getMapFromBytes(raw []byte, legacy bool) (svcInfluxCfg.BucketSchemaMap, error) {
	var err error
	var csm svcInfluxCfg.BucketSchemaMap
	if raw[0] == '{' {
		err = json.Unmarshal(raw, &csm)
	} else {
		err = yaml.Unmarshal(raw, &csm)
	}
	if err != nil {
		csm = nil
	}
	return csm, nil
}

func getMapFromFile(inFile string, legacy bool) (svcInfluxCfg.BucketSchemaMap, error) {
	raw, err := os.ReadFile(inFile)
	if err != nil {
		panic(fmt.Sprintf("read: %s: %s", inFile, err.Error()))
	}

	return getMapFromBytes(raw, legacy)
}

func doMonitor(p svcRedis.Pool) {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		s := <-sigc
		fmt.Printf("signal: %s, exiting\n", s)
		cancel()
	}()

	key := svcInflux.SCHEMA_REDIS_KEY
	pschan := svcInflux.SCHEMA_REDIS_PSCHAN
	fmt.Printf("Monitor %s on %s\n", key, pschan)
	kmb := svcRedis.NewKeyMonitorBase(ctx, "psMon", p, logMaker)
	_, err := kmb.Subscribe(pschan)
	if err != nil {
		panic(err)
	}
	m, err := kmb.NewMonitor(key, 1)
	if err != nil {
		panic(err)
	}

	var csm svcInfluxCfg.BucketSchemaMap
	for raw := range m.Updates() {
		if raw == nil {
			fmt.Println("No initial value in Redis")
			continue
		}
		var ncsm svcInfluxCfg.BucketSchemaMap
		if rba, ok := raw.([]byte); ok {
			err = yaml.Unmarshal(rba, &ncsm)
		} else {
			panic(fmt.Sprintf("Unexpected response %T: %v", raw, raw))
		}
		if err != nil {
			panic(err)
		}
		if true || csm == nil {
			fmt.Printf("Base: %# v\n", pretty.Formatter(ncsm))
		} else {
			diff := deep.Equal(csm, ncsm)
			fmt.Printf("New: %d differences:\n", len(diff))
			for _, d := range diff {
				fmt.Println(d)
			}
		}
		csm = ncsm
	}
}

func main() {
	var inFile string
	var baseFile string
	var inBase, fetch, legacy, monitor, update, write, display bool
	flag.StringVar(&inFile, "in", "/home/pab/Consulting/Notes/influx.yaml", "file read to populate database")
	flag.BoolVar(&inBase, "in-base", false, "use base as input")
	flag.StringVar(&baseFile, "base", "", "file used as basis for comparison")
	flag.BoolVar(&fetch, "fetch", false, "fetch base from redis")
	flag.BoolVar(&monitor, "monitor", false, "monitor for redis changes")
	flag.BoolVar(&update, "update", false, "update redis with result")
	flag.BoolVar(&write, "write", false, "whether object should be written")
	flag.BoolVar(&display, "display", false, "whether object should be displayed")
	flag.Parse()

	var err error
	var c redis.Conn
	var p svcRedis.Pool
	if fetch || monitor || update {
		cfg := svcRedis.Config{
			ID:  "sensors",
			URL: "redis://redis.pab",
		}
		if err := cfg.Validate(); err != nil {
			panic(err)
		}
		p, err = cfg.NewPool()
		if err != nil {
			panic(err)
		}
		c = p.Get()
		if err := c.Err(); err != nil {
			panic(err)
		}
		defer c.Close()
	}

	if monitor {
		doMonitor(p)
		return
	}

	var base svcInfluxCfg.BucketSchemaMap
	if fetch {
		var raw []byte
		key := svcInflux.SCHEMA_REDIS_KEY
		fmt.Printf("fetch base from Redis %s\n", key)
		raw, err = redis.Bytes(c.Do("GET", key))
		if err != nil {
			panic(err)
		}
		base, err = getMapFromBytes(raw, legacy)
		if err != nil {
			panic(fmt.Sprintf("fetch: %s", err.Error()))
		}
		fmt.Printf("fetch raw: %s\n", string(raw))
	} else if baseFile != "" {
		fmt.Printf("fetch base from %s\n", baseFile)
		base, err = getMapFromFile(baseFile, legacy)
		if err != nil {
			panic(fmt.Sprintf("read: %s: %s", baseFile, err.Error()))
		}
	} else if inBase && inFile != "" {
		fmt.Printf("fetch base from %s\n", inFile)
		base, err = getMapFromFile(inFile, legacy)
		if err != nil {
			panic(fmt.Sprintf("read: %s: %s", inFile, err.Error()))
		}
	}
	if false {
		fmt.Printf("base: %# v\n", pretty.Formatter(base))
		if raw, err := yaml.Marshal(base); err != nil {
			panic(err)
		} else {
			fmt.Printf("# base YAML\n%s\n", raw)
		}
		if raw, err := json.MarshalIndent(base, "", "  "); err != nil {
			panic(err)
		} else {
			fmt.Printf("# base JSON\n%s\n", raw)
		}
	}

	var obj svcInfluxCfg.BucketSchemaMap
	if inBase {
		obj = base
	} else {
		fmt.Printf("inFile: %s\n", inFile)
		raw, err := os.ReadFile(inFile)
		if err != nil {
			panic(fmt.Sprintf("read: %s: %s", inFile, err.Error()))
		}
		obj, err = getMapFromBytes(raw, legacy)
	}
	if false {
		fmt.Printf("obj: %# v\n", pretty.Formatter(obj))
		if raw, err := yaml.Marshal(obj); err != nil {
			panic(err)
		} else {
			fmt.Printf("# obj YAML\n%s\n", raw)
		}
		if raw, err := json.MarshalIndent(obj, "", "  "); err != nil {
			panic(err)
		} else {
			fmt.Printf("# obj JSON\n%s\n", raw)
		}
	}

	fmt.Printf("base %t obj %t\n", base != nil, obj != nil)
	if base != nil {
		diff := deep.Equal(obj, base)
		fmt.Printf("%d differences:\n", len(diff))
		for _, d := range diff {
			fmt.Println(d)
		}
	}

	_, err = svcInflux.BucketSchemaMapFromConfig(obj)
	if err != nil {
		panic(err.Error())
	}

	if display {
		fmt.Printf("content:\n% #v\n", pretty.Formatter(obj))
	}
	if write {
		var raw []byte
		if raw, err = yaml.Marshal(obj); err == nil {
			err = os.WriteFile("out.yaml", raw, 0644)
		}
		if err != nil {
			fmt.Printf("write yaml: %s\n", err.Error())
		}
		if raw, err = json.Marshal(obj); err == nil {
			err = os.WriteFile("out.json", raw, 0644)
		}
		if err != nil {
			fmt.Printf("write json: %s\n", err.Error())
		}
	}
	if update {
		key := svcInflux.SCHEMA_REDIS_KEY
		pschan := svcInflux.SCHEMA_REDIS_PSCHAN
		fmt.Printf("Update main %s via %s", key, pschan)

		raw, err := json.Marshal(obj)
		if err != nil {
			panic(err)
		}
		v, err := c.Do("SET", key, raw)
		if err == nil {
			v, err = c.Do("PUBLISH", pschan, key)
		}
		fmt.Printf("res %v %v\n", v, err)
	}
}

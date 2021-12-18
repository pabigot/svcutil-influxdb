// Copyright 2021-2022 Peter Bigot Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/pabigot/svcutil/influxdb"
	"github.com/pabigot/svcutil/influxdb/config"

	"github.com/influxdata/influxdb-client-go/v2" // influxdb2
	http2 "github.com/influxdata/influxdb-client-go/v2/api/http"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	logI "github.com/influxdata/influxdb-client-go/v2/log"
	lp "github.com/influxdata/line-protocol" // protocol

	lw "github.com/pabigot/logwrap"
)

const (
	cfgEnvKey = "INFLUX_CONF"
)

type options struct {
	archivePath    string
	mainPriority   lw.Priority
	connPriority   lw.Priority
	bucketPriority lw.Priority
	genInterval    time.Duration
}

var appOpt options
var bucketName string

// logMaker constructs a log.Logger and increases the timestamp precision to
// milliseconds.
func logMaker(inst interface{}) lw.Logger {
	logger := lw.LogLogMaker(inst)
	switch inst.(type) {
	case *influxdb.Connection:
		logger.SetPriority(appOpt.connPriority)
	case *influxdb.Bucket:
		logger.SetPriority(appOpt.bucketPriority)
	default:
		logger.SetPriority(appOpt.mainPriority)
	}
	lgr := logger.(*lw.LogLogger).Instance()
	lgr.SetFlags(lgr.Flags() | log.Lmicroseconds)
	return logger
}

func parseConfig() (config.Connection, *influxdb.Connection, error) {
	var cfg config.Connection
	path, ok := os.LookupEnv(cfgEnvKey)
	if !ok {
		return cfg, nil, fmt.Errorf("locating config: %s: Not in environment", cfgEnvKey)
	}

	f, err := os.Open(path)
	if err != nil {
		return cfg, nil, fmt.Errorf("loading config: %v", err)
	}
	defer f.Close()

	raw, err := ioutil.ReadAll(f)
	if err != nil {
		return cfg, nil, fmt.Errorf("loading config: %v", err)
	}

	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return cfg, nil, fmt.Errorf("parsing config: %v", err)
	}

	// Pick the first bucket as the destination for the metrics.
	for k := range cfg.Buckets {
		bucketName = k
		break
	}

	opts := influxdb2.DefaultOptions().
		SetLogLevel(logI.WarningLevel)

	defer func() {
		y2, _ := yaml.Marshal(cfg)
		fmt.Println(string(y2))
	}()

	conn, err := influxdb.NewConnection(&cfg, opts, logMaker)
	return cfg, conn, err
}

func main() {
	opt := &appOpt
	desc := "where to store unwritable metrics"
	flag.StringVar(&opt.archivePath, "archive-path", "", desc)
	flag.StringVar(&opt.archivePath, "A", "", desc+" (short)")
	desc = "log priority for main"
	opt.mainPriority = lw.Info
	flag.Var(&opt.mainPriority, "main-priority", desc)
	flag.Var(&opt.mainPriority, "M", desc+" (short)")
	desc = "log priority for connection"
	opt.connPriority = lw.Warning
	flag.Var(&opt.connPriority, "conn-priority", desc)
	flag.Var(&opt.connPriority, "C", desc+" (short)")
	desc = "log priority for bucket"
	opt.bucketPriority = lw.Warning
	flag.Var(&opt.bucketPriority, "bucket-priority", desc)
	flag.Var(&opt.bucketPriority, "B", desc+" (short)")
	desc = "interval between metric generation"
	flag.DurationVar(&opt.genInterval, "gen-interval", time.Second, desc)
	flag.DurationVar(&opt.genInterval, "I", time.Second, desc+" (short)")
	flag.Parse()

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)

	lgr := logMaker(nil)
	lgr.SetId("main")
	lprn := lw.MakePriWrapper(lgr, lw.Notice)
	lpri := lw.MakePriWrapper(lgr, lw.Info)
	lprd := lw.MakePriWrapper(lgr, lw.Debug)

	cfg, conn, err := parseConfig()
	if err != nil {
		panic(err)
	}
	if lgr.Priority() <= lw.Info {
		y, _ := yaml.Marshal(cfg)
		lpri("config:\n%s", string(y))
	}
	idb := conn.Client()
	defer idb.Close()

	b := conn.FindBucket(bucketName)
	if b == nil {
		panic("no bucket")
	}

	var tmrC <-chan time.Time
	tmr := time.NewTimer(0)
	if !tmr.Stop() {
		<-tmr.C
	}

	// The only way to find out success/failure is a WriteSettledCallback,
	// which isn't allowed to block.  However, it must preserve order of
	// completion.  So have it send the results to the application select
	// loop for processing, through a channel with sufficient capacity to
	// ensure it won't block the bucket.  Have the helper drop the batch;
	// the application can resubmit it if necessary.
	type wscResult struct {
		batch       *influxdb.Batch
		err         error
		bucketState influxdb.BucketState
	}
	wscCh := make(chan *wscResult, 1) // TODO what's the right limit here?
	b.SetWriteSettledCallback(func(batch influxdb.Batch, err error, bs influxdb.BucketState) influxdb.WriteRecovery {
		// go func(res *wscResult) {
		// 	wscCh <- res
		// }(&wscResult{
		// 	batch:       &batch,
		// 	err:         err,
		// 	bucketState: bs,
		// })
		wscCh <- &wscResult{
			batch:       &batch,
			err:         err,
			bucketState: bs,
		}
		return influxdb.WriteRecoveryDrop
	})

	var failed []*influxdb.Batch
	var wrtTotal influxdb.Cardinality
	id := fmt.Sprintf("HACK/influx.go:%sZ", time.Now().UTC().Format("2006-01-02T15:04:05"))
	var cs influxdb.ConnectionState
	var bs influxdb.BucketState
	stch := conn.RequestStateChan(1)

	var archivePath string
	if len(appOpt.archivePath) > 0 {
		prec := idb.Options().Precision()
		archivePath = path.Join(appOpt.archivePath,
			fmt.Sprintf("archive.lp%s", prec.String()))

		var batch *influxdb.Batch
		dat, err := os.ReadFile(archivePath)
		if err == nil {
			lprn("%s: read %d bytes unwritten data",
				archivePath, len(dat))
			batch, err = influxdb.NewBatchFromLineProtocol(string(dat), prec)
		}
		if err == nil {
			nr := b.State().BatchRuneCapacity
			batches := batch.Partition(nr, nr)
			lprn("%s: %d batches max %d runes",
				archivePath, len(batches), nr)
			failed = make([]*influxdb.Batch, len(batches))
			for i, batch := range batches {
				lpri("%d: %s", i, batch.Cardinality())
				failed[i] = &batch
			}
		}
		if err == nil {
			err = os.Remove(archivePath)
			lpri("remove %s: %v", archivePath, err)
		} else {
			lprn("%s: failed load: %s",
				archivePath, err.Error())
		}
	}

	ok := true
	running := true
	ctr := 0
	for ok || running {
		enable := false
		disable := false
		flushBatch := false
		err = nil
		lprd("loop top")
		select {
		case cs, ok = <-stch:
			// Terminate on ErrConnShutDown, rather than waiting
			// for the uninitialized state that comes when the
			// channel is closed.
			ok = ok && !errors.Is(err, influxdb.ErrConnShutDown)
			if ok {
				lpri("State: %s", cs)
			} else {
				lpri("Shutdown: %s", cs)
				stch = nil
			}
			disable = tmrC != nil && !cs.Ready()
			enable = tmrC == nil && cs.Ready()
		case s := <-sigc:
			lprn("shutdown on signal: %s", s)
			b.FlushBatch()
			conn.Shutdown(false)
		case wscr := <-wscCh:
			err = wscr.err
			bs = wscr.bucketState
			lprd("WSC: %t %v %s", running, err, bs)
			// && compensates for out-of-order delivery where the
			// terminated state arrives before other states.  That
			// shouldn't happen.
			running = running && !bs.Terminated
			if !running {
				lprn("TERMINATED: %s, %s", wscr.batch.Cardinality(), bs)
			}
			if err == nil {
				wrtTotal.Add(wscr.batch.Cardinality())
				lpri("WSC: wrote %s, total %s, %s", wscr.batch.Cardinality(), wrtTotal, bs)
				break
			}

			lprn("WSC: write %s failed: %s", wscr.batch.Cardinality(), err.Error())
			retain := true
			disable = true
			temporary := true

			// The errors.As branches assume that upstream issue
			// #293 has been resolved and the blocking API returns
			// the actual error, not just its text.
			var merr *lp.MetricError
			var nerr net.Error
			var herr *http2.Error
			// Errors received may be:
			// * net.Error instances
			// * api.http.Error instances wrapped by fmt.Errorf()
			//   - 401 if not authorized
			//   - 404 if bucket not found
			//   - 422 if some points were outside retention policy
			// * from github.com/influxdata/line-protocol
			//   - lp.MetricError of various flavors
			//
			// See
			// https://github.com/influxdata/influxdb-client-go/issues/292
			// and #293 for more information about trying to
			// decode errors from the v2 client.
			if errors.As(err, &merr) {
				if errors.Is(merr, lp.ErrInvalidName) {
					lpri("WSC: measurement name missing")
				} else {
					lpri("WSC: MetricError: %s", merr.Error())
				}
			} else if errors.As(err, &nerr) {
				lpri("WSC: net.Error (perm=%t): %s", !nerr.Temporary(), nerr.Error())
				temporary = nerr.Temporary()
			} else if errors.As(err, &herr) {
				temporary = false
				switch herr.StatusCode {
				case 401: // handle unauthorized
					fallthrough
				case 404: // handle not found
					fallthrough
				case 422: //handle partial write
					retain = false
					temporary = true
					disable = false
					fallthrough
				default:
					lpri("WSC: http2.Error: %d: %s\n", herr.StatusCode, herr.Error())
				}
			} else if errors.Is(err, influxdb.ErrBucketTerminated) {
				temporary = false
			} else {
				lpri("WSC: Unhandled %T: %s", err, err.Error())
			}

			if retain {
				lprn("WSC: added %s to %d failed", wscr.batch.Cardinality(),
					len(failed))
				failed = append(failed, wscr.batch)
			}

			// ECONNREFUSED can come from a net error or an http2
			// error.  Go considers it a permanent error.  We
			// don't, because a nice way to play with this example
			// is to shut the server down and see the retry
			// behavior.
			if strings.Contains(err.Error(), "connection refused") {
				temporary = true
			}
			if temporary {
				lpri("WSC: stopping generation due to recoverable error")
			} else {
				lprn("WSC: exiting due to unrecoverable error")
				conn.Shutdown(false)
			}

		case <-tmrC:
			lprd("tmr fired")
			tmr.Reset(appOpt.genInterval)
			ctr++
			np := write.NewPoint(
				"counter",
				map[string]string{
					"id": id,
				},
				map[string]interface{}{
					"counter": ctr,
				},
				time.Now())
			batch, rej := b.MakeBatch(np)
			if rej != nil {
				// Code and schema are inconsistent: that's a
				// bug in the example.
				panic("rejected metric")
			}
			bs, err = b.QueueBatch(batch)
			lprd("QB: added %s", bs)
			if errors.Is(err, influxdb.ErrBacklogOverflow) {
				lprn("QB: stopping generation due to backlog overflow")
				failed = append(failed, &batch)
				disable = true
			} else if err != nil {
				lprn("QB: Unhandled %T: %s", err, err.Error())
			}
		}
		if enable {
			tmrC = tmr.C
			tmr.Reset(0)
			lpri("enabled generation")
		}
		if lf := len(failed); lf > 0 && cs.Ready() {
			rc := bs.AvailableRuneCapacity() - bs.HeldRuneCapacity/2
			bp := failed[0]
			for bp != nil && (rc-bp.NumRunes()) >= 0 {
				bs, err = b.QueueBatch(*bp)
				if err != nil {
					lprn("FB: unable to queue %s: %s", bp.Cardinality(), err.Error())
					break
				}
				lpri("FB: re-queued %s", bp.Cardinality())
				lf--
				copy(failed[:lf], failed[1:])
				failed = failed[:lf]
				bp = nil
				if lf > 0 {
					bp = failed[0]
				}
			}
			flushBatch = true
		}
		if flushBatch {
			lprd("FB flushed")
			b.FlushBatch()
		}
		if disable {
			if tmrC != nil && !tmr.Stop() {
				<-tmr.C
			}
			tmrC = nil
			lpri("disabled generation")
		}
	}
	lprn("exited loop, %d failed", len(failed))
	if len(failed) > 0 && archivePath != "" {
		f, err := os.OpenFile(archivePath, os.O_CREATE|os.O_WRONLY, 0640)
		if err != nil {
			lprn("failed to open %s: %s", archivePath, err.Error())
		} else {
			defer f.Close()
			for i, batch := range failed {
				_, err := f.WriteString(batch.LPData())
				if err == nil {
					lpri("%d: %s", i, batch.Cardinality())
				} else {
					lprn("failed to write %s: %s", archivePath, err.Error())

					break
				}
			}
		}
	}
	lprn("Final state: %s", b.State())

	lprn("done")
}

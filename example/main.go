// Copyright 2021-2022 Peter Bigot Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
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

	svcInflux "github.com/pabigot/svcutil/influxdb"
	svcInfluxCfg "github.com/pabigot/svcutil/influxdb/config"

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
	case *svcInflux.Connection:
		logger.SetPriority(appOpt.connPriority)
	case *svcInflux.Bucket:
		logger.SetPriority(appOpt.bucketPriority)
	default:
		logger.SetPriority(appOpt.mainPriority)
	}
	lgr := logger.(*lw.LogLogger).Instance()
	lgr.SetFlags(lgr.Flags() | log.Lmicroseconds)
	return logger
}

func parseConfig() (svcInfluxCfg.Connection, *svcInflux.Connection, error) {
	var cfg svcInfluxCfg.Connection
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

	conn, err := svcInflux.NewConnection(&cfg, opts, logMaker)
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
	opt.connPriority = lw.Info
	flag.Var(&opt.connPriority, "conn-priority", desc)
	flag.Var(&opt.connPriority, "C", desc+" (short)")
	desc = "log priority for bucket"
	opt.bucketPriority = lw.Info
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
	lpr := lw.MakePriPr(lgr)

	cfg, conn, err := parseConfig()
	if err != nil {
		panic(err)
	}
	if lgr.Priority() <= lw.Info {
		y, _ := yaml.Marshal(cfg)
		lpr.I("config:\n%s", string(y))
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
		batch       *svcInflux.Batch
		err         error
		bucketState svcInflux.BucketState
	}
	wscCh := make(chan *wscResult, 1) // TODO what's the right limit here?
	b.SetWriteSettledCallback(func(batch svcInflux.Batch, err error, bs svcInflux.BucketState) {
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
	})

	var failed []*svcInflux.Batch
	var wrtTotal svcInflux.Cardinality
	id := fmt.Sprintf("HACK/influx.go:%sZ", time.Now().UTC().Format("2006-01-02T15:04:05"))
	var cs svcInflux.ConnectionState
	var bs svcInflux.BucketState
	stch := conn.RequestStateChan(1)

	var archivePath string
	if len(appOpt.archivePath) > 0 {
		prec := idb.Options().Precision()
		archivePath = path.Join(appOpt.archivePath,
			fmt.Sprintf("archive.lp%s", prec.String()))

		var batch *svcInflux.Batch
		dat, err := os.ReadFile(archivePath)
		if err == nil {
			lpr.N("%s: read %d bytes unwritten data",
				archivePath, len(dat))
			batch, err = svcInflux.NewBatchFromLineProtocol(string(dat), prec)
		}
		if err == nil {
			nr := b.State().BatchRuneCapacity
			batches := batch.Partition(nr, nr)
			lpr.N("%s: %d batches max %d runes",
				archivePath, len(batches), nr)
			failed = make([]*svcInflux.Batch, len(batches))
			for i, batch := range batches {
				lpr.I("%d: %s", i, batch.Cardinality())
				failed[i] = &batch
			}
		}
		if err == nil {
			err = os.Remove(archivePath)
			lpr.I("remove %s: %v", archivePath, err)
		} else {
			lpr.N("%s: failed load: %s",
				archivePath, err.Error())
		}
	}

	var sdCtx context.Context
	var sdCtxCancel context.CancelFunc
	var sdChan <-chan svcInflux.BucketState

	loop := sdChan == nil
	ctr := 0
	for loop {
		var ok bool
		enable := false
		disable := false
		flushBatch := false
		doShutdown := false
		err = nil
		lpr.D("loop top")
		select {
		case cs, ok = <-stch:
			if !ok {
				lpr.I("connection state channel closed")
				stch = nil
				break
			}
			lpr.I("State: %s", cs)
			disable = tmrC != nil && !cs.Ready()
			enable = tmrC == nil && cs.Ready()
		case s := <-sigc:
			lpr.N("shutdown on signal: %s", s)
			doShutdown = true
		case bs, ok := <-sdChan:
			if !ok {
				dl, _ := sdCtx.Deadline()
				lpr.N("shutdown complete at %s before deadline", time.Until(dl))
				sdChan = nil
				loop = false
				break
			}
			lpr.N("shutdown proceeding: %s", bs)
		case wscr := <-wscCh:
			err = wscr.err
			bs = wscr.bucketState
			lpr.D("WSC: %v %s", err, bs)
			if err == nil {
				wrtTotal.Add(wscr.batch.Cardinality())
				lpr.I("WSC: wrote %s, total %s, %s", wscr.batch.Cardinality(), wrtTotal, bs)
				break
			}

			lpr.N("WSC: write %s failed: %s", wscr.batch.Cardinality(), err.Error())
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
					lpr.I("WSC: measurement name missing")
				} else {
					lpr.I("WSC: MetricError: %s", merr.Error())
				}
			} else if errors.As(err, &nerr) {
				lpr.I("WSC: net.Error (perm=%t): %s", !nerr.Temporary(), nerr.Error())
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
					lpr.I("WSC: http2.Error: %d: %s\n", herr.StatusCode, herr.Error())
				}
			} else if errors.Is(err, svcInflux.ErrBucketTerminated) {
				temporary = false
			} else {
				lpr.I("WSC: Unhandled %T: %s", err, err.Error())
			}

			if batch := wscr.batch; retain && !batch.Empty() {
				lpr.N("WSC: added %s to %d failed", batch.Cardinality(),
					len(failed))
				failed = append(failed, batch)
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
				lpr.I("WSC: stopping generation due to recoverable error")
			} else if sdCtx == nil {
				lpr.N("WSC: exiting due to unrecoverable error")
				doShutdown = true
			}

		case <-tmrC:
			lpr.D("tmr fired")
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
			lpr.D("QB: added %s", bs)
			if errors.Is(err, svcInflux.ErrBacklogOverflow) ||
				errors.Is(err, svcInflux.ErrBucketTerminated) {
				lpr.N("QB: stopping generation: %s", err.Error())
				failed = append(failed, &batch)
				disable = true
			} else if err != nil {
				lpr.N("QB: Unhandled %T: %s", err, err.Error())
			}
		}
		if enable {
			tmrC = tmr.C
			tmr.Reset(0)
			lpr.I("enabled generation")
		}
		if lf := len(failed); lf > 0 && cs.Ready() {
			rc := bs.AvailableRuneCapacity() - bs.HeldRuneCapacity/2
			bp := failed[0]
			for bp != nil && (rc-bp.NumRunes()) >= 0 {
				bs, err = b.QueueBatch(*bp)
				if err != nil {
					lpr.N("FB: unable to queue %s: %s", bp.Cardinality(), err.Error())
					break
				}
				lpr.I("FB: re-queued %s", bp.Cardinality())
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
			lpr.D("FB flushed")
			b.FlushBatch()
		}
		if doShutdown {
			now := time.Now()
			if sdCtx == nil {
				sdCtx, sdCtxCancel = context.WithDeadline(context.Background(),
					now.Add(5*time.Second))
				sdChan, err = conn.Shutdown(sdCtx)
				if err != nil {
					panic(fmt.Sprintf("shutdown failed: %s", err.Error()))
				}
				dl, _ := sdCtx.Deadline()
				lpr.N("shutdown: initiated, deadline in %s", time.Until(dl))
			} else {
				dl, _ := sdCtx.Deadline()
				sdCtxCancel()
				lpr.N("shutdown: triggered immediate with %s left", time.Until(dl))
			}
		}
		if disable {
			if tmrC != nil && !tmr.Stop() {
				<-tmr.C
			}
			tmrC = nil
			lpr.I("disabled generation")
		}
	}
	lpr.N("exited loop, %d failed", len(failed))
	if len(failed) > 0 && archivePath != "" {
		f, err := os.OpenFile(archivePath, os.O_CREATE|os.O_WRONLY, 0640)
		if err != nil {
			lpr.N("failed to open %s: %s", archivePath, err.Error())
		} else {
			defer f.Close()
			for i, batch := range failed {
				_, err := f.WriteString(batch.LPData())
				if err == nil {
					lpr.I("%d: %s", i, batch.Cardinality())
				} else {
					lpr.N("failed to write %s: %s", archivePath, err.Error())

					break
				}
			}
		}
	}
	lpr.N("Final state: %s", b.State())

	lpr.N("done")
}

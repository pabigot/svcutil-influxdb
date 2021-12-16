// Copyright 2021-2022 Peter Bigot Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package influxdb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"reflect"
	//	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gopkg.in/yaml.v2"

	//	"github.com/kr/pretty"

	"github.com/influxdata/influxdb-client-go/v2" // influxdb2
	"github.com/influxdata/influxdb-client-go/v2/api"
	http2 "github.com/influxdata/influxdb-client-go/v2/api/http"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/influxdata/influxdb-client-go/v2/domain"
	lp "github.com/influxdata/line-protocol"

	"github.com/pabigot/done"
	"github.com/pabigot/edcode"
	lw "github.com/pabigot/logwrap"
	"github.com/pabigot/set"

	svcInfluxCfg "github.com/pabigot/svcutil/influxdb/config"
)

// Run standard verification of expected errors, i.e. that err is an
// error and its text contains errstr.
func confirmError(t *testing.T, err error, base error, errstr string) {
	t.Helper()
	if err == nil {
		t.Fatalf("succeed, expected error %s", errstr)
	}
	if base != nil && !errors.Is(err, base) {
		t.Fatalf("err not from %s: %T: %s", base, err, err)
	}
	if testing.Verbose() {
		t.Logf("Error=`%v`", err.Error())
	}
	if !strings.Contains(err.Error(), errstr) {
		t.Fatalf("failed, missing %s: %v", errstr, err)
	}
}

var unterminatedBatch = Batch{
	lpData:     "a",
	numMetrics: 1,
	prec:       time.Nanosecond,
}

func checkBNT(t *testing.T) {
	r := recover()
	if r == nil {
		t.Errorf("Missing panic")
	}
	t.Logf("Caught panic: %v", r)
	confirmError(t, r.(error), ErrBatchNotTerminated, "missing newline terminator")
	err := r.(*ErrorBatch)
	if err.Batch != &unterminatedBatch {
		t.Errorf("Panic does not include expected batch")
	}
}

func debugLogMaker(inst interface{}) lw.Logger {
	logger := lw.LogLogMaker(inst)
	logger.SetPriority(lw.Debug)
	lgr := logger.(*lw.LogLogger).Instance()
	lgr.SetFlags(lgr.Flags() | log.Lmicroseconds)
	return logger
}

func cancelledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

// Some tests aren't complete until a bucket or connection has shut down.
// Since conn.Shutdown() can legitimately be invoked only once, we use a
// wrapper.
func completeShutdown(t *testing.T, conn *Connection, wait bool) {
	t.Helper()
	t.Logf("shutdown %t", wait)
	sdch, err := conn.Shutdown(cancelledContext())
	if err != nil {
		t.Logf("shutdown: %s", err.Error())
	} else {
		for bs := range sdch {
			t.Logf("shutdown: %s", bs)
		}
		t.Logf("shutdown: complete")
	}
}

func TestBucketTerminated(t *testing.T) {
	parent := fmt.Errorf("%w: bucket name here", ErrBucketTerminated)
	sub := fmt.Errorf("test error")
	err := &errBucketTerminated{
		parent: parent,
		err:    sub,
	}
	confirmError(t, err, ErrBucketTerminated, "bucket terminated: bucket name here: test error")
	if errors.Unwrap(err) != sub {
		t.Fatal("not Unwrap")
	}
}

func TestCardinality(t *testing.T) {
	var c Cardinality

	c.Add(Cardinality{Metrics: 3, Runes: 4})
	if c.Metrics != 3 || c.Runes != 4 {
		t.Errorf("add failed: %v", c)
	}

	if v := c.String(); v != "4 runes (3 metrics)" {
		t.Errorf("String() failed: %s", v)
	}

	c2 := c
	if !c.Equal(c2) {
		t.Error("identity equal failed")
	}
	c.Sub(Cardinality{Metrics: 1, Runes: 2})
	if c.Metrics != 2 || c.Runes != 2 {
		t.Errorf("sub failed: %v", c)
	}
	if c.Equal(c2) {
		t.Error("diff equal failed")
	}

	c.Reset()
	if c.Metrics != 0 || c.Runes != 0 {
		t.Errorf("reset failed: %v", c)
	}
}

func TestBucketState(t *testing.T) {
	bs := BucketState{
		HeldRuneCapacity: 150,
		HeldSize:         Cardinality{3, 60},
	}
	if v := bs.AvailableRuneCapacity(); v != 90 {
		t.Errorf("available %d", v)
	}
	if v := bs.HoldLevel(); v != 60./150. {
		t.Errorf("hold level %f", v)
	}
}

func lp2tx(lpData string) string {
	return strings.ReplaceAll(lpData, "\n", "$")
}

func lps2tx(lpData []string) string {
	var rv []string
	for _, ld := range lpData {
		rv = append(rv, lp2tx(ld))
	}
	return strings.Join(rv, ":")
}

func TestBatchFromLineProtocol(t *testing.T) {
	type test struct {
		in  string
		npt int
	}
	tests := []test{
		{
			in:  "a\nb\n",
			npt: 2,
		},
		{
			in:  "a\nb\n\n",
			npt: 2,
		},
		{
			in:  "\na\nb",
			npt: 2,
		},
		{
			in:  "\n\na\n\n\nb",
			npt: 2,
		},
	}

	for _, tc := range tests {
		batch, _ := NewBatchFromLineProtocol(tc.in, time.Nanosecond)
		ins := lp2tx(tc.in)
		if batch.Empty() {
			t.Errorf("%s: empty batch", ins)
		}
		lpData := batch.lpData
		if enl := lpData[len(lpData)-1] == '\n'; !enl {
			t.Errorf("%s: batch does not end in newline", ins)
		}
		if npt := batch.NumMetrics(); npt != tc.npt {
			t.Errorf("%s: npt %d exp %d", ins, npt, tc.npt)
		}
	}
	batch, _ := NewBatchFromLineProtocol("\n\n", time.Nanosecond)
	if lpd := batch.lpData; lpd != "" {
		t.Errorf("empty lpData not detected")
	}
	if !batch.Empty() {
		t.Errorf("empty batch not detected")
	}

	_, err := NewBatchFromLineProtocol("1", 2*time.Millisecond)
	confirmError(t, err, ErrInvalidPrecision, "invalid precision: 2ms must be 1s, 1ms, 1us, or 1ns")
}

func TestBatchPartition(t *testing.T) {
	type test struct {
		in                string
		maxFirst, maxRest int
		out               []string
		npt               int
	}
	tests := []test{
		{
			in: "",
		},
		{
			in: "\n",
		},
		{
			in: "\n\n",
		},
		{
			in:  "a",
			out: []string{"a\n"},
			npt: 1,
		},
		{
			in:  "a",
			out: []string{"a\n"},
			npt: 1,
		},
		{
			in:       "a",
			out:      []string{"", "a\n"},
			maxFirst: -1,
			npt:      1,
		},
		{
			in:  "too long",
			out: []string{"", "too long\n"},
			npt: 1,
		},
		{
			in:  "\ntoo long",
			out: []string{"", "too long\n"},
			npt: 1,
		},
		{
			in:  "too long\n",
			out: []string{"", "too long\n"},
			npt: 1,
		},
		{
			in:  "1\n23\ntoo long",
			out: []string{"1\n", "23\n", "too long\n"},
			npt: 3,
		},
		{
			in:  "\na",
			out: []string{"a\n"},
			npt: 1,
		},
		{
			in:  "a\n",
			out: []string{"a\n"},
			npt: 1,
		},
		{
			in:  "a\n\n\nb", // test trimming trailing newlines from chunk
			out: []string{"a\n", "b\n"},
			npt: 2,
		},
		{
			in:  "a\nb\nc\n",
			out: []string{"a\nb\n", "c\n"},
			npt: 3,
		},
		{
			in:       "a\nb\nc\n",
			out:      []string{"a\nb\n", "c\n"},
			maxFirst: 5,
			npt:      3,
		},
		{
			in:  "a\nb\nfour\nsomething long\n",
			out: []string{"a\nb\n", "four\n", "something long\n"},
			npt: 4,
		},
		{
			in:       "\n\n1\n12\n123\n1234\n12345\n",
			out:      []string{"1\n12\n", "123\n", "1234\n", "12345\n"},
			maxFirst: 6,
			npt:      5,
		},
		{
			in:  "12\n1234",
			out: []string{"12\n", "1234\n"},
			npt: 2,
		},
		{
			in:       "1\n2\n1234",
			maxFirst: 2,
			maxRest:  7,
			out:      []string{"1\n", "2\n1234\n"},
			npt:      3,
		},
		{
			in:       "1\n2\n1234",
			maxFirst: 2,
			maxRest:  5,
			out:      []string{"1\n", "2\n", "1234\n"},
			npt:      3,
		},
		{
			in:       "1\n2\n1234",
			maxFirst: 4,
			maxRest:  5,
			out:      []string{"1\n2\n", "1234\n"},
			npt:      3,
		},
		{
			in:       "1\n2\n3\n4\n5\n",
			maxFirst: 6,
			maxRest:  2,
			out:      []string{"1\n2\n3\n", "4\n", "5\n"},
			npt:      5,
		},
	}

	for ti, tc := range tests {
		batch, _ := NewBatchFromLineProtocol(tc.in, time.Nanosecond)
		if v := batch.NumMetrics(); v != tc.npt {
			t.Errorf("point count difference: %d != %d", v, tc.npt)
		}
		if tc.maxFirst == 0 && tc.maxRest == 0 {
			tc.maxFirst = 4
		}
		if tc.out == nil {
			tc.out = []string{}
		}
		batches := batch.Partition(tc.maxFirst, tc.maxRest)
		chunks := []string{}
		for _, b := range batches {
			chunks = append(chunks, b.lpData)
		}
		if !reflect.DeepEqual(chunks, tc.out) {
			t.Errorf("%d: %s [%d, %d]: '%s' not '%s'", ti,
				lp2tx(tc.in), tc.maxFirst, tc.maxRest,
				lps2tx(chunks), lps2tx(tc.out))
		}
	}

	defer checkBNT(t)
	unterminatedBatch.Partition(0, 0)
}

func dupFields(m map[string]interface{}) map[string]interface{} {
	rv := make(map[string]interface{})
	for k, v := range m {
		rv[k] = v
	}
	return rv
}

func TestMetricValidate(t *testing.T) {
	sch := NewSchema(&svcInfluxCfg.Schema{
		Name:         "meas",
		RequiredTags: []string{"t1", "t2"},
		OptionalTags: []string{"t3"},
		Fields: map[string]svcInfluxCfg.FieldType{
			"d":  svcInfluxCfg.Float,
			"b":  svcInfluxCfg.Boolean,
			"i":  svcInfluxCfg.Integer,
			"ic": svcInfluxCfg.Integer,
			"u":  svcInfluxCfg.UInteger,
			"uc": svcInfluxCfg.UInteger,
		},
	})
	goodTags := map[string]string{
		"t1": "v1",
		"t2": "v2",
		"t3": "v3",
	}
	goodFields := map[string]interface{}{
		"d":  1.2,
		"b":  true,
		"i":  int32(-1),
		"ic": -2.0,
		"u":  uint8(1),
		"uc": 2.0,
	}
	ts := time.Unix(12345, 0)

	err := sch.ValidateMetric(nil)
	confirmError(t, err, ErrMetricNil, "")

	gf := dupFields(goodFields)
	pt, _ := lp.New("notmeas", goodTags, gf, ts)
	err = sch.ValidateMetric(pt)
	confirmError(t, err, ErrMetricMeasurement, "notmeas not meas")

	bt := make(map[string]string)
	gf = dupFields(goodFields)
	pt, _ = lp.New(sch.Name(), bt, gf, ts)
	err = sch.ValidateMetric(pt)
	confirmError(t, err, ErrMetricTagsMissing, ": t1 t2")
	for k, v := range goodTags {
		if k != "t3" {
			bt[k] = v
		}
	}
	bt["ut"] = "vut"
	pt, _ = lp.New(sch.Name(), bt, gf, ts)
	err = sch.ValidateMetric(pt)
	confirmError(t, err, ErrMetricTagsUnknown, ": ut")
	delete(bt, "ut")
	bt["t3"] = goodTags["t3"]
	pt, _ = lp.New(sch.Name(), bt, gf, ts)
	if err = sch.ValidateMetric(pt); err != nil {
		t.Errorf("unexpected failure: %v", err)
	}

	gf = dupFields(goodFields)
	// NewPoint will convert numeric values to 64-bit, but won't convert
	// floats to integers.
	pt, _ = lp.New(sch.Name(), goodTags, gf, ts)
	for _, f := range pt.FieldList() {
		switch f.Key {
		case "ic":
			if _, ok := f.Value.(float64); !ok {
				t.Errorf("ic value not float64: %T", f.Value)
			}
		case "uc":
			if _, ok := f.Value.(float64); !ok {
				t.Errorf("uc value not float64: %T", f.Value)
			}
		}
	}

	err = sch.ValidateMetric(pt)
	if err != nil {
		t.Fatal(err)
	}
	upd := sch.NormalizeMetric(pt)
	if l := len(upd); l != 2 {
		t.Fatalf("wrong updates: %v", upd)
	}
	for _, f := range upd {
		switch f.Key {
		case "d":
			if _, ok := f.Value.(float64); !ok {
				t.Errorf("d value not float64: %T", f.Value)
			}
		case "b":
			if _, ok := f.Value.(bool); !ok {
				t.Errorf("b value not bool: %T", f.Value)
			}
		case "i":
			if _, ok := f.Value.(int64); !ok {
				t.Errorf("i value not int32: %T", f.Value)
			}
		case "ic":
			if _, ok := f.Value.(int64); !ok {
				t.Errorf("ic value not int64: %T", f.Value)
			}
		case "u":
			if _, ok := f.Value.(uint64); !ok {
				t.Errorf("u value not uint8: %T", f.Value)
			}
		case "uc":
			if _, ok := f.Value.(uint64); !ok {
				t.Errorf("uc value not uint64: %T", f.Value)
			}
		default:
			t.Errorf("unexpected: %s", f)
		}
	}
}

func TestDatabaseMaps(t *testing.T) {
	conn_yaml := `
id: TestDatabaseMaps
url: http://host.domain:8086
organization: my-org
token: my-token
buckets:
  buck1:
    organization: org1
    measurements:
      meas1:
        required-tags: [id]
      meas2:
        required-tags: [chn]
  buck2:
      measurements:
        m1:
        m2:
        m3:
`
	var cfg *svcInfluxCfg.Connection
	err := yaml.Unmarshal([]byte(conn_yaml), &cfg)
	if err != nil {
		t.Fatal(err)
	}

	conn, err := NewConnection(cfg, nil, debugLogMaker)
	if err != nil {
		t.Fatal(err)
	}
	defer completeShutdown(t, conn, true)

	buckets := conn.Buckets()
	if v := len(buckets); v != 2 {
		t.Errorf("wrong size: %d", v)
	}
	if b := buckets["buck1"]; b == nil {
		t.Errorf("unable to find buck1")
	} else if b2 := conn.FindBucket(b.Name()); b != b2 {
		t.Errorf("buck1 lookup wrong: %v", b2)
	}
	b2 := buckets["buck2"]
	if b2 == nil {
		t.Errorf("unable to find buck2")
	} else if b := conn.FindBucket(b2.Name()); b != b2 {
		t.Errorf("buck2 lookup wrong: %v", b2)
	}

	schemas := b2.Schemas()
	if v := len(schemas); v != 3 {
		t.Errorf("wrong size: %d", v)
	}
	if s := b2.FindSchema("m1"); s == nil {
		t.Errorf("unable to find m1")
	} else if s2 := schemas[s.Name()]; s != s2 {
		t.Errorf("m1 lookup wrong: %v", s2)
	}
}

func TestBucketSchemaMap(t *testing.T) {
	cfg := svcInfluxCfg.BucketSchemaMap{
		"": nil,
	}
	_, err := BucketSchemaMapFromConfig(cfg)
	confirmError(t, err, ErrBucketSchemaMapKey, "empty bucket name")

	delete(cfg, "")
	b1 := map[string]*svcInfluxCfg.Schema{
		"": {
			RequiredTags: []string{"tb1s1"},
		},
		"s2": {
			RequiredTags: []string{"tb1s2"},
		},
	}
	cfg["b1"] = b1
	_, err = BucketSchemaMapFromConfig(cfg)
	confirmError(t, err, ErrBucketSchemaMapSchema, "b1: empty schema name")

	b1["s1"] = b1[""]
	delete(b1, "")

	bsm, err := BucketSchemaMapFromConfig(cfg)
	if err != nil {
		t.Fatal(err.Error())
	}

	b := bsm["b1"]
	if b == nil {
		t.Fatal("no b1")
	}

	if b["s1"] == nil || b["s2"] == nil {
		t.Fatal("wrong schema")
	}
}

func TestConnectionImplStructure(t *testing.T) {
	cf := mockMakeClient(returnMockClient(nil))
	defer mockMakeClient(cf)

	var cfg *svcInfluxCfg.Connection
	conn, err := NewConnection(nil, nil, nil)
	if conn != nil {
		t.Fatal("nil connection found")
	}
	confirmError(t, err, svcInfluxCfg.ErrConnection, ": nil")

	conn_yaml := `
id: TestConnectionImplStructure
url: http://host.domain:8086
organization: my-org
token: my-token
buckets:
  buck1:
    organization: org1
    measurements:
      meas1:
        required-tags: [id]
        fields:
          if1: integer
          if2: integer
          uf1: uinteger
      meas2:
        required-tags: []
        optional-tags: [ot1]
`
	err = yaml.Unmarshal([]byte(conn_yaml), &cfg)
	if err != nil {
		t.Fatal(err)
	}

	var connNL *Connection
	var buck1NL *Bucket
	var connLog, buckLog lw.Logger
	newLog := func(inst interface{}) lw.Logger {
		logger := lw.LogLogMaker(inst)
		switch inst := inst.(type) {
		case *Connection:
			if inst.Id() != cfg.Id {
				t.Errorf("newLog given unconfigured Connection: %v", inst)
			}
			connNL = inst
			connLog = logger
		case *Bucket:
			if inst.Name() != "buck1" {
				t.Errorf("newLog given unconfigured Bucket: %v", inst)
			}
			buck1NL = inst
			buckLog = logger
		}
		return logger
	}

	conn, err = NewConnection(cfg, nil, newLog)
	if err != nil {
		t.Fatal(err)
	}
	if conn != connNL {
		t.Errorf("newLog not given correct conn instance: %v", connNL)
	}
	defer completeShutdown(t, conn, true)

	if v := conn.Id(); v != "TestConnectionImplStructure" {
		t.Errorf("Id incorrect: %s", v)
	}
	if p := conn.LogPriority(); p != lw.Warning {
		t.Errorf("Log priority not Warning: %v", p)
	}
	if p := connLog.Priority(); p != lw.Warning {
		t.Errorf("Underlying log priority not Warning: %v", p)
	}
	conn.LogSetPriority(lw.Info)
	if p := conn.LogPriority(); p != lw.Info {
		t.Errorf("Log priority not Warning: %v", p)
	}
	if p := connLog.Priority(); p != lw.Info {
		t.Errorf("Underlying log priority not Info: %v", p)
	}

	stch := conn.RequestStateChan(3)
	if c := cap(stch); c != 3 {
		t.Errorf("StateChan cap not 3: %d", c)
	}

	cli := conn.Client()
	if cli == nil {
		t.Error("cli not set")
	}
	if _, ok := cli.(*mockClient); !ok {
		t.Error("cli not mocked")
	}

	b := conn.FindBucket("nsb")
	if b != nil {
		t.Errorf("found nsb %v", b)
	}

	b = conn.FindBucket("buck1")
	if b == nil {
		t.Fatalf("find failed")
	}
	if n := b.Name(); n != "buck1" {
		t.Errorf("%s != buck1", n)
	}
	if o := b.Organization(); o != "org1" {
		t.Errorf("%s != org1", o)
	}
	if b != buck1NL {
		t.Errorf("newLog not given correct b1 instance: %v", b)
	}

	b2 := conn.FindBucket(b.Name())
	if !(b2 == b) {
		t.Errorf("FindBucket inconsistent")
	}
	if p := b2.LogPriority(); p != lw.Warning {
		t.Errorf("Log priority not Warning: %v", p)
	}
	if p := buckLog.Priority(); p != lw.Warning {
		t.Errorf("Underlying log priority not Warning: %v", p)
	}
	b2.LogSetPriority(lw.Info)
	if p := b2.LogPriority(); p != lw.Info {
		t.Errorf("Log priority not Warning: %v", p)
	}
	if p := buckLog.Priority(); p != lw.Info {
		t.Errorf("Underlying log priority not Info: %v", p)
	}

	s := b.FindSchema("nsm")
	if s != nil {
		t.Error("unknown Schema found")
	}

	s = b.FindSchema("meas1")
	if s == nil {
		t.Fatal("failed schema")

	}
	if n := s.Name(); n != "meas1" {
		t.Errorf("%s != meas1", n)
	}
	if !(s.reqTags.Has("id") && len(s.reqTags) == 1) {
		t.Errorf("reqTags not populated correctly")
	}
	if !(s.intFields.Has("if1") && s.intFields.Has("if2") && len(s.intFields) == 2) {
		t.Errorf("intFields not populated correctly")
	}
	if !(s.uintFields.Has("uf1") && len(s.uintFields) == 1) {
		t.Errorf("uintFields not populated correctly")
	}

	s = b.FindSchema("meas2")
	if s == nil {
		t.Fatal("failed schema")

	}
	if n := s.Name(); n != "meas2" {
		t.Errorf("%s != meas2", n)
	}
	if s.reqTags != nil {
		t.Errorf("reqTags not nil")
	}
	if s.optTags != nil {
		t.Errorf("optTags not nil")
	}

	conn.ReleaseStateChan(stch)
}

type netError struct{}

func (e netError) Error() string {
	return "netError"
}
func (e netError) Temporary() bool {
	return false
}
func (e netError) Timeout() bool {
	return false
}

func TestConnectionStateImpl(t *testing.T) {
	var cs ConnectionState
	if cs.Ready() {
		t.Error("uninit should not be ready")
	}
	if s := cs.String(); s != "not-ready: uninitialized" {
		t.Errorf("uninit string wrong: %s", s)
	}

	var rdy domain.Ready
	var rdyRes = &rdy
	var rdyErr error
	mc := mockClient{
		ready: func(ctx context.Context) (*domain.Ready, error) {
			return rdyRes, rdyErr
		},
	}
	cf := mockMakeClient(returnMockClient(&mc))
	defer mockMakeClient(cf)

	cfg := &svcInfluxCfg.Connection{
		Id: "TestConnectionStateImpl",
	}
	conn, err := NewConnection(cfg, nil, debugLogMaker)
	if err != nil {
		t.Fatal(err)
	}
	defer completeShutdown(t, conn, true)

	stch := conn.RequestStateChan(0)
	if c := cap(stch); c != 1 {
		t.Errorf("StateChan cap not 1: %d", c)
	}
	cs = <-stch
	confirmError(t, cs.Err, ErrConnNotReady, "not ready")

	st := domain.ReadyStatusReady
	rdyRes.Status = &st
	conn.CheckConnection()
	cs = <-stch
	if !cs.Ready() {
		t.Error(cs)
	}
	if !cs.AutoReconnect {
		t.Errorf("not auto-reconnect: %s", cs)
	}

	rdyRes = nil
	nerr := netError{}
	_ = error(nerr).(net.Error)
	rdyErr = fmt.Errorf("%w: wrapped", &nerr)
	conn.CheckConnection()
	cs = <-stch
	confirmError(t, cs.Err, nil, "netError")
	if cs.Err != error(&nerr) {
		t.Error("netError not unwrapped")
	}

	herr := http2.Error{
		StatusCode: 401,
	}
	rdyErr = fmt.Errorf("%w: wrapped", &herr)
	conn.CheckConnection()
	cs = <-stch
	if cs.Err != error(&herr) {
		t.Errorf("http2.Error not unwrapped: %s", cs.Err)
	}

	rdyErr = ErrMetricNil
	conn.CheckConnection()
	cs = <-stch
	if cs.Err != rdyErr {
		t.Errorf("unrecognized error not forwarded")
	}

	rdyErr = nil
	rdyRes = &rdy
	conn.CheckConnection()
	cs = <-stch
	if !cs.Ready() {
		t.Error(cs)
	}
	if !cs.AutoReconnect {
		t.Errorf("not auto-reconnect: %s", cs)
	}

	conn.StopReconnection()
	cs = conn.State()
	if v := cs.String(); v != "ready (reconnect disabled)" {
		t.Errorf("bad string: %s", v)
	}
	if cs.Err != nil {
		t.Errorf("existing conn broken: %s", cs.Err)
	}
	if !cs.Ready() {
		t.Error(cs)
	}
	if cs.AutoReconnect {
		t.Errorf("still auto-reconnect: %s", cs)
	}

	// initiate check without re-enabling AutoReconnect
	conn.checkConnection(false)
	cs = <-stch
	if !cs.Ready() {
		t.Error(cs)
	}
	if cs.AutoReconnect {
		t.Errorf("not auto-reconnect: %s", cs)
	}

	rdyRes = nil
	rdyErr = ErrMetricNil
	conn.checkConnection(false)
	cs = <-stch
	if cs.Ready() {
		t.Error(cs)
	}
	confirmError(t, cs.Err, ErrConnRetriesDisabled, "retries disabled")
	confirmError(t, cs.Err, ErrMetricNil, "metric is nil")
	if err := errors.Unwrap(cs.Err); err != ErrMetricNil {
		t.Errorf("unwrap failed: %v", err)
	}
	if cs.AutoReconnect {
		t.Errorf("not auto-reconnect: %s", cs)
	}

	// CheckConnection restores auto reconnect
	t.Log("CC")
	rdyErr = fmt.Errorf("%w: wrapped", &nerr)
	conn.CheckConnection()
	cs = <-stch
	if cs.Ready() {
		t.Error(cs)
	}
	confirmError(t, cs.Err, nil, "netError")
	if !cs.AutoReconnect {
		t.Errorf("not auto-reconnect: %s", cs)
	}

	conn.ReleaseStateChan(stch)
}

func TestConnectionStateRetryPolicy(t *testing.T) {
	bo := &mockBackOff{
		dur: svcInfluxCfg.BackOffStop,
	}
	var cs ConnectionState

	var rdyRes = &domain.Ready{}
	mc := mockClient{
		ready: func(ctx context.Context) (*domain.Ready, error) {
			return rdyRes, nil
		},
	}
	cf := mockMakeClient(returnMockClient(&mc))
	defer mockMakeClient(cf)

	cfg := &svcInfluxCfg.Connection{
		Id:          "TestConnectionStateImpl",
		RetryPolicy: bo,
	}

	conn, err := NewConnection(cfg, nil, debugLogMaker)
	if err != nil {
		t.Fatal(err)
	}
	defer completeShutdown(t, conn, true)

	stch := conn.RequestStateChan(0)
	cs = <-stch
	confirmError(t, cs.Err, ErrConnRetriesExhausted, "retries exhausted")
	confirmError(t, cs.Err, ErrConnNotReady, "not ready")
	if cs.AutoReconnect {
		t.Error("BackoffStop left AutoReconnect enabled")
	}

	conn.ReleaseStateChan(stch)
}

func TestConnectionShutdownDuringCheckReady(t *testing.T) {
	id := "TestConnectionShutdownDuringCheckReady"
	inRdy := make(chan struct{})
	rdv := make(chan context.Context)
	mc := mockClient{
		ready: func(ctx context.Context) (*domain.Ready, error) {
			t.Logf("%s: blocking in ready", id)
			close(inRdy)
			<-ctx.Done()
			t.Logf("%s: past ctx", id)
			rdv <- ctx
			return nil, ctx.Err()
		},
	}
	cf := mockMakeClient(returnMockClient(&mc))
	defer mockMakeClient(cf)

	cfg := &svcInfluxCfg.Connection{
		Id: id,
	}
	conn, err := NewConnection(cfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	<-inRdy
	conn.LogSetPriority(lw.Debug)

	sdch, err := conn.Shutdown(cancelledContext())
	if err != nil {
		t.Fatalf("shutdown: %s", err.Error())
	}

	cs := conn.State()
	confirmError(t, cs.Err, ErrConnShuttingDown, "shutting down")

	cap := <-rdv
	confirmError(t, cap.Err(), context.Canceled, "context canceled")

	for bs := range sdch {
		t.Logf("shutdown: %s", bs)
	}

	cs = conn.State()
	confirmError(t, cs.Err, ErrConnShutDown, "shut down")

	// The resident command handler completes LogSetPriority but doesn't
	// do anything because that'd be a race condition with the main
	// goroutine, which is still running until the ready check is
	// released.
	conn.LogSetPriority(lw.Info)
	if pri := conn.LogPriority(); false && pri != lw.Debug {
		t.Errorf("LogPriority after shutdown failed: %v", pri)
	}

	stch := conn.RequestStateChan(1)
	cs, ok := <-stch
	if !ok {
		t.Errorf("stch unexpected inactive")
	}
	confirmError(t, cs.Err, ErrConnNotReady, "connection not ready: shut down")
	confirmError(t, cs.Err, ErrConnShutDown, "connection not ready: shut down")
	if cs.AutoReconnect {
		t.Errorf("shutdown cs left AutoReconnect")
	}

	cs, ok = <-stch
	if ok {
		t.Errorf("stch unexpected active")
	}

	conn.ReleaseStateChan(stch)

	// CheckConnection should not hang
	conn.CheckConnection()

	// Repeated shutdown should not hang
	t.Logf("second shutdown")
	completeShutdown(t, conn, true)
	t.Logf("past shutdown")

	// State should not hang
	cs = conn.State()

	// StopReconnection should not hang
	conn.StopReconnection()
}

type lineMetric struct {
	pt  lp.Metric
	rec string
}

func makeMetrics(ts, bv, cnt int) []lp.Metric {
	pts := make([]lp.Metric, cnt)
	for i := 0; i < cnt; i++ {
		pts[i], _ = lp.New(
			"meas",
			map[string]string{
				"tag": "t",
			},
			map[string]interface{}{
				"field": bv,
			},
			time.Unix(int64(ts), 0))
		bv++
		ts++
	}
	return pts
}

func makeLineMetrics(ts, bv, cnt int, optprec ...time.Duration) ([]*lineMetric, time.Duration) {
	var sb strings.Builder
	enc := lp.NewEncoder(&sb)
	prec := time.Nanosecond
	if len(optprec) > 0 {
		prec = optprec[0]
	}
	enc.SetPrecision(prec)
	lps := make([]*lineMetric, cnt)
	for i, pt := range makeMetrics(ts, bv, cnt) {
		sb.Reset()
		_, _ = enc.Encode(pt)
		lps[i] = &lineMetric{
			pt:  pt,
			rec: sb.String(),
		}
	}
	return lps, prec
}

func makeBatchState(ts, bv, cnt int, optprec ...time.Duration) (int, *Batch) {
	var sb strings.Builder
	lps, prec := makeLineMetrics(ts, bv, cnt, optprec...)
	for _, lp := range lps {
		sb.WriteString(lp.rec)
	}
	return bv + cnt, &Batch{
		numMetrics: cnt,
		prec:       prec,
		lpData:     sb.String(),
	}
}

func TestBatch(t *testing.T) {
	ts := 1
	bv, bs := makeBatchState(ts, 1, 0)
	if v := bs.NumMetrics(); v != 0 {
		t.Errorf("empty points %d", v)
	}
	if v := bs.NumRunes(); v != 0 {
		t.Errorf("empty runes %d", v)
	}
	if v := bs.LPData(); v != "" {
		t.Errorf("empty runes '%s'", v)
	}
	if v := bs.Precision(); v != time.Nanosecond {
		t.Errorf("empty inferred wrong precision: %v", v)
	}
	ts++
	bv, bs2 := makeBatchState(ts, bv, 1)
	r2 := len(bs2.lpData)
	if v := bs2.NumRunes(); v != r2 {
		t.Errorf("non-empty runes %d", v)
	}
	if ok, _ := bs.Merge(*bs2, r2-1); ok {
		t.Error("incorrect merge success")
	}
	if ok, err := bs.Merge(*bs2, r2); !ok || err != nil {
		t.Error("incorrect merge failure")
	}

	if bs.NumMetrics() != bs2.numMetrics {
		t.Error("merge incorrect NumPt")
	}
	if bs.LPData() != bs2.lpData {
		t.Error("merge incorrect Batch")
	}

	ts++
	bv, bs3 := makeBatchState(ts, bv, 2)
	nr := bs.NumRunes() + bs3.NumRunes()
	if ok, err := bs.Merge(*bs3, nr); !ok || err != nil {
		t.Error("incorrect merge failure")
	}
	if v := bs.NumMetrics(); v != 3 {
		t.Errorf("merge pts %d", v)
	}
	if v := bs.NumRunes(); v != nr {
		t.Errorf("merge NumRunes %d", v)
	}
	if v := bs.LPData(); v != bs2.lpData+bs3.lpData {
		t.Errorf("merge batch %s", v)
	}

	ts++
	_, bs4 := makeBatchState(ts, bv, 1, time.Microsecond)
	_, err := bs.Merge(*bs4, bs.NumMetrics()+1)
	confirmError(t, err, ErrIncompatibleBatch, "1ns vs 1µs")
}

func TestBacklogAddBatch(t *testing.T) {
	bl := newBacklog(4, 16, time.Nanosecond)
	empty := Batch{
		prec: time.Nanosecond,
	}
	b2, _ := NewBatchFromLineProtocol("1\n", time.Nanosecond)
	b3, _ := NewBatchFromLineProtocol("12\n", time.Nanosecond)
	b4, _ := NewBatchFromLineProtocol("1\n2\n", time.Nanosecond)
	b6, _ := NewBatchFromLineProtocol("1\n2\n3\n", time.Nanosecond)
	b18, _ := NewBatchFromLineProtocol("1\n2\n3\n4\n5\n6\n7\n8\n9\n", time.Nanosecond)

	sawEmpty, unflushed, numMetrics, err := bl.addBatch(&empty, 0)
	if !sawEmpty || unflushed || numMetrics != 0 || err != nil {
		t.Errorf("failed: %t %t %d %v", sawEmpty, unflushed, numMetrics, err)
	}

	sawEmpty, unflushed, numMetrics, err = bl.addBatch(b18, 0)
	if !sawEmpty || unflushed || numMetrics != 0 || err == nil {
		t.Errorf("failed: %t %t %d %v", sawEmpty, unflushed, numMetrics, err)
	}
	confirmError(t, err, ErrBacklogOverflow, "16 of 16 available, need 18")

	// 1/2 in acc
	sawEmpty, unflushed, numMetrics, err = bl.addBatch(b2, 0)
	t.Logf("bl: %s", bl)
	if !sawEmpty || !unflushed || numMetrics != 1 || err != nil {
		t.Errorf("failed: %t %t %d %v", sawEmpty, unflushed, numMetrics, err)
	}
	if bl.sb.String() != b2.LPData() {
		t.Errorf("content: %s != %s", lp2tx(bl.sb.String()), lp2tx(b2.LPData()))
	}
	if bl.batchMetrics != b2.NumMetrics() {
		t.Errorf("size: %d != %d", bl.batchMetrics, b2.NumMetrics())
	}

	sawEmpty, unflushed, numMetrics, err = bl.addBatch(&empty, 0)
	if sawEmpty || unflushed || numMetrics != 0 || err != nil {
		t.Errorf("failed: %t %t %d %v", sawEmpty, unflushed, numMetrics, err)
	}

	// 0/0 in acc, 2/4 in cpl
	sawEmpty, unflushed, numMetrics, err = bl.addBatch(b2, 0)
	if !sawEmpty || unflushed || numMetrics != 1 || err != nil {
		t.Errorf("failed: %t %t %d %v", sawEmpty, unflushed, numMetrics, err)
	}
	if bl.sb.String() != "" {
		t.Errorf("content: excess %s", lp2tx(bl.sb.String()))
	}
	if bl.batchMetrics != 0 {
		t.Errorf("size: %d", bl.batchMetrics)
	}
	if c := bl.card; !c.Equal(Cardinality{2, 4}) {
		t.Errorf("backlog %s", c)
	}

	// 0/0 in acc, 2/4 2/4 in cpl
	sawEmpty, unflushed, numMetrics, err = bl.addBatch(b4, 0)
	if !sawEmpty || unflushed || numMetrics != 2 || err != nil {
		t.Errorf("failed: %t %t %d %v", sawEmpty, unflushed, numMetrics, err)
	}
	if bl.batchMetrics != 0 {
		t.Errorf("size: %d", bl.batchMetrics)
	}
	if c := bl.card; !c.Equal(Cardinality{4, 8}) {
		t.Errorf("backlog %s", c)
	}

	// 1/2 in acc, 2/4 2/4 2/4 in cpl
	sawEmpty, unflushed, numMetrics, err = bl.addBatch(b6, 0)
	if !sawEmpty || !unflushed || numMetrics != 3 || err != nil {
		t.Errorf("failed: %t %t %d %v", sawEmpty, unflushed, numMetrics, err)
	}
	if c := bl.card; !c.Equal(Cardinality{6, 12}) {
		t.Errorf("backlog %s", c)
	}
	if bl.batchMetrics != 1 {
		t.Errorf("size: %d", bl.batchMetrics)
	}

	sawEmpty, unflushed, numMetrics, err = bl.addBatch(b3, 0)
	if sawEmpty || unflushed || numMetrics != 0 || err == nil {
		t.Errorf("failed: %t %t %d %v", sawEmpty, unflushed, numMetrics, err)
	}
	confirmError(t, err, ErrBacklogOverflow, "2 of 16 available, need 3")

	sawEmpty, unflushed, numMetrics, err = bl.addBatch(b2, 2)
	if sawEmpty || unflushed || numMetrics != 0 || err == nil {
		t.Errorf("failed: %t %t %d %v", sawEmpty, unflushed, numMetrics, err)
	}
	confirmError(t, err, ErrBacklogOverflow, "0 of 16 available, need 2")

	sawEmpty, unflushed, numMetrics, err = bl.addBatch(b2, 0)
	if !sawEmpty || unflushed || numMetrics != 1 || err != nil {
		t.Errorf("failed: %t %t %d %v", sawEmpty, unflushed, numMetrics, err)
	}
	if c := bl.card; !c.Equal(Cardinality{8, 16}) {
		t.Errorf("backlog %s", c)
	}
	if bl.batchMetrics != 0 {
		t.Errorf("size: %d", bl.batchMetrics)
	}

	sawEmpty, unflushed, numMetrics, err = bl.addBatch(b2, 0)
	if !sawEmpty || unflushed || numMetrics != 0 || err == nil {
		t.Errorf("failed: %t %t %d %v", sawEmpty, unflushed, numMetrics, err)
	}
	confirmError(t, err, ErrBacklogOverflow, "0 of 16 available, need 2")

	// Test short add that fills accumulation
	bl.reset()
	_, unflushed, numMetrics, _ = bl.addBatch(b4, 0)
	if unflushed {
		t.Errorf("didn't flush full batch")
	}
	if numMetrics != 2 {
		t.Errorf("count wrong: %d", numMetrics)
	}
	if c := bl.accCard(); !c.Equal(Cardinality{0, 0}) {
		t.Errorf("not flushed")
	}
	if l := len(bl.batches); l != 1 {
		t.Errorf("len wrong: %d", l)
	}
	if c := bl.card; !c.Equal(Cardinality{2, 4}) {
		t.Errorf("contents wrong: %s", c)
	}

	defer checkBNT(t)
	_, _, _, _ = bl.addBatch(&unterminatedBatch, 0)
}

func TestBacklog(t *testing.T) {
	bl := newBacklog(4, 20, time.Nanosecond)
	if bl.batchSize != 4 || bl.cap != 20 {
		t.Fatalf("newBacklog: %s", bl)
	}
	if v := bl.accCard(); !v.Equal(Cardinality{0, 0}) {
		t.Errorf("accCard: %v", v)
	}
	if v := bl.sizes(); !v.Equal(Cardinality{0, 0}) {
		t.Errorf("accCard: %v", v)
	}
	if !bl.empty() || bl.haveAccumulating() || bl.haveComplete() {
		t.Errorf("have failed: %t %t %t", bl.empty(), bl.haveAccumulating(), bl.haveComplete())
	}

	lpData := "1\n2\n3\n4\n5\n6\n7\n8\n9\n"
	batch, _ := NewBatchFromLineProtocol(lpData, time.Nanosecond)
	sawEmpty, unflushed, numMetrics, err := bl.addBatch(batch, 0)
	if !sawEmpty || !unflushed || numMetrics != 9 || err != nil {
		t.Errorf("failed: %t %t %d %v", sawEmpty, unflushed, numMetrics, err)
	}
	t.Logf("bl: %s", bl)
	if c := bl.accCard(); !c.Equal(Cardinality{1, 2}) {
		t.Errorf("acc wrong: %s", c)
	}
	if c := bl.sizes(); !c.Equal(Cardinality{9, 18}) {
		t.Errorf("sizes wrong: %s", c)
	}

	b2 := bl.finalizeBatch()
	if b2.NumMetrics() != 1 || b2.LPData() != "9\n" {
		t.Errorf("finalize wrong: %d %s", b2.NumMetrics(), b2.LPData())
	}
	if c := bl.sizes(); !c.Equal(Cardinality{8, 16}) {
		t.Errorf("sizes wrong: %s", c)
	}

	if flushed := bl.flush(); flushed {
		t.Errorf("flush succeeded")
	}

	_, _, _, _ = bl.addBatch(b2, 0)
	if c := bl.sizes(); !c.Equal(Cardinality{9, 18}) {
		t.Errorf("sizes wrong: %s", c)
	}

	if flushed := bl.flush(); !flushed {
		t.Errorf("flush failed")
	}

	if bl.empty() {
		t.Errorf("wrong empty")
	}

	b2 = bl.dequeueAll()
	t.Logf("dequeued: %s", bl)
	if !bl.empty() {
		t.Errorf("dequeue didn't empty")
	}
	if b2.NumMetrics() != batch.NumMetrics() {
		t.Errorf("dequeueAll len fail: %d %d", b2.NumMetrics(), batch.NumMetrics())
	}
	if b2.LPData() != batch.LPData() {
		t.Errorf("dequeueAll data fail: %s %s", b2.LPData(), batch.LPData())
	}
	if !reflect.DeepEqual(b2, batch) {
		t.Errorf("dequeueAll wrong: %d %v %s\n%d %v %s", b2.NumMetrics(),
			b2.Precision(), b2.LPData(),
			batch.NumMetrics(), batch.Precision(), batch.LPData())
	}

	if bl.flush() {
		t.Errorf("flush succeeded")
	}

	// Build up batches 2/2/2/4 to test dequeue

	batch, _ = NewBatchFromLineProtocol(lpData[:2], time.Nanosecond)
	_, _, _, _ = bl.addBatch(batch, 0)
	if c := bl.sizes(); !c.Equal(Cardinality{1, 2}) {
		t.Errorf("sizes wrong: %s", c)
	}
	if !bl.flush() {
		t.Errorf("flush failed")
	}
	if c := bl.sizes(); !c.Equal(Cardinality{1, 2}) {
		t.Errorf("sizes wrong: %s", c)
	}
	if l := len(bl.batches); l != 1 {
		t.Errorf("wrong length: %d", l)
	}

	batch, _ = NewBatchFromLineProtocol(lpData[2:4], time.Nanosecond)
	_, _, _, _ = bl.addBatch(batch, 0)
	if !bl.flush() {
		t.Errorf("flush failed")
	}
	if c := bl.sizes(); !c.Equal(Cardinality{2, 4}) {
		t.Errorf("sizes wrong: %s", c)
	}
	if l := len(bl.batches); l != 2 {
		t.Errorf("wrong length: %d", l)
	}

	batch, _ = NewBatchFromLineProtocol(lpData[4:6], time.Nanosecond)
	_, _, _, _ = bl.addBatch(batch, 0)
	if !bl.flush() {
		t.Errorf("flush failed")
	}
	if c := bl.sizes(); !c.Equal(Cardinality{3, 6}) {
		t.Errorf("sizes wrong: %s", c)
	}
	if l := len(bl.batches); l != 3 {
		t.Errorf("wrong length: %d", l)
	}

	batch, _ = NewBatchFromLineProtocol(lpData[6:10], time.Nanosecond)
	_, unflushed, _, _ = bl.addBatch(batch, 0)
	if unflushed {
		t.Errorf("didn't flush")
	}
	if c := bl.accCard(); !c.Equal(Cardinality{0, 0}) {
		t.Errorf("acc wrong: %s", c)
	}

	// t.Logf("ready %s", bl.card)
	if c := bl.sizes(); !c.Equal(Cardinality{5, 10}) {
		t.Errorf("sizes wrong: %s", c)
	}
	if l := len(bl.batches); l != 4 {
		t.Errorf("wrong length: %d", l)
	}

	b2 = bl.dequeue()
	if b2.NumMetrics() != 2 || b2.LPData() != "1\n2\n" {
		t.Errorf("wrong %d %s", b2.NumMetrics(), b2.LPData())
	}
	if c := bl.sizes(); !c.Equal(Cardinality{3, 6}) {
		t.Errorf("sizes wrong: %s", c)
	}
	if l := len(bl.batches); l != 2 {
		t.Errorf("wrong length: %d", l)
	}

	b2 = bl.dequeue()
	if b2.NumMetrics() != 1 || b2.LPData() != "3\n" {
		t.Errorf("wrong %d %s", b2.NumMetrics(), b2.LPData())
	}
	if c := bl.sizes(); !c.Equal(Cardinality{2, 4}) {
		t.Errorf("sizes wrong: %s", c)
	}
	if l := len(bl.batches); l != 1 {
		t.Errorf("wrong length: %d", l)
	}

	b2 = bl.dequeue()
	if b2.NumMetrics() != 2 || b2.LPData() != "4\n5\n" {
		t.Errorf("wrong %d %s", b2.NumMetrics(), b2.LPData())
	}
	if c := bl.sizes(); !c.Equal(Cardinality{0, 0}) {
		t.Errorf("sizes wrong: %s", c)
	}
	if l := len(bl.batches); l != 0 {
		t.Errorf("wrong length: %d", l)
	}
	if !bl.empty() {
		t.Errorf("not empty")
	}

	b2 = bl.dequeue()
	if b2 != nil {
		t.Errorf("wrong: %v", b2)
	}

	b2 = bl.dequeueAll()
	if b2 == nil || !b2.Empty() {
		t.Errorf("wrong: %v", b2)
	}

}

func TestBucketSetWriteHandler(t *testing.T) {
	cf := mockMakeClient(returnMockClient(nil))
	defer mockMakeClient(cf)

	cfg := &svcInfluxCfg.Connection{
		Id: "TestBucketSetWriteHandler",
		Buckets: map[string]*svcInfluxCfg.Bucket{
			"bucket": {
				Organization: "types_test",
			},
		},
	}
	conn, err := NewConnection(cfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer completeShutdown(t, conn, true)

	b := conn.FindBucket("bucket")

	if b.wsc != nil {
		t.Error("init not nil")
	}

	b.SetWriteSettledCallback(func(batch Batch, err error, bs BucketState) {})
	if b.wsc == nil {
		t.Error("set failed")
	}
}

func TestBucketHandleWriteSettled(t *testing.T) {
	merr := &mockError{}
	wab := &mockWriteAPIBlocking{}
	chGo := struct{}{}
	readyCh := make(chan struct{}) // indicates WAB is blocking
	goCh := make(chan struct{})    // indicates WAB may continue
	wab.writeRecord = func(ctx context.Context, line ...string) error {
		readyCh <- chGo
		select {
		case <-goCh:
		case <-ctx.Done():
			return ctx.Err()
		}
		return wab.defaultWriteRecord(ctx, line...)
	}
	mc := &mockClient{
		writeAPIBlocking: func(org, bucket string) api.WriteAPIBlocking {
			return wab
		},
	}
	cf := mockMakeClient(returnMockClient(mc))
	defer mockMakeClient(cf)

	cfg := &svcInfluxCfg.Connection{
		Id:           "TestBucketHandleWriteSettled",
		Organization: "types_test",
		Buckets: map[string]*svcInfluxCfg.Bucket{
			"bucket": {
				Options: &svcInfluxCfg.BucketOptions{
					FlushInterval:     edcode.Duration(10 * time.Second),
					BatchRuneCapacity: 100,
					HeldRuneCapacity:  500,
				},
				Measurements: map[string]*svcInfluxCfg.Schema{
					"meas": nil,
				},
			},
		},
	}
	conn, err := NewConnection(cfg, nil, debugLogMaker)
	if err != nil {
		t.Fatal(err)
	}
	defer completeShutdown(t, conn, true)

	stch := conn.RequestStateChan(1)
	<-stch

	b := conn.FindBucket("bucket")
	if b == nil {
		t.Fatal("no bucket")
	}
	if v := b.Capacity(); v != 500 {
		t.Fatalf("capacity wrong: %d", v)
	}
	// Synchronize
	_ = b.State()

	ws := defaultedWriteSettler(nil)
	wscch := ws.requestNotify()

	pts := makeMetrics(1, 1, 11)

	// Set up writer to raise an error.  Absence of WSC should cause the
	// metrics to be dropped.
	wab.retErr = merr

	// Queue 7 metrics in two complete and one pending batch
	batch, rej := b.MakeBatch(pts...)
	bs, err := b.QueueBatch(batch)
	if rej != nil || err != nil {
		t.Errorf("failed: %v %v", rej, err)
	}
	if bs.Bucket != b {
		t.Error("bucket mismatch")
	}
	// Make that three complete batches
	b.FlushBatch()

	// Wait for first branch to get to the writer, and verify the
	// state.
	<-readyCh
	bs = b.State()
	t.Logf("bs0 %v\n", b.State())
	if v := bs.BatchSize; !v.Equal(Cardinality{0, 0}) {
		t.Errorf("bad batch state: %v", v)
	}
	if v := bs.PendingSize; !v.Equal(Cardinality{3, 93}) {
		t.Errorf("bad pending state: %v", v)
	}
	if v := bs.HeldSize; !v.Equal(Cardinality{11, 345}) {
		t.Errorf("bad held state: %v", v)
	}
	if v := bs.ReadyBatchCount; v != 3 {
		t.Errorf("bad ready batch count: %v", v)
	}
	if v := bs.MetricsWritten; v != 0 {
		t.Errorf("bad write count: %v", v)
	}
	if v := bs.MetricsDropped; v != 0 {
		t.Errorf("bad drop count: %v", v)
	}

	goCh <- chGo // Release first transmission, which should fail and drop
	<-readyCh    // Wait for the second transmission

	// Verify the drop
	bs = b.State()
	t.Logf("bs1 %v\n", bs)
	if v := bs.ReadyBatchCount; v != 2 {
		t.Errorf("bad ready batch count: %v", v)
	}
	if v := bs.ReadySize; !v.Equal(Cardinality{5, 159}) {
		t.Errorf("bad pending state: %v", v)
	}
	if v := bs.PendingCount; v != 1 {
		t.Errorf("bad pending count: %v", v)
	}
	if v := bs.PendingSize; !v.Equal(Cardinality{3, 93}) {
		t.Errorf("bad pending state: %v", v)
	}
	if v := bs.HeldSize; !v.Equal(Cardinality{8, 252}) {
		t.Errorf("bad held state: %v", v)
	}
	if v := bs.MetricsWritten; v != 0 {
		t.Errorf("bad write count: %v", v)
	}
	if v := bs.MetricsDropped; v != 3 {
		t.Errorf("bad drop count: %v", v)
	}
	calls := ws.calls()
	if v := len(calls); v != 0 {
		t.Errorf("wsc called: %v", v)
	}

	b.SetWriteSettledCallback(ws.callback)
	wab.retErr = nil
	goCh <- chGo // Release second transmission
	<-wscch      // Acknowledge the write settled callback
	calls = ws.calls()

	// Verify settled of second transmission
	if v := len(calls); v != 1 {
		t.Errorf("wsc called: %v", v)
	}
	call := calls[0]
	if v := call.batch.Cardinality(); !v.Equal(Cardinality{3, 93}) {
		t.Errorf("wsc batch %s", v)
	}
	if v := call.err; v != nil {
		t.Errorf("wsc err %v", v)
	}
	bs = call.bs
	t.Logf("call bs %v", bs)
	if bs.Bucket != b {
		t.Error("wrong bucket")
	}
	if v := bs.PendingSize; !v.Equal(Cardinality{0, 0}) {
		t.Errorf("bad pending state: %v", v)
	}
	if v := bs.ReadyBatchCount; v != 2 {
		t.Errorf("bad ready batch count: %v", v)
	}
	if v := bs.ReadySize; !v.Equal(Cardinality{5, 159}) {
		t.Errorf("bad ready state: %v", v)
	}
	if v := bs.PendingSize; !v.Equal(Cardinality{0, 0}) {
		t.Errorf("bad pending state: %v", v)
	}
	if v := bs.HeldSize; !v.Equal(Cardinality{5, 159}) {
		t.Errorf("bad held state: %v", v)
	}
	if v := bs.MetricsWritten; v != 3 {
		t.Errorf("bad write count: %v", v)
	}
	if v := bs.MetricsDropped; v != 3 {
		t.Errorf("bad drop count: %v", v)
	}

	<-readyCh // Wait for the third transmission
	bs = b.State()
	t.Logf("bs2 %v", bs)
	if v := bs.ReadyBatchCount; v != 1 {
		t.Errorf("bad ready batch count: %v", v)
	}
	if v := bs.ReadySize; !v.Equal(Cardinality{2, 66}) {
		t.Errorf("bad ready state: %v", v)
	}
	if v := bs.PendingSize; !v.Equal(Cardinality{3, 93}) {
		t.Errorf("bad pending state: %v", v)
	}
	if v := bs.HeldSize; !v.Equal(Cardinality{5, 159}) {
		t.Errorf("bad held state: %v", v)
	}
	if v := bs.MetricsWritten; v != 3 {
		t.Errorf("bad write count: %v", v)
	}
	if v := bs.MetricsDropped; v != 3 {
		t.Errorf("bad drop count: %v", v)
	}

	bs = b.State()
	t.Logf("end %v", bs)
	if v := bs.MetricsDropped; v != 3 {
		t.Errorf("bad drop count: %v", v)
	}
}

var epoch = time.Unix(0, 0)

// This creates an lp.Metric that is not an lp.MutableMetric.
func immutableFromMetric(m lp.Metric) lp.Metric {
	type immutableMetric struct {
		lp.Metric
	}
	return &immutableMetric{
		Metric: lp.FromMetric(m),
	}
}

func TestBucketCheckMetric(t *testing.T) {
	lm, _ := lp.New("", nil, nil, epoch)
	if _, ok := lm.(AddField); !ok {
		t.Error("updatable does not implement AddField")
	}
	b := &Bucket{
		sch: SchemaMap{
			"mn": {},
			"mr": {
				reqTags: set.MakeSet[string]("t1"),
			},
			"mf": {
				fieldTypes: map[string]svcInfluxCfg.FieldType{
					"f1": svcInfluxCfg.Integer,
				},
			},
		},
	}
	for k, s := range b.sch {
		s.bkt = b
		s.name = k
		s.initFieldSets()
	}

	pt, _ := lp.New("mx", nil, nil, epoch)
	err := b.checkMetric(pt)
	confirmError(t, err, ErrMetricUnknown, ": mx")
	b.allowUnknown = true
	err = b.checkMetric(pt)
	if err != nil {
		t.Error(err)
	}

	pt, _ = lp.New("mr", nil, nil, epoch)
	err = b.checkMetric(pt)
	if err != nil {
		t.Error(err)
	}
	b.validateTags = true
	err = b.checkMetric(pt)
	confirmError(t, err, ErrMetricTagsMissing, ": t1")
	pt.AddTag("t1", "v1")
	err = b.checkMetric(pt)
	if err != nil {
		t.Error(err)
	}

	pt, _ = lp.New("mf", nil, nil, epoch)
	pt.AddField("f1", 1.0)
	um := lp.FromMetric(pt)
	mm := immutableFromMetric(pt)
	f1 := pt.FieldList()[0]
	if f1.Key != "f1" {
		t.Fatal("f1 field lookup failed")
	}
	if _, ok := f1.Value.(float64); !ok {
		t.Fatal("init not float64")
	}
	err = b.checkMetric(pt)
	if err != nil {
		t.Fatal(err)
	}
	mfs := b.FindSchema("mf")
	t.Logf("%v", mfs.intFields)
	f1 = pt.FieldList()[0]
	if _, ok := f1.Value.(int64); !ok {
		t.Fatalf("norm not int64: %T", f1.Value)
	}

	f1 = mm.FieldList()[0]
	if f1.Key != "f1" {
		t.Fatal("f1 field lookup failed")
	}
	if _, ok := f1.Value.(float64); !ok {
		t.Fatalf("init not float64: %T", f1.Value)
	}

	err = b.checkMetric(um)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(pt.FieldList(), um.FieldList()) {
		t.Errorf("not equal")
	}

	err = b.checkMetric(mm)
	confirmError(t, err, ErrMetricFieldValue, "incorrect metric field value type: f1:float64!int64")
	var fve *ErrorMetricFieldValue
	if !errors.As(err, &fve) {
		t.Fatalf("non-AddField missing error: %v", err)
	}
	if v := len(fve.Updates); v != 1 {
		t.Errorf("fve updates len %d", v)
	}
	if m := fve.Metric; m != mm {
		t.Errorf("wrong metric")
	}

	f1.Value = 1.0
	if _, ok := f1.Value.(float64); !ok {
		t.Fatal("init not float64")
	}
	b.bypassNormalization = true
	err = b.checkMetric(pt)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := f1.Value.(float64); !ok {
		t.Fatal("unnorm not float64")
	}
}

type writeSettledArgs struct {
	batch Batch
	err   error
	bs    BucketState
}

// Infrastructure for common task of capturing and controlling calls to
// handleWriteSettled.
type writeSettler struct {
	m        sync.Mutex
	captures []*writeSettledArgs
	notifyCh chan struct{}
	callback WriteSettledCallback
}

// Return a channel that will be closed when the write settler callback is
// invoked.  Invoking calls() after the channel closes is guaranteed to
// include the results of the call that closed it.
func (ws *writeSettler) requestNotify() <-chan struct{} {
	ws.m.Lock()
	defer ws.m.Unlock()
	if ws.notifyCh == nil {
		ws.notifyCh = make(chan struct{})
	}
	return ws.notifyCh
}

func (ws *writeSettler) defaultCallback(batch Batch, err error, bs BucketState) {
	args := &writeSettledArgs{
		batch: batch,
		err:   err,
		bs:    bs,
	}
	ws.m.Lock()
	defer ws.m.Unlock()
	if nch := ws.notifyCh; nch != nil {
		ws.notifyCh = make(chan struct{})
		close(nch)
	}
	ws.captures = append(ws.captures, args)
}

func defaultedWriteSettler(ws *writeSettler) *writeSettler {
	if ws == nil {
		ws = &writeSettler{}
	}
	if ws.callback == nil {
		ws.callback = ws.defaultCallback
	}
	return ws
}

func (ws *writeSettler) calls() []*writeSettledArgs {
	ws.m.Lock()
	defer ws.m.Unlock()
	return ws.captures[:]
}

func TestBucketMakeBatch(t *testing.T) {
	pts := makeMetrics(1, 1, 8)
	b := &Bucket{
		sch: SchemaMap{
			"meas": {
				fieldTypes: map[string]svcInfluxCfg.FieldType{
					"field": svcInfluxCfg.UInteger,
				},
			},
		},
		prec:         time.Millisecond,
		checkMetrics: true,
	}
	for k, s := range b.sch {
		s.bkt = b
		s.name = k
		s.initFieldSets()
	}
	ba, rej := b.MakeBatch(pts...)
	if rej != nil {
		t.Errorf("rej: %v", rej)
	}
	if v := ba.Cardinality(); !v.Equal(Cardinality{8, 200}) {
		t.Errorf("cardinality: %s", v)
	}
	if ba.lpData != `meas,tag=t field=1i 1000
meas,tag=t field=2i 2000
meas,tag=t field=3i 3000
meas,tag=t field=4i 4000
meas,tag=t field=5i 5000
meas,tag=t field=6i 6000
meas,tag=t field=7i 7000
meas,tag=t field=8i 8000
` {
		t.Errorf("batch:\n%s", ba.lpData)
	}

	tm1 := lp.FromMetric(pts[0]).(lp.MutableMetric)
	tm1.AddField("field", 1.2)
	mm1 := immutableFromMetric(tm1)
	mm2, _ := lp.New("other", nil, nil, epoch)
	mm3, _ := lp.New("", nil, nil, epoch)
	ba, rej = b.MakeBatch(mm1, mm2, mm3)
	if rej == nil {
		t.Error("missing rej")
	}
	err := rej[mm1]
	confirmError(t, err, ErrMetricFieldValue, "incorrect metric field value type: field:float64!uint64")
	var fve *ErrorMetricFieldValue
	if !errors.As(err, &fve) {
		t.Fatalf("non-AddField missing error: %v", err)
	}
	if v, ok := fve.Updates[0].Value.(uint64); !ok || v != 1 {
		t.Errorf("update value not correct")
	}
	err = rej[mm2]
	confirmError(t, err, ErrMetricUnknown, mm2.Name())
	err = rej[mm3]
	confirmError(t, err, ErrMetricUnknown, "")

	b.allowUnknown = true
	ba, rej = b.MakeBatch(mm3)
	if rej == nil {
		t.Error("missing rej")
	}
	err = rej[mm3]
	confirmError(t, err, lp.ErrInvalidName, "invalid name")
}

func TestBucketQueueBatch(t *testing.T) {
	// Sync on ready checks and count the number of times the connection
	// is checked.
	var rdyCalls int32
	ready := func(ctx context.Context) (*domain.Ready, error) {
		atomic.AddInt32(&rdyCalls, 1)
		rdyStatus := domain.ReadyStatusReady
		return &domain.Ready{
			Status: &rdyStatus,
		}, nil
	}

	// Capture calls to handleWriteSettler
	ws := defaultedWriteSettler(nil)

	// Force WAB errors to test control path, allowing correlation with
	// settled callback.
	type batchData struct {
		wabErr error
		retErr error
		lpData string
		wsa    *writeSettledArgs
	}
	nerr := netError{}
	herrv := http2.Error{
		StatusCode: 403,
	}
	batchCtl := []*batchData{
		{},
		{
			wabErr: &herrv,
		},
		{},
		{
			wabErr: &nerr,
		},
		{},
	}
	wabRelease := make(chan *batchData, len(batchCtl))
	wab := mockWriteAPIBlocking{}
	wab.writeRecord = func(ctx context.Context, line ...string) error {
		var bd *batchData
		var err error
		select {
		case bd = <-wabRelease:
			bd.lpData = line[0]
			err = bd.wabErr
		case <-ctx.Done():
			return ctx.Err()
		}
		if err == nil {
			err = wab.defaultWriteRecord(ctx, line...)
		}
		bd.retErr = err
		return bd.retErr
	}

	mc := defaultedMockClient(&mockClient{
		ready: ready,
		writeAPIBlocking: func(org, bucket string) api.WriteAPIBlocking {
			return &wab
		},
	})
	cf := mockMakeClient(returnMockClient(mc))
	defer mockMakeClient(cf)

	cfg := &svcInfluxCfg.Connection{
		Id:           "TestBucketQueueBatch",
		Organization: "types_test",
		Buckets: map[string]*svcInfluxCfg.Bucket{
			"bucket": {
				Options: &svcInfluxCfg.BucketOptions{
					FlushInterval:     edcode.Duration(10 * time.Second),
					BatchRuneCapacity: 100,
					HeldRuneCapacity:  500,
					PendingLimit:      3,
				},
				Measurements: map[string]*svcInfluxCfg.Schema{
					"meas": nil,
				},
			},
		},
	}
	conn, err := NewConnection(cfg, nil, debugLogMaker)
	if err != nil {
		t.Fatal(err)
	}
	defer completeShutdown(t, conn, true)

	// We expect four connection state updates: the initial one, then the
	// two triggered by write failures, plus one for the status sent when
	// the channel is requested.  Use that as the channel capacity so the
	// connection goroutine won't be blocked in a notification while we're
	// trying to send it the shutdown command.
	stch := conn.RequestStateChan(4)
	<-stch
	var nr int32 = 1
	if nrs := atomic.LoadInt32(&rdyCalls); nrs != nr {
		t.Fatal("ready wrong")
	}
	b := conn.FindBucket("bucket")
	if b == nil {
		t.Fatal("no bucket")
	}

	b.SetWriteSettledCallback(ws.callback)

	// Prep for four complete batches plus two accumulating metrics
	batch, rej := b.MakeBatch(makeMetrics(1, 1, 14)...)
	bs, err := b.QueueBatch(batch)
	if rej != nil || err != nil {
		t.Fatalf("failed: %v %v", rej, err)
	}
	t.Logf("qp bs: %s", bs)
	if v := bs.BatchSize; !v.Equal(Cardinality{2, 66}) {
		t.Fatalf("batch: %s", v)
	}
	if v := bs.ReadyBatchCount; v != 4 {
		t.Fatalf("bad ready batch count: %v", v)
	}
	if v := bs.ReadySize; !v.Equal(Cardinality{12, 378}) {
		t.Fatalf("ready: %s", v)
	}
	if v := bs.PendingSize; !v.Equal(Cardinality{0, 0}) {
		t.Fatalf("pending size: %s", v)
	}
	if v := bs.HeldSize; !v.Equal(Cardinality{14, 444}) {
		t.Fatalf("backlog: %s", v)
	}
	if v := bs.MetricsSubmitted; v != 14 {
		t.Fatalf("submitted: %d", v)
	}

	bs = b.State()

	t.Logf("post qp: %s", bs)
	if v := bs.BatchSize; !v.Equal(Cardinality{2, 66}) {
		t.Fatalf("batch: %s", v)
	}
	if v := bs.ReadyBatchCount; v != 1 {
		t.Fatalf("bad ready batch count: %v", v)
	}
	if v := bs.ReadySize; !v.Equal(Cardinality{3, 99}) {
		t.Fatalf("ready: %s", v)
	}
	if v := bs.PendingCount; v != 3 {
		t.Fatalf("pending count: %d", v)
	}
	if v := bs.PendingSize; !v.Equal(Cardinality{9, 279}) {
		t.Fatalf("pending: %s", v)
	}
	if v := bs.HeldSize; !v.Equal(Cardinality{14, 444}) {
		t.Fatalf("backlog: %s", v)
	}

	for _, bd := range batchCtl {
		wabRelease <- bd
	}
	err = b.Drain(context.Background())
	if err != nil {
		t.Logf("drain: %v", err)
	}

	bs = b.State()
	t.Logf("post drain: %s", bs)

	for i, bd := range batchCtl {
		for _, a := range ws.calls() {
			if bd.lpData == a.batch.lpData {
				bd.wsa = a
				if bd.retErr != bd.wabErr {
					t.Errorf("%d: bd err %v != %v", i, bd.retErr, bd.wabErr)
				}
				if bd.retErr != a.err {
					t.Errorf("%d: err %v != wsc %v", i, bd.retErr, a.err)
				}
			}
		}
	}

	// Issue a non-blocking shutdown so we settle any pending ready check
	// before we verify that these were emitted.
	completeShutdown(t, conn, false)

	// Consume state changes until shutdown completes
	ok := true
	for ok {
		var cs ConnectionState
		time.Sleep(time.Millisecond)
		cs, ok = <-stch
		t.Logf("%t, cs %v", ok, cs)
	}

	bs = b.State()
	t.Logf("post sync: %s", bs)

	nrs := atomic.LoadInt32(&rdyCalls)
	t.Logf("got %d connection checks", nrs)
	// We can observe 3 if each expected check completed before the next
	// arrived; or 2 if the third check arrived while the second was still
	// pending (the third would be dropped).  We would observe 1 if the
	// second is still pending when we get here, which we've ruled out by
	// shutting down the connection above: this will wait for the ready
	// check to complete.
	if nrs < 2 || nrs > 3 {
		t.Errorf("expect 2 or 3 connection rechecks: %d", nrs)
	}
}

func TestBucketShutdown(t *testing.T) {
	wab := mockWriteAPIBlocking{}

	wabRelease := make(chan struct{})
	wab.retErr = &mockError{}
	wab.writeRecord = func(ctx context.Context, line ...string) error {
		// NB: We specifically do not wake on ctx.Done() because
		// shutdown closes the context asynchronously from the test
		// code, and we want to make sure the request remains pending
		// until we've verified other state.
		<-wabRelease
		return wab.defaultWriteRecord(ctx, line...)
	}

	mc := mockClient{
		writeAPIBlocking: func(org, bucket string) api.WriteAPIBlocking {
			return &wab
		},
	}
	cf := mockMakeClient(returnMockClient(&mc))
	defer mockMakeClient(cf)

	cfg := &svcInfluxCfg.Connection{
		Id:           "TestBucketShutdown",
		Organization: "types_test",
		Buckets: map[string]*svcInfluxCfg.Bucket{
			"stuff": {
				Options: &svcInfluxCfg.BucketOptions{
					FlushInterval:     edcode.Duration(10 * time.Second),
					BatchRuneCapacity: 100,
					HeldRuneCapacity:  500,
				},
				Measurements: map[string]*svcInfluxCfg.Schema{
					"meas": nil,
				},
			},
		},
	}
	conn, err := NewConnection(cfg, nil, debugLogMaker)
	if err != nil {
		t.Fatal(err)
	}
	defer completeShutdown(t, conn, true)

	stch := conn.RequestStateChan(1)
	for cs := range stch {
		t.Logf("flush read %s", cs)
		if cs.Ready() {
			break
		}
	}

	b := conn.FindBucket("stuff")
	if b == nil {
		t.Fatal("no bucket")
	}
	if c := cap(b.pending); c != 1 {
		t.Fatalf("pending capacity: %d", c)
	}

	// Capture calls to handleWriteSettler
	ws := defaultedWriteSettler(nil)
	wscch := ws.requestNotify()
	b.SetWriteSettledCallback(ws.callback)

	pts := makeMetrics(1, 1, 8)

	ba, rej := b.MakeBatch(pts...)
	if rej != nil {
		t.Errorf("rej: %v", rej)
	}
	if v := ba.Cardinality(); !v.Equal(Cardinality{8, 248}) {
		t.Errorf("cardinality: %s", v)
	}
	if ba.lpData != `meas,tag=t field=1i 1000000000
meas,tag=t field=2i 2000000000
meas,tag=t field=3i 3000000000
meas,tag=t field=4i 4000000000
meas,tag=t field=5i 5000000000
meas,tag=t field=6i 6000000000
meas,tag=t field=7i 7000000000
meas,tag=t field=8i 8000000000
` {
		t.Errorf("batch:\n%s", ba.lpData)
	}

	batch, rej := b.MakeBatch(pts...)
	bs, err := b.QueueBatch(batch)
	t.Logf("bs: %v\n", bs)
	if rej != nil || err != nil {
		t.Fatalf("failed: %v %v", rej, err)
	}
	if v := bs.BatchSize; !v.Equal(Cardinality{2, 62}) {
		t.Fatalf("bad batch state: %v", v)
	}
	if v := bs.ReadyBatchCount; v != 2 {
		t.Fatalf("bad ready batch count: %v", v)
	}
	if v := bs.ReadySize; !v.Equal(Cardinality{6, 186}) {
		t.Fatalf("bad batch state: %v", v)
	}
	if v := bs.PendingSize; !v.Equal(Cardinality{0, 0}) {
		t.Fatalf("bad pending state: %v", v)
	}
	if v := bs.HeldSize; !v.Equal(Cardinality{8, 248}) {
		t.Fatalf("bad ready state: %v", v)
	}

	bs = b.State() // sync
	t.Logf("bs: %v\n", bs)
	if rej != nil || err != nil {
		t.Fatalf("failed: %v %v", rej, err)
	}
	if v := bs.BatchSize; !v.Equal(Cardinality{2, 62}) {
		t.Fatalf("bad batch state: %v", v)
	}
	if v := bs.ReadyBatchCount; v != 1 {
		t.Fatalf("bad ready batch count: %v", v)
	}
	if v := bs.ReadySize; !v.Equal(Cardinality{3, 93}) {
		t.Fatalf("bad batch state: %v", v)
	}
	if v := bs.PendingCount; v != 1 {
		t.Fatalf("bad pending count: %v", v)
	}
	if v := bs.PendingSize; !v.Equal(Cardinality{3, 93}) {
		t.Fatalf("bad pending state: %v", v)
	}
	if v := bs.HeldSize; !v.Equal(Cardinality{8, 248}) {
		t.Fatalf("bad held state: %v", v)
	}
	if v := bs.ReadyBatchCount; v != 1 {
		t.Fatalf("bad ready batch count: %v", v)
	}

	b.LogSetPriority(lw.Debug)
	sdch, err := conn.Shutdown(cancelledContext())
	t.Logf("sdch %v %T %v", sdch, err, err)
	if err != nil {
		t.Fatalf("unexpected error: %T %v", err, err)
	}
	if v := cap(sdch); v != len(cfg.Buckets) {
		t.Errorf("cap %d", v)
	}

	bs = b.State() // sync, after this write settled data will be available

	// The pending write will have been cancelled and the batch flushed,
	// but the pending write is still pending because we haven't released
	// it.
	t.Logf("bs: %v", bs)
	if v := bs.HeldSize; !v.Equal(Cardinality{8, 248}) {
		t.Fatalf("backlog %s", v)
	}
	if v := bs.PendingSize; !v.Equal(Cardinality{3, 93}) {
		t.Fatalf("pending %s", v)
	}
	wshist := ws.calls()
	if v := len(wshist); v != 0 {
		t.Fatalf("wsc called %d times", v)
	}

	// Queue operations while shutting down are rejected with a terminated
	// error, but the bucket state is not yet terminated.
	//
	// TODO: Non-determinism: the bucket may process the QueueBatch before
	// it receives the shutdown command.
	time.Sleep(time.Millisecond)
	batch, _ = b.MakeBatch(pts[4])
	bs, err = b.QueueBatch(batch)
	t.Logf("bs qb: %v", bs)
	if bs.Terminated {
		t.Error("premature termination marker")
	}
	confirmError(t, err, ErrBucketTerminated, "terminated: stuff")

	// Release the pending write (which will "succeed"), then request
	// state until it's confirmed terminated.
	t.Log("releasing pending write")
	close(wabRelease)
	<-wscch
	wscch = ws.requestNotify()
	t.Log("got wsc notification from write")
	wshist = ws.calls()
	if v := len(wshist); v < 1 {
		t.Fatalf("wsc called %d times", v)
	}

	// First settled is the in-flight data
	wv := wshist[0]
	bs = wv.bs
	t.Logf("bs: %v", bs)
	if v := wv.batch.Cardinality(); !v.Equal(Cardinality{3, 93}) {
		t.Fatalf("failed %v", v)
	}
	if bs.Terminated {
		t.Error("premature termination marker")
	}
	confirmError(t, wv.err, ErrBucketTerminated, "stuff")

	if len(wshist) < 2 {
		<-wscch
	}
	t.Log("got wsc notification from backlog")
	wshist = ws.calls()
	if v := len(wshist); v != 2 {
		t.Fatalf("wsc called %d times", v)
	}
	// Second settled is the backlog
	wv = wshist[1]
	bs = wv.bs
	t.Logf("bs: %v", bs)
	if v := wv.batch.Cardinality(); !v.Equal(Cardinality{5, 155}) {
		t.Fatalf("failed %v", v)
	}
	if !bs.Terminated {
		t.Error("missing termination marker")
	}
	confirmError(t, wv.err, ErrBucketTerminated, "stuff")

	// An attempt to queue metrics while terminating should be rejected
	// without invoking write recovery.
	batch, _ = b.MakeBatch(pts[:3]...)
	bs, err = b.QueueBatch(batch)
	confirmError(t, err, ErrBucketTerminated, "")
	wshist = ws.calls()
	if v := len(wshist); v != 2 {
		t.Fatalf("wsc called %d times", v)
	}

	// FlushBatch and Drain on a terminated bucket returns immediately.
	b.FlushBatch()
	err = b.Drain(context.Background())
	if err != nil {
		t.Errorf("unexpected drain error: %v", err)
	}

	batch, _ = b.MakeBatch(pts[4])
	bs, err = b.QueueBatch(batch)
	confirmError(t, err, ErrBucketTerminated, "")
	if v := len(wshist); v != 2 {
		t.Errorf("wsc called %d times", v)
	}

	// SetWSC on a terminated bucket will synchronize and exit
	b.SetWriteSettledCallback(nil)

	bs = b.State()
	t.Logf("bs: %v", bs)
	if !bs.Terminated {
		t.Errorf("bucket not terminated")
	}

	// The resident command handler completes LogSetPriority but doesn't
	// do anything because that'd be a race condition with the main
	// goroutine if it were still running.
	b.LogSetPriority(lw.Info)
	if pri := b.LogPriority(); false && pri != lw.Debug {
		t.Errorf("LogPriority after shutdown failed: %v", pri)
	}
}

func TestBucketShutdownWhenIdle(t *testing.T) {
	wab := mockWriteAPIBlocking{}

	wabRelease := make(chan struct{})
	wab.retErr = &mockError{}
	wab.writeRecord = func(ctx context.Context, line ...string) error {
		// NB: We specifically do not wake on ctx.Done() because
		// shutdown closes the context asynchronously from the test
		// code, and we want to make sure the request remains pending
		// until we've verified other state.
		<-wabRelease
		return wab.defaultWriteRecord(ctx, line...)
	}

	mc := mockClient{
		writeAPIBlocking: func(org, bucket string) api.WriteAPIBlocking {
			return &wab
		},
	}
	cf := mockMakeClient(returnMockClient(&mc))
	defer mockMakeClient(cf)

	cfg := &svcInfluxCfg.Connection{
		Id:           "TestBucketShutdown",
		Organization: "types_test",
		Buckets: map[string]*svcInfluxCfg.Bucket{
			"stuff": {
				Options: &svcInfluxCfg.BucketOptions{
					FlushInterval:     edcode.Duration(10 * time.Second),
					BatchRuneCapacity: 100,
					HeldRuneCapacity:  500,
				},
				Measurements: map[string]*svcInfluxCfg.Schema{
					"meas": nil,
				},
			},
		},
	}
	conn, err := NewConnection(cfg, nil, debugLogMaker)
	if err != nil {
		t.Fatal(err)
	}
	defer completeShutdown(t, conn, true)

	b := conn.FindBucket("stuff")
	if b == nil {
		t.Fatal("no bucket")
	}

	sdch, err := conn.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %T %s", err, err.Error())
	}
	bs := <-sdch
	if !bs.Terminated {
		t.Fatalf("unterminated: %s", bs)
	}
	_, ok := <-sdch
	if ok {
		t.Fatalf("not terminated")
	}

	select {
	case <-conn.Done():
	default:
		// The connection closes sdch just before finalizing done; if
		// the test is too fast we might check for Done before it's
		// been signaled.  Give it a little more time.
		t.Logf("not done yet")
		time.Sleep(time.Millisecond)
	}
	confirmError(t, conn.Err(), done.TerminatedOK, "")
}

func TestBucketShutdownWhenBlocked(t *testing.T) {
	wab := mockWriteAPIBlocking{}

	wabRelease := make(chan struct{})
	wab.retErr = &mockError{}
	wab.writeRecord = func(ctx context.Context, line ...string) error {
		// NB: We specifically do not wake on ctx.Done() because
		// shutdown closes the context asynchronously from the test
		// code, and we want to make sure the request remains pending
		// until we've verified other state.
		select {
		case <-wabRelease:
			t.Logf("wR released")
		case <-ctx.Done():
			t.Logf("wR ctx cancelled: %s", ctx.Err())
		}

		return wab.defaultWriteRecord(ctx, line...)
	}

	mc := mockClient{
		writeAPIBlocking: func(org, bucket string) api.WriteAPIBlocking {
			return &wab
		},
	}
	cf := mockMakeClient(returnMockClient(&mc))
	defer mockMakeClient(cf)

	cfg := &svcInfluxCfg.Connection{
		Id:           "TestBucketShutdown",
		Organization: "types_test",
		Buckets: map[string]*svcInfluxCfg.Bucket{
			"stuff": {
				Options: &svcInfluxCfg.BucketOptions{
					FlushInterval:     edcode.Duration(10 * time.Second),
					BatchRuneCapacity: 100,
					HeldRuneCapacity:  500,
				},
				Measurements: map[string]*svcInfluxCfg.Schema{
					"meas": nil,
				},
			},
		},
	}
	conn, err := NewConnection(cfg, nil, debugLogMaker)
	if err != nil {
		t.Fatal(err)
	}
	defer completeShutdown(t, conn, true)

	b := conn.FindBucket("stuff")
	if b == nil {
		t.Fatal("no bucket")
	}

	ba, _ := b.MakeBatch(makeMetrics(1, 1, 8)...)
	if v := ba.Cardinality(); !v.Equal(Cardinality{8, 248}) {
		t.Fatalf("cardinality: %s", v)
	}
	bs, err := b.QueueBatch(ba)
	if err != nil {
		t.Fatalf("queue failed: %s", err.Error())
	}
	t.Logf("%s", bs)

	ctx, cancel := context.WithCancel(context.Background())
	sdch, err := conn.Shutdown(ctx)
	if err != nil {
		t.Fatalf("shutdown err: %s", err.Error())
	}
	bs = b.State()
	t.Logf("post shutdown: %s\n", bs)
	time.Sleep(time.Millisecond)
	if v := len(sdch); v != 0 {
		t.Logf("unexpected completions: %d", v)
	}
	cancel()
	t.Logf("post cancel: %s\n", b.State())
	nsd := 0
	for bs := range sdch {
		t.Logf("Bucket result: %s", bs)
		nsd++
		if nsd > 1 {
			t.Fatalf("too many bucket notifications: %d", nsd)
		}
		if v := bs.MetricsDropped; v != 8 {
			t.Errorf("bad drop count: %v", v)
		}
	}
}

func TestDrainAll(t *testing.T) {
	cf := mockMakeClient(returnMockClient(nil))
	defer mockMakeClient(cf)

	cfg := &svcInfluxCfg.Connection{
		Id:           "TestDrainAll",
		Organization: "org",
		Buckets: map[string]*svcInfluxCfg.Bucket{
			"b1": {},
			"b2": {},
			"b3": {},
		},
	}
	conn, err := NewConnection(cfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer completeShutdown(t, conn, true)

	ctx := context.Background()

	n := 0
	for r := range conn.DrainAll(ctx) {
		n++
		t.Logf("%s result: %v", r.Bucket.Name(), r.Err)
		if r.Err != nil {
			t.Errorf("unexpected error: %v", r.Err)
		}
	}
	if n != 3 {
		t.Errorf("received %d results", n)
	}

	ctx, cancel := context.WithCancel(ctx)
	cancel()
	n = 0
	for r := range conn.DrainAll(ctx) {
		t.Logf("%s result: %v", r.Bucket.Name(), r.Err)
		// If the bucket is already drained, as is the case here, we
		// have a race between whether that case of the select wins,
		// or the one that detects ctx.Done().  Allow both.
		if r.Err != nil && !errors.Is(r.Err, context.Canceled) {
			t.Errorf("unexpected error: %T %v", r.Err, r.Err)
		}
		n++
	}
	if n != 3 {
		t.Errorf("received %d results", n)
	}
}

// An error that support both Is() and Temporary().
type mockError struct {
	temporary bool
	is        error
}

func (e *mockError) Error() string {
	return "mock error"
}

func (e *mockError) Is(target error) bool {
	return target == e.is
}

func (e *mockError) Temporary() bool {
	return e.temporary
}

type mockWriteAPIBlocking struct {
	writeRecord func(ctx context.Context, line ...string) error
	writePoint  func(ctx context.Context, point ...*write.Point) error
	retErr      error
	m           sync.Mutex
	ptStore     []*write.Point
	batches     []string
}

func (w *mockWriteAPIBlocking) defaultWritePoint(ctx context.Context, point ...*write.Point) error {
	w.m.Lock()
	defer w.m.Unlock()
	if w.retErr != nil {
		return w.retErr
	}
	w.ptStore = append(w.ptStore, point...)
	return nil
}

func (w *mockWriteAPIBlocking) defaultWriteRecord(ctx context.Context, line ...string) error {
	w.m.Lock()
	defer w.m.Unlock()
	if w.retErr != nil {
		return w.retErr
	}
	w.batches = append(w.batches, strings.Join(line, "\n")+"\n")
	return nil
}

func (w *mockWriteAPIBlocking) WriteRecord(ctx context.Context, line ...string) error {
	return w.writeRecord(ctx, line...)
}

func (w *mockWriteAPIBlocking) WritePoint(ctx context.Context, point ...*write.Point) error {
	return w.writePoint(ctx, point...)
}

type mockBackOff struct {
	dur time.Duration
}

func (m *mockBackOff) NextBackOff() time.Duration {
	return m.dur
}

func (m *mockBackOff) Reset() {}

type mockClient struct {
	setup             func(ctx context.Context, username, password, org, bucket string, retentionPeriodHours int) (*domain.OnboardingResponse, error)
	ready             func(ctx context.Context) (*domain.Ready, error)
	health            func(ctx context.Context) (*domain.HealthCheck, error)
	ping              func(ctx context.Context) (bool, error)
	close             func()
	options           func() *influxdb2.Options
	serverURL         func() string
	httpService       func() http2.Service
	writeAPI          func(org, bucket string) api.WriteAPI
	writeAPIBlocking  func(org, bucket string) api.WriteAPIBlocking
	queryAPI          func(org string) api.QueryAPI
	authorizationsAPI func() api.AuthorizationsAPI
	organizationsAPI  func() api.OrganizationsAPI
	usersAPI          func() api.UsersAPI
	deleteAPI         func() api.DeleteAPI
	bucketsAPI        func() api.BucketsAPI
	labelsAPI         func() api.LabelsAPI
	tasksAPI          func() api.TasksAPI
}

func (m *mockClient) Setup(ctx context.Context, username, password, org, bucket string, retentionPeriodHours int) (*domain.OnboardingResponse, error) {
	return m.setup(ctx, username, password, org, bucket, retentionPeriodHours)
}
func (m *mockClient) Ready(ctx context.Context) (*domain.Ready, error) {
	return m.ready(ctx)
}
func (m *mockClient) Health(ctx context.Context) (*domain.HealthCheck, error) {
	return m.health(ctx)
}
func (m *mockClient) Ping(ctx context.Context) (bool, error) {
	return m.ping(ctx)
}
func (m *mockClient) Close() {
	m.close()
}
func (m *mockClient) Options() *influxdb2.Options {
	return m.options()
}
func (m *mockClient) ServerURL() string {
	return m.serverURL()
}
func (m *mockClient) HTTPService() http2.Service {
	return m.httpService()
}
func (m *mockClient) WriteAPI(org, bucket string) api.WriteAPI {
	return m.writeAPI(org, bucket)
}
func (m *mockClient) WriteAPIBlocking(org, bucket string) api.WriteAPIBlocking {
	return m.writeAPIBlocking(org, bucket)
}
func (m *mockClient) QueryAPI(org string) api.QueryAPI {
	return m.queryAPI(org)
}
func (m *mockClient) AuthorizationsAPI() api.AuthorizationsAPI {
	return m.authorizationsAPI()
}
func (m *mockClient) OrganizationsAPI() api.OrganizationsAPI {
	return m.organizationsAPI()
}
func (m *mockClient) UsersAPI() api.UsersAPI {
	return m.usersAPI()
}
func (m *mockClient) DeleteAPI() api.DeleteAPI {
	return m.deleteAPI()
}
func (m *mockClient) BucketsAPI() api.BucketsAPI {
	return m.bucketsAPI()
}
func (m *mockClient) LabelsAPI() api.LabelsAPI {
	return m.labelsAPI()
}
func (m *mockClient) TasksAPI() api.TasksAPI {
	return m.tasksAPI()
}

func defaultedMockClient(mc *mockClient) *mockClient {
	if mc == nil {
		// Minimum client signals ready
		mc = &mockClient{}
	}
	if mc.ready == nil {
		mc.ready = func(ctx context.Context) (*domain.Ready, error) {
			status := domain.ReadyStatusReady
			return &domain.Ready{
				Status: &status,
			}, nil
		}
	}
	if mc.writeAPIBlocking == nil {
		wab := mockWriteAPIBlocking{}
		wab.writePoint = wab.defaultWritePoint
		wab.writeRecord = wab.defaultWriteRecord
		mc.writeAPIBlocking = func(org, bucket string) api.WriteAPIBlocking {
			return &wab
		}
	}
	return mc
}

func returnMockClient(mc *mockClient) func(serverURL, authToken string, options *influxdb2.Options) influxdb2.Client {
	mc = defaultedMockClient(mc)
	return func(serverURL string, authToken string, options *influxdb2.Options) influxdb2.Client {
		return mc
	}
}

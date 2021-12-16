// Copyright 2021-2022 Peter Bigot Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

// Some terminology, since the distinction between a metric and a measurement
// and their relationship to schema are somewhat loose in InfluxDB
// documentation.
//
// measurement: The name used in a metric to identify the data stored in the
// metric's fields.  A measurement is just a string.
//
// metric: An observation that couples a measurement (name), a timestamp, a
// tag set, and a field set.  Imprecise language might use measurement where
// metric would be more correct.
//
// schema: Information about the metrics for a given measurement.  This usage
// is inherited from the Node-Influx client library, which associates with
// measurements the set of tags expected for each measurement, and the types
// to be used for field values.  It's a subset of the InfluxDB concept of
// schema which extends to buckets and series.

package influxdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb-client-go/v2" // influxdb2
	"github.com/influxdata/influxdb-client-go/v2/api"
	http2 "github.com/influxdata/influxdb-client-go/v2/api/http"
	"github.com/influxdata/influxdb-client-go/v2/domain"
	lp "github.com/influxdata/line-protocol"

	"github.com/pabigot/done"
	lw "github.com/pabigot/logwrap"
	"github.com/pabigot/set"

	svcInfluxCfg "github.com/pabigot/svcutil/influxdb/config"
)

const (
	// Redis key prefix to retrieve the current schema supported on the
	// local influx server.
	SCHEMA_REDIS_KEY = "cfg:influx:schema"

	// Redis subscribe channel for notifications of changes to the influx
	// schema.
	SCHEMA_REDIS_PSCHAN = "ps:cfg"
)

// ErrorBatch instances as error values are passed to panic() if the code
// encounters a Batch that violates a Batch contract requirement, e.g. a
// non-empty batch that lacks a newline terminator.  There *should* be no way
// to construct such a thing outside of the module testing environment; if
// this occurs there's probably a bug in this module.
type ErrorBatch struct {
	base  error
	Batch *Batch
}

func (e *ErrorBatch) Error() string {
	return e.base.Error()
}

func (e *ErrorBatch) Is(target error) bool {
	return errors.Is(e.base, target)
}

func makeErrorBatch(base error, batch *Batch) error {
	return &ErrorBatch{
		base:  base,
		Batch: batch,
	}
}

// Error type returned when a metric is rejected as identified by
// ErrMetricFieldValue.
type ErrorMetricFieldValue struct {
	Metric  lp.Metric
	Updates []*lp.Field
}

func (e *ErrorMetricFieldValue) Error() string {
	var desc []string
	fl := e.Metric.FieldList()
	fm := make(map[string]interface{}, len(fl))
	for _, fv := range fl {
		fm[fv.Key] = fv.Value
	}
	for _, upd := range e.Updates {
		desc = append(desc, fmt.Sprintf("%s:%T!%T", upd.Key, fm[upd.Key], upd.Value))
	}
	return fmt.Sprintf("%s: %s", ErrMetricFieldValue.Error(), strings.Join(desc, " ; "))
}

func (e *ErrorMetricFieldValue) Is(target error) bool {
	return target == ErrMetricFieldValue
}

var (
	// Error returned when some operation would result in the backlog size
	// limit being exceeded.
	ErrBacklogOverflow = errors.New("bucket backlog would overflow")

	// Identifies an ErrorBatch for a non-empty batch that lacks the
	// required terminal newline.  This indicates a bug in the module.
	ErrBatchNotTerminated = errors.New("missing newline terminator")

	// Identifies an ErrorBatch where it's been determined that a batch's
	// metric count was wrong.  This indicates a bug in the module.
	ErrBatchMetricCount = errors.New("inconsistent metric count")

	// Error returned from BucketSchemaMapFromConfig when a bucket name is
	// empty.
	ErrBucketSchemaMapKey = errors.New("empty bucket name")

	// Error returned from BucketSchemaMapFromConfig when a schema name is
	// empty.
	ErrBucketSchemaMapSchema = errors.New("empty schema name")

	// Error returned when the bucket has been shut down.  This may wrap
	// an error that provides additional context for WriteSettledCallback.
	ErrBucketTerminated = errors.New("bucket terminated")

	// Connection state Err wrapper when connection status has been
	// verified to be unavailable by an explicit response from the server,
	// or the connection has been shut down.
	ErrConnNotReady = errors.New("connection not ready")

	// Connection state Err wrapper when the reconnections have been
	// disabled by application command.
	ErrConnRetriesDisabled = errors.New("retries disabled")

	// Connection state Err wrapper when the connection failed and the configured
	// RetryPolicy disallows more automatic retries.
	ErrConnRetriesExhausted = errors.New("retries exhausted")

	// Connection state Err when ConnectionShutdown() has completed.
	ErrConnShutDown = fmt.Errorf("%w: shut down", ErrConnNotReady)

	// Connection state Err when Connection Shutdown() has been initiated but has
	// not yet completed.
	ErrConnShuttingDown = fmt.Errorf("%w: shutting down", ErrConnNotReady)

	// Connection state Err when connection status is unknown.
	ErrConnStateUnknown = errors.New("connection state unknown")

	// Error returned when some operation would combine batches that are
	// incompatible, e.g. the line protocol encodings do not have the same
	// precision.
	ErrIncompatibleBatch = errors.New("incompatible batch")

	// Error returned when a provided precision is not one of the
	// precisions supported by line protocol.
	ErrInvalidPrecision = errors.New("invalid precision")

	// Error returned from metric validation when the metric schema
	// defines fields, and the metric includes additional fields.
	ErrMetricFieldsUnknown = errors.New("metric includes unknown field(s)")

	// Error returned from metric checking when the metric has field
	// values that are inconsistent with the schema, and the metric is not
	// an instance of write.Point (for which the infrastructure can
	// internally update the field values).  Values can be asserted to
	// pointers to ErrorMetricFieldValue instances to identify the
	// incorrectly-typed fields.
	ErrMetricFieldValue = errors.New("incorrect metric field value type")

	// Error returned from metric validation when the measurement does not
	// match the schema against which it is validated.
	ErrMetricMeasurement = errors.New("metric measurement mismatch")

	// Error returned when a nil pointer is validated.
	ErrMetricNil = errors.New("metric is nil")

	// Error returned from metric validation when the measurement isn't
	// described in the bucket schema, and validation disallows this case.
	ErrMetricUnknown = errors.New("metric measurement missing")

	// Error returned from metric validation when the schema requires a
	// tag that is not in the metric.
	ErrMetricTagsMissing = errors.New("metric lacks required tag(s)")

	// Error returned from metric validation when the metric includes tags
	// that are not present in the required or optional tag sets.
	ErrMetricTagsUnknown = errors.New("metric includes unknown tag(s)")
)

type errBucketTerminated struct {
	parent, err error
}

func (e *errBucketTerminated) Error() string {
	es := e.parent.Error()
	if e.err != nil {
		es = fmt.Sprintf("%s: %s", es, e.err.Error())
	}
	return es
}

func (e *errBucketTerminated) Is(target error) bool {
	return target == ErrBucketTerminated || errors.Is(e.err, target)
}

func (e *errBucketTerminated) Unwrap() error {
	return e.err
}

type errConnRetriesBase struct {
	err error
}

func (e *errConnRetriesBase) Unwrap() error {
	return e.err
}

type errConnRetriesExhausted struct {
	errConnRetriesBase
}

func newErrConnRetriesExhausted(err error) *errConnRetriesExhausted {
	return &errConnRetriesExhausted{
		errConnRetriesBase{
			err: err,
		},
	}
}
func (e *errConnRetriesExhausted) Error() string {
	return fmt.Sprintf("%s: %s", ErrConnRetriesExhausted.Error(), e.err.Error())
}

func (e *errConnRetriesExhausted) Is(target error) bool {
	return target == ErrConnRetriesExhausted || errors.Is(e.err, target)
}

type errConnRetriesDisabled struct {
	errConnRetriesBase
}

func newErrConnRetriesDisabled(err error) *errConnRetriesDisabled {
	return &errConnRetriesDisabled{
		errConnRetriesBase{
			err: err,
		},
	}
}

func (e *errConnRetriesDisabled) Error() string {
	return fmt.Sprintf("%s: %s", ErrConnRetriesDisabled.Error(), e.err.Error())
}

func (e *errConnRetriesDisabled) Is(target error) bool {
	return target == ErrConnRetriesDisabled || errors.Is(e.err, target)
}

// ConnectionState describes the state of the connection.
type ConnectionState struct {
	// Err is nil if the Connection appears to be ready, otherwise the
	// error that triggered the connection being made unready.
	// ErrConnNotReady indicates the server is responsive but not ready;
	// other errors indicate inability to reach the server.  The value
	// often contains multiple wrapped errors to fully describe the state,
	// e.g. ErrConnRetriesExhausted and ErrConnNotReady.
	//
	// Use this for diagnosis.  Check for connection readiness with the
	// Ready() method.
	Err error

	// AutoReconnect is true if the infrastructure will attempt to
	// reconnect whenever an error is detected.  It can be cleared by
	// invoking StopReconnections() on the Connection.
	AutoReconnect bool

	// Hidden value that inhibits Ready() for uninitialized instances.
	initialized bool
}

func newConnectionState() ConnectionState {
	return ConnectionState{
		initialized:   true,
		Err:           ErrConnStateUnknown,
		AutoReconnect: true,
	}
}

// Ready returns true if and only if the connection state is valid and does
// not indicate an error.
func (cs ConnectionState) Ready() bool {
	return cs.initialized && cs.Err == nil
}

func (cs ConnectionState) String() string {
	ar := ""
	if !cs.AutoReconnect {
		ar = " (reconnect disabled)"
	}
	if cs.Ready() {
		return fmt.Sprintf("ready%s", ar)
	} else if !cs.initialized {
		return "not-ready: uninitialized"
	}
	return fmt.Sprintf("not-ready%s: %s", ar, cs.Err.Error())
}

// Cardinality measures a collection of metrics in number of line protocol
// records (metrics), and number of runes in the aggregated newline-terminated
// representation of the records.
type Cardinality struct {
	Metrics int
	Runes   int
}

// Add adjusts the count of the receiver upwards by the value in the argument.
func (c *Cardinality) Add(add Cardinality) *Cardinality {
	c.Metrics += add.Metrics
	c.Runes += add.Runes
	return c
}

// Sub adjusts the count of the receiver down by the value in the argument.
func (c *Cardinality) Sub(sub Cardinality) *Cardinality {
	c.Metrics -= sub.Metrics
	c.Runes -= sub.Runes
	return c
}

// Reset sets the cardinality to zero.
func (c *Cardinality) Reset() *Cardinality {
	c.Metrics = 0
	c.Runes = 0
	return c
}

func (c Cardinality) Equal(c2 Cardinality) bool {
	return c.Metrics == c2.Metrics && c.Runes == c2.Runes
}

func (c Cardinality) String() string {
	return fmt.Sprintf("%d runes (%d metrics)", c.Runes, c.Metrics)
}

// BucketState provides information about a bucket.
type BucketState struct {
	// Bucket is a pointer to the Bucket to which the state belongs.
	Bucket *Bucket

	// ConnState provides the most recent ConnectionState received by the
	// bucket.  If the connection has been shut down that will be present
	// in the state.
	ConnState ConnectionState

	// Terminated indicates that the bucket will no longer process new
	// data.
	Terminated bool

	// Maximum number of runes for a batch, i.e. encoded metrics stored in
	// in line protocol format for transmission to the server.
	BatchRuneCapacity int

	// HeldRuneCapacity specifies the number of runes allowed for data held in
	// the backlog (accumulating plus ready plus pending).
	HeldRuneCapacity int

	// Number of metrics and runes in the accumulating batch.
	BatchSize Cardinality

	// Number of metrics and runes in the ready backlog excluding accumulating.
	ReadySize Cardinality

	// Number of batches in the ready backlog.
	ReadyBatchCount int

	// PendingSize counts the metrics and runes currently in transit to
	// the server over all pending writes.
	PendingSize Cardinality

	// PendingCapacity is the maximum number of simultaneous in-flight
	// writes to the server.
	PendingCapacity int

	// PendingCount is the number of in-flight writes to the server.
	PendingCount int

	// HeldSize counts the metrics and runes held by the bucket, whether
	// in the accumulating batch, ready batches, or pending writes.
	HeldSize Cardinality

	// MetricsSubmitted is the number of metrics submitted through
	// QueueBatch.
	MetricsSubmitted int

	// MetricsWritten is the number of metrics successfully written to the
	// server.
	MetricsWritten int

	// MetricsDropped is the number of metrics dropped by the bucket
	// because they could not be written to the server.
	MetricsDropped int
}

// AvailableRuneCapacity returns the maximum number of runes of metric data
// that can be queued into the bucket without overflowing.
func (bs BucketState) AvailableRuneCapacity() int {
	return bs.HeldRuneCapacity - bs.HeldSize.Runes
}

// HoldLevel returns the fraction of hold capacity (in runes) that is being
// used.
func (bs BucketState) HoldLevel() float32 {
	return float32(bs.HeldSize.Runes) / float32(bs.HeldRuneCapacity)
}

func (bs BucketState) String() string {
	terminated := ""
	if bs.Terminated {
		terminated = "TERMINATED "
	}
	return fmt.Sprintf(`%sConn: %s
Bucket: %s
Accumulated %s of %d
Ready %s in %d batches
Sending %s in %d of %d
Holding %s of %d
Submitted %d; Written %d; Dropped %d
`,
		terminated, bs.ConnState,
		bs.Bucket.Name(),
		bs.BatchSize, bs.BatchRuneCapacity,
		bs.ReadySize, bs.ReadyBatchCount,
		bs.PendingSize, bs.PendingCount, bs.PendingCapacity,
		bs.HeldSize, bs.HeldRuneCapacity,
		bs.MetricsSubmitted, bs.MetricsWritten, bs.MetricsDropped)
}

// WriteSettledCallback can be provided by the application on a per-bucket
// basis to be notified of write completion and to retain data when writes
// fail.
//
// batch provides the metrics submitted for write.  This may be empty for the
// final call indicating that the bucket has terminated.
//
// err is nil for successful writes, or describes the reason the write failed.
// If this is ErrBucketTerminated more details on the actual error is obtained
// by unwrapping it, but caller should understand that it will not be able to
// requeue the data.
//
// bs provides information on the bucket state.  The metrics in batch will
// have been recorded as dropped.  When the Terminated field of this state is
// set no further calls should be expected by the application.
//
// WARNING: WriteSettledCallback is animated by the bucket main goroutine, so
// must not block or initiate any functions that communicate with the bucket.
type WriteSettledCallback func(batch Batch, err error, bs BucketState)

// RejectedMetrics maps from submitted metrics to the reason they could not be
// queued for the server.
type RejectedMetrics map[lp.Metric]error

// AddField can be implemented along with lp.Metric to allow Bucket.MakeBatch
// to correct the types of fields to match measurement schema requirements.
// See NormalizeMetric() in Schema.
//
// If metrics are constructed with lp.New() an interface that supports
// AddField() is obtained by type assertion to lp.MutableMetric.  write.Point
// metrics do not satisfy this interface because the AddField method returns a
// pointer.
type AddField interface {
	// AddField must behave as the same method in the lp.MutableMetric
	// interface when provided a new value for an existing field.
	AddField(key string, value interface{})
}

type clientFactory func(serverURL string, authToken string, options *influxdb2.Options) influxdb2.Client

func defaultClientFactory(serverURL string, authToken string, options *influxdb2.Options) influxdb2.Client {
	return influxdb2.NewClientWithOptions(serverURL, authToken, options)
}

var makeClient clientFactory = defaultClientFactory

func mockMakeClient(cf clientFactory) clientFactory {
	rv := makeClient
	makeClient = cf
	return rv
}

// command is used for buffered transmission of data between goroutines.
type command uint8

const (
	cmdDrain command = iota
	cmdFlush
	cmdLogPriority
	cmdPing
	cmdRelChan
	cmdReqChan
	cmdSetLogPriority
	cmdSetWSC
	cmdShutdown
	cmdState
	cmdStopReconnect
)

func (c command) String() string {
	switch c {
	case cmdDrain:
		return "drain"
	case cmdFlush:
		return "flush"
	case cmdLogPriority:
		return "logPriority"
	case cmdPing:
		return "ping"
	case cmdRelChan:
		return "relChan"
	case cmdReqChan:
		return "reqChan"
	case cmdSetLogPriority:
		return "setLogPriority"
	case cmdSetWSC:
		return "setWriteSettledCallback"
	case cmdShutdown:
		return "shutdown"
	case cmdState:
		return "state"
	case cmdStopReconnect:
		return "stop"
	default:
		return fmt.Sprintf("!!cmd:%d", c)
	}
}

type commandData struct {
	cmd command
	wg  sync.WaitGroup
	prm interface{}
}

// Connection captures information about a influxdb Client and the buckets
// to be accessed by that client.
//
// All functions in this interface are safe for concurrent use.
type Connection struct {
	log               lw.Logger
	id                string
	readyCheckTimeout time.Duration
	retryPolicy       svcInfluxCfg.BackOff
	bucket            map[string]*Bucket
	precision         time.Duration
	useGZip           bool
	cli               influxdb2.Client
	stch              chan ConnectionState
	wg                sync.WaitGroup
	cmdch             chan *commandData

	// doneImpl supports connection completion tracking.
	doneImpl done.Implementation
}

// Done returns a channel that is closed when the Monitor exits.
func (c *Connection) Done() <-chan struct{} {
	return c.doneImpl.Done()
}

// Err is nil until the monitor exits, then provides the reason for the exit.
// On normal termination done.TerminatedOK is returned.
func (c *Connection) Err() error {
	return c.doneImpl.Err()
}

// Buckets returns the set of Bucket objects in the connection.
func (c *Connection) Buckets() map[string]*Bucket {
	rv := make(map[string]*Bucket, len(c.bucket))
	for k, v := range c.bucket {
		rv[k] = v
	}
	return rv
}

// NewConnection creates a new connection.  cfg describes how to connect to
// the server and the buckets of interest.  options provides some control over
// the way the influxdb2 client behaves.  newLog provides a way to control
// how log messages are emitted by this package.
//
// Be aware that the options parameter contains the base package write.Options
// that allows setting things like a flush interval, batch size, and retry
// behavior.  Most of these features are specific to using the client write
// APIs (specifically influxdb2.WriteAPI), and are ignored when using the
// Bucket API in this package.  Precision and UseGZip options are present at
// the svcInfluxCfg.Connection level and apply to all server actions regardless of
// bucket, and these values overridden any setting in the passed options.
//
// Other options, such as control of the HTTPClient used to connect to the
// server, are respected as this package doesn't intervene at that level.
//
// If newLog is nil, lw.LogLogMaker will be used.  All created Connection and
// Bucket interfaces will be passed to newLog to construct the logger for the
// goroutine animating those objects.  Supply an implementation if you wish to
// override the default log priority to something other than the lw.Logger
// default of lw.Warning.
func NewConnection(cfg *svcInfluxCfg.Connection, options *influxdb2.Options, newLog lw.LogMaker) (*Connection, error) {
	if newLog == nil {
		newLog = lw.LogLogMaker
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if options == nil {
		options = influxdb2.DefaultOptions()
	}
	c := &Connection{
		id:                cfg.Id,
		readyCheckTimeout: time.Duration(cfg.ReadyCheckTimeout),
		retryPolicy:       cfg.RetryPolicy,
		precision:         time.Duration(cfg.Precision),
		useGZip:           cfg.UseGZip,
		bucket:            make(map[string]*Bucket),
		stch:              make(chan ConnectionState, 1),
		cmdch:             make(chan *commandData),
	}
	options.SetUseGZip(c.useGZip)
	options.SetPrecision(c.precision)
	c.cli = makeClient(cfg.URL, cfg.Token, options)
	c.log = newLog(c)
	for _, bc := range cfg.Buckets {
		newBucket(c, bc, newLog)
	}
	c.wg.Add(1)
	go c.main()
	return c, nil
}

// Id provides a user-supplied identifier for the connection.
func (c *Connection) Id() string {
	return c.id
}

// FindBucket returns the bucket with the given name, if present.  If there is
// no bucket with the given name a nil pointer is returned.
func (c *Connection) FindBucket(name string) *Bucket {
	return c.bucket[name]
}

// UpdateSchema replaces the schema for any bucket on this connection that has
// an entry in the provided map.
func (c *Connection) UpdateSchemas(bsm BucketSchemaMap) {
	for bn, sm := range bsm {
		b := c.bucket[bn]
		if b != nil && sm != nil {
			b.m.Lock()
			b.sch = sm
			b.m.Unlock()
		}
	}
}

// Client returns the InfluxDB client used for the connection.
func (c *Connection) Client() influxdb2.Client {
	return c.cli
}

type reqStateChan struct {
	cap int
	ch  <-chan ConnectionState
}

// RequestStateChan returns a buffered state channel that receives a
// ConnectionState value when the connection state changes.  cap specifies the
// channel capacity required to prevent blocking the caller given the expected
// responsiveness of the process reading from it.  Values of cap less than 1
// are silently replaced by 1.
//
// If the Connection has already completed its first state update the current
// state update will be sent on the channel before this call returns.
func (c *Connection) RequestStateChan(cap int) <-chan ConnectionState {
	if cap < 1 {
		cap = 1
	}
	prm := &reqStateChan{
		cap: cap,
	}
	cd := commandData{
		cmd: cmdReqChan,
		prm: prm,
	}
	cd.wg.Add(1)
	c.cmdch <- &cd
	cd.wg.Wait()
	return prm.ch
}

// ReleaseStateChan informs the Connection that the provided state channel is
// no longer needed.  No further connections will be sent on it, and it will
// be closed.
func (c *Connection) ReleaseStateChan(ch <-chan ConnectionState) {
	c.cmdch <- &commandData{
		cmd: cmdRelChan,
		prm: ch,
	}
}

// State returns the current connection state.
func (c *Connection) State() ConnectionState {
	rv := ConnectionState{}
	cd := commandData{
		cmd: cmdState,
		prm: &rv,
	}
	cd.wg.Add(1)
	c.cmdch <- &cd
	cd.wg.Wait()
	return rv
}

// CheckConnection causes the connection to re-check whether it is ready.  The
// call blocks until the request is received by the connection, but returns
// before the result of the check is available.  The result will appear on all
// state channels.
func (c *Connection) checkConnection(enableRetries bool) {
	c.cmdch <- &commandData{
		cmd: cmdPing,
		prm: &enableRetries,
	}
}

// CheckConnection causes the connection to re-check whether it is ready.  The
// call blocks until the request is received by the connection, but returns
// before the result of the check is available.  The result will appear on all
// state channels.
func (c *Connection) CheckConnection() {
	c.checkConnection(true)
}

// StopReconnection causes any in-progress reconnection attempt to be aborted,
// and the reconnection logic to be reset and halted.  It has no effect on any
// existing connection.  The call blocks until the request has been processed
// by the connection infrastructure.
func (c *Connection) StopReconnection() {
	cd := &commandData{
		cmd: cmdStopReconnect,
	}
	cd.wg.Add(1)
	c.cmdch <- cd
	cd.wg.Wait()
}

type shutdownData struct {
	ctx  context.Context
	sdch chan BucketState
}

// Shutdown stops all processing related to the connection.  The connection
// state maintenance loop terminates, leaving the connection state unchanged.
// All buckets are told to shut down, which involves marking them ineligible
// for queueing new data and flushing any already-received data, but allowing
// them to continue to transmit data they've already accepted.  Once all
// accepted data has been transmitted or rejected the bucket will terminate.
//
// ctx is a context that, when cancelled, forces all buckets to stop trying to
// process the data they've already accepted.  Any in-progress transactions
// are cancelled, and unwritten data is returned to the application.  To
// immediately terminate with untransmitted data rejected pass a context that
// has already been cancelled.  You will want to pass a context that is
// eventually cancelled, generally by a deadline, as shutdown will never
// complete if there's unwritten data and the connection is not available for
// it to be written.)
//
// The returned channel provides the terminal BucketState for each Bucket in
// the connection.  The channel is closed when all buckets have terminated and
// the connection completes shutdown.  The channel has sufficient capacity
// that the caller is not obliged to receive from it.
//
// ErrConnShuttingDown or ErrConnShutDown are returned if a shutdown request
// had already been issued for the connection.
func (c *Connection) Shutdown(ctx context.Context) (<-chan BucketState, error) {
	sdch := make(chan BucketState, len(c.bucket))
	sd := &shutdownData{
		ctx:  ctx,
		sdch: sdch,
	}
	cd := &commandData{
		cmd: cmdShutdown,
		prm: sd,
	}
	cd.wg.Add(1)
	c.cmdch <- cd
	cd.wg.Wait()
	var err error
	var ok bool
	if err, ok = cd.prm.(error); ok && err != nil {
		sdch = nil
	}
	return sdch, err
}

// BucketError associates an bucket with a possibly-nil error from some
// operation performed across buckets.
type BucketError struct {
	// Bucket identifies a bucket.
	Bucket *Bucket
	// Err indicates the result of an operation on Bucket.
	Err error
}

// DrainAll is equivalent to issuing Bucket.Drain() with ctx on all buckets
// and waiting until they all return.
//
// The returned channel can be used to monitor completion results as they
// occur, and is closed once all Drain() results have been reported.  Its
// capacity is sufficient to hold all results, so the caller is not required
// to receive from it.
func (c *Connection) DrainAll(ctx context.Context) <-chan BucketError {
	ch := make(chan BucketError, len(c.bucket))
	go c.drainAll(ctx, ch)
	return ch
}

func (c *Connection) drainAll(ctx context.Context, ch chan BucketError) {
	var wg sync.WaitGroup
	for _, b := range c.bucket {
		wg.Add(1)
		go func(b *Bucket) {
			err := b.Drain(ctx)
			ch <- BucketError{
				Bucket: b,
				Err:    err,
			}
			wg.Done()
		}(b)
	}
	wg.Wait()
	close(ch)
}

// LogSetPriority changes the priority of logging by the connection.
func (c *Connection) LogSetPriority(pri lw.Priority) {
	cd := &commandData{
		cmd: cmdSetLogPriority,
		prm: pri,
	}
	cd.wg.Add(1)
	c.cmdch <- cd
	cd.wg.Wait()
}

// LogPriority returns priority of logging by the connection.
func (c *Connection) LogPriority() lw.Priority {
	cd := &commandData{
		cmd: cmdLogPriority,
	}
	cd.wg.Add(1)
	c.cmdch <- cd
	cd.wg.Wait()
	return cd.prm.(lw.Priority)
}

type readyResult struct {
	ready *domain.Ready
	err   error
}

func (c *Connection) checkReady(ctx context.Context,
	ch chan<- readyResult,
	wg *sync.WaitGroup) {
	rdy, err := c.cli.Ready(ctx)
	ch <- readyResult{rdy, err}
	close(ch)
	wg.Done()
}

func (c *Connection) main() {
	c.log.SetId(fmt.Sprintf("inflconn.%s ", c.id))
	lpr := lw.MakePriPr(c.log)

	for _, b := range c.bucket {
		c.wg.Add(1)
		go b.main(&c.wg)
	}

	autoReconnect := true
	cs := newConnectionState()
	cs.AutoReconnect = autoReconnect
	var rdyResCh chan readyResult
	var crCancel context.CancelFunc
	rdyTmr := time.NewTimer(0)
	rdyTmrC := rdyTmr.C

	var sdChan chan BucketState
	var sdCtx context.Context

	var listeners []chan ConnectionState

	loop := true
	for loop {
		chkReady := false
		select {
		case <-rdyTmrC:
			rdyTmrC = nil
			chkReady = true
		case rr := <-rdyResCh:
			rdyResCh = nil
			crCancel = nil

			err := rr.err
			rdy := rr.ready
			lpr.I("ready %t: %v %v", autoReconnect, err, rdy)

			cs = newConnectionState()
			var nerr net.Error
			var herr *http2.Error
			if err == nil {
				if rdy.Status == nil {
					cs.Err = ErrConnNotReady
				} else if s := *rdy.Status; s != domain.ReadyStatusReady {
					// At the time of writing ReadyStatusReady is the
					// only value in the ReadyStatus domain.
					cs.Err = fmt.Errorf("%w: %s", ErrConnNotReady, s)
				} else {
					cs.Err = nil
				}
			} else if errors.As(err, &nerr) {
				cs.Err = nerr
				lpr.I("ready: net.Error (temporary=%t): %s", nerr.Temporary(), nerr.Error())
			} else if errors.As(err, &herr) {
				cs.Err = herr
				lpr.I("ready: http2.Error: %d: %s\n", herr.StatusCode, herr.Error())
			} else {
				// This can be a context cancel/deadline
				// error, or some other unhandled situation
				lpr.I("ready failed: %T: %s", err, err)
				cs.Err = err
			}
			if rdyTmrC != nil && !rdyTmr.Stop() {
				<-rdyTmr.C
			}
			rdyTmrC = nil
			if cs.Ready() {
				c.retryPolicy.Reset()
			} else if !autoReconnect {
				cs.Err = newErrConnRetriesDisabled(cs.Err)
				lpr.N(cs.Err.Error())
			} else if dl := c.retryPolicy.NextBackOff(); dl != svcInfluxCfg.BackOffStop {
				lpr.I("next retry in %s due to: %s", dl, cs.Err.Error())
				rdyTmrC = rdyTmr.C
				rdyTmr.Reset(dl)
			} else {
				lpr.N("retries stopped by RetryPolicy: %s", cs.Err.Error())
				autoReconnect = false
				cs.Err = newErrConnRetriesExhausted(cs.Err)
			}
			cs.AutoReconnect = autoReconnect
			for _, ch := range listeners {
				ch <- cs
			}
			for _, b := range c.bucket {
				b.stch <- cs
			}
		case cd := <-c.cmdch:
			switch cd.cmd {
			case cmdLogPriority:
				cd.prm = c.log.Priority()
				cd.wg.Done()
			case cmdPing:
				enable := *cd.prm.(*bool)
				lpr.I("cmd: ping, enable %t, rechecking ready %t", enable, rdyResCh == nil)
				if enable {
					autoReconnect = true
					cs.AutoReconnect = autoReconnect
				}
				chkReady = true
			case cmdRelChan:
				ch := cd.prm.(<-chan ConnectionState)
				li := len(listeners) - 1
				for i, lch := range listeners {
					if lch == ch {
						close(lch)
						if i < li {
							listeners[i] = listeners[li]
						}
						listeners[li] = nil
						listeners = listeners[:li]
						break
					}
				}
			case cmdReqChan:
				rsc := cd.prm.(*reqStateChan)
				ch := make(chan ConnectionState, rsc.cap)
				listeners = append(listeners, ch)
				rsc.ch = ch
				cd.wg.Done()
				// Send current state if it's known
				if !errors.Is(cs.Err, ErrConnStateUnknown) {
					ch <- cs
				}
			case cmdSetLogPriority:
				c.log.SetPriority(cd.prm.(lw.Priority))
				cd.wg.Done()
			case cmdShutdown:
				lpr.N("cmd: shutdown")
				sd := cd.prm.(*shutdownData)
				sdChan = sd.sdch
				sdCtx = sd.ctx
				loop = false
				cd.prm = nil
				cd.wg.Done()
			case cmdState:
				rp := cd.prm.(*ConnectionState)
				*rp = cs
				cd.wg.Done()
			case cmdStopReconnect:
				lpr.I("cmd: stop %s, inProgress %t", cs.String(), rdyResCh != nil)
				autoReconnect = false
				cs.AutoReconnect = autoReconnect
				if rdyResCh != nil {
					crCancel()
				}
				if rdyTmrC != nil && !rdyTmr.Stop() {
					<-rdyTmr.C
				}
				c.retryPolicy.Reset()
				cd.wg.Done()
			default:
				panic(fmt.Errorf("unhandled Connection cmd: %s", cd.cmd))
			}
		}

		if chkReady && rdyResCh == nil {
			var crCtx context.Context
			lpr.D("checking ready")
			crCtx, crCancel = context.WithTimeout(context.Background(),
				c.readyCheckTimeout)
			rdyResCh = make(chan readyResult)
			c.wg.Add(1)
			go c.checkReady(crCtx, rdyResCh, &c.wg)
		}
	}

	lpr.I("exiting")

	lbNotify := make(chan struct{})
	cs.Err = ErrConnShuttingDown
	cs.AutoReconnect = false

	// Leave something that will respond to any commands received after
	// shutdown was issued.  It's also told when shutdown completes so it
	// can provide the correct state.
	go func(c *Connection, cs ConnectionState, lbNotify chan struct{}) {
		for {
			var cd *commandData
			select {
			case <-lbNotify:
				lbNotify = nil
				cs.Err = ErrConnShutDown
			case cd = <-c.cmdch:
			}
			if cd == nil {
				continue
			}
			lpr.I("cmd-post-shutdown: %s", cd.cmd)
			switch cd.cmd {
			case cmdLogPriority:
				cd.prm = c.log.Priority()
				cd.wg.Done()
			case cmdPing:
				// nothing to do
			case cmdRelChan:
				// nothing to do
			case cmdReqChan:
				// A new state channel is returned that has
				// been closed after receiving the current
				// state.  It is not recorded so release is a
				// no-op.
				rsc := cd.prm.(*reqStateChan)
				ch := make(chan ConnectionState, rsc.cap)
				rsc.ch = ch
				ch <- cs
				close(ch)
				cd.wg.Done()
			case cmdSetLogPriority:
				// Changing the log priority introduces a race
				// condition with the main goroutine if it
				// hasn't terminated yet, because it's still
				// using the log.  So this does nothing.
				cd.wg.Done()
			case cmdShutdown:
				cd.prm = cs.Err
				cd.wg.Done()
			case cmdState:
				rp := cd.prm.(*ConnectionState)
				*rp = cs
				cd.wg.Done()
			case cmdStopReconnect:
				cd.wg.Done()
			}
		}
	}(c, cs, lbNotify)

	if crCancel != nil {
		crCancel()
		lpr.I("waiting for cancelled in-progress ready check")
		<-rdyResCh
	}

	lpr.I("shutting down buckets")
	var sdwg sync.WaitGroup
	for _, b := range c.bucket {
		sdwg.Add(1)
		go func(b *Bucket) {
			close(b.stch)
			sdChan <- b.shutdown(&cs, sdCtx)
			sdwg.Done()
		}(b)
	}
	sdwg.Wait()
	c.wg.Done() // pairs NewConnection
	c.wg.Wait()

	lpr.I("closing state channels")
	close(c.stch)
	for _, lch := range listeners {
		lch <- cs
		close(lch)
	}

	// Finalize the shutdown
	close(sdChan)
	close(lbNotify)

	lpr.N("exited")
	c.doneImpl.Finalize(nil)
}

// Bucket data management:
//
// Material is submitted by QueueBatch, which causes the content to be
// appended to the batch.  On various conditions the content of the batch is
// appended to the backlog.  When there is material in the backlog and the
// connection state is ready, content is transmitted in chunks of at most
// batchSize.  If successfully transmitted the content is discarded.  If
// transmission fails the content is provided to a callback along with the
// reason for failure, and the callback return determines whether the content
// is discarded or is placed back onto the backlog for retransmission.

// Batch describes a batch of newline-terminated records in line protocol
// format.
//
// NOTE: Line Protocol uses newline-separated records.  Non-empty Batch
// objects always include a terminating newline to simplify combining batches.
type Batch struct {
	// The number of metrics in the batch
	numMetrics int
	// The precision with which timestamps are encoded
	prec time.Duration
	// The line protocol encoding of the batch metrics
	lpData string
}

// The timestamp precision with which the records in the line protocol data
// were encoded.
func (batch *Batch) Precision() time.Duration {
	return batch.prec
}

// Empty indicates that the batch has an empty string for its line protocol
// content, encoding no metrics.
func (batch *Batch) Empty() bool {
	return batch.lpData == ""
}

// setNumMetrics examines the Batch field and sets the numMetrics field to the
// number of non-empty substrings that are separated by newlines and the
// string bounds.
func (batch *Batch) setNumMetrics() int {
	s := batch.lpData
	si := 0
	ei := len(s)
	n := 0
	for si < ei {
		// skip leading newlines
		for ; si < ei && s[si] == '\n'; si++ {
		}
		if si == ei {
			break
		}
		// find next separating newline or end
		for si++; si < ei && s[si] != '\n'; si++ {
		}
		// count the contents as one metric and move on
		n++
		si++
	}
	batch.numMetrics = n
	return n
}

// Create a Batch ready for metrics using a given precision.
func NewBatch(prec time.Duration) (*Batch, error) {
	if err := svcInfluxCfg.ValidatePrecision(prec); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidPrecision, err.Error())
	}
	return &Batch{
		prec: prec,
	}, nil
}

// Create a Batch from a string of newline-separated Line Protocol metrics
// encoded with precision prec.  The batch is empty if lpData contains no
// non-newline runes.
func NewBatchFromLineProtocol(lpData string, prec time.Duration) (*Batch, error) {
	rv, err := NewBatch(prec)
	if err != nil {
		return nil, err
	}
	nr := len(lpData)
	if nr > 0 && lpData[nr-1] != '\n' {
		lpData += "\n"
	}
	rv.lpData = lpData
	rv.setNumMetrics()
	if rv.numMetrics == 0 {
		rv.lpData = ""
	}
	return rv, nil
}

// Return the number of metrics and runes in the batch in a standalone object.
func (batch *Batch) Cardinality() Cardinality {
	return Cardinality{
		Metrics: batch.numMetrics,
		Runes:   len(batch.lpData),
	}
}

// NumRunes returns the length of the line protocol data in runes.
func (batch *Batch) NumRunes() int {
	return len(batch.lpData)
}

// NumMetrics returns the length of the line protocol data in metrics.
func (batch *Batch) NumMetrics() int {
	return batch.numMetrics
}

// LPData returns the line protocol data held by the batch.
func (batch *Batch) LPData() string {
	return batch.lpData
}

// Merge appends the metrics and records from batch to the receiver if doing
// so would not cause the receiver's batch length to exceed maxRunes.
//
// An error will be returned if the precision of metrics in the receiver is
// different from the precision of the metrics in the batch to merge.
//
// Return true if and only if the merge was successful.
func (r *Batch) Merge(batch Batch, maxRunes int) (bool, error) {
	if r.prec != batch.prec {
		return false, fmt.Errorf("%w: %v vs %v", ErrIncompatibleBatch,
			r.prec, batch.prec)
	}
	ok := len(r.lpData)+len(batch.lpData) <= maxRunes
	if ok {
		r.numMetrics += batch.numMetrics
		r.lpData += batch.lpData
	}
	return ok, nil
}

// Partition fragments a large Batch into a sequence of Batches where each
// element is intended to have no more than maxRest runes including newline,
// except the first element should have no more than maxFirst runes.  This is
// used to partition a large batch so that the first element can be used to
// fill out the remaining space in an existing accumulating batch, and the
// subsequent batches will be at maximum capacity until the last which
// contains the remaining records.
//
// Values of maxFirst less than 1 shall be replaced by 1, which will produce
// an empty first batch.
//
// Values of maxRest less than 1 shall be replaced by the cupped maxFirst.
//
// The returned slice shall not be nil, and will be empty only if the batch is
// empty.
//
// The first batch will be empty if the length required to encode the first
// record would exceed maxFirst.  Subsequent batches will not be empty, but
// may exceed maxRest and carry only a single record if maxRest is too small
// for that record.
func (b *Batch) Partition(maxFirst, maxRest int) []Batch {
	chunks := make([]Batch, 0, 10)
	in := b.lpData

	sumMetrics := 0
	si := 0
	if maxFirst <= 0 {
		maxFirst = 1
	}
	if maxRest <= 0 {
		maxRest = maxFirst
	}
	max := maxFirst
	ilen := len(in)
	for si < ilen {
		// Skip leading newlines.
		for si < ilen && in[si] == '\n' {
			si++
		}
		// Assume max chunk capped by input length.
		ei := si + max
		if ei > ilen {
			ei = ilen
		}
		if eio := strings.LastIndexByte(in[si:ei], 0x0a); eio > 0 {
			// Stop at the last newline in the chunk.  It can't be
			// at the start because we skipped those.
			ei = si + eio
		} else if eio := strings.IndexByte(in[ei:], 0x0a); eio >= 0 {
			// Stop at the first newline after max.  This chunk
			// exceeds its maximum.
			ei = si + max + eio
		} else {
			// There's a non-empty string that doesn't have a
			// newline.  That's not valid for a Batch lpData.
			panic(makeErrorBatch(ErrBatchNotTerminated, b))
		}

		eiv := ei
		// eiv metrics to newline but that may be preceded by
		// more newlines.  Back up to the start or the last
		// non-newline, then advance one to include the
		// newline that terminates the batch.
		for eiv > si && in[eiv-1] == '\n' {
			eiv--
		}
		eiv++

		// If the first batch would be oversized flush with an empty
		// batch and don't consume anything until we retry with
		// maxRest.  This prevents the batch, which will likely be
		// combined with other data, from exceeding the actual maximum.
		if (eiv-si) > max && len(chunks) == 0 {
			chunks = append(chunks, Batch{})
			max = maxRest
			continue
		}

		// If there's anything left, it's a chunk.  What follows isn't
		// the first chunk, so it uses a different max.
		if si < eiv {
			batch, _ := NewBatchFromLineProtocol(in[si:eiv], b.prec)
			sumMetrics += batch.NumMetrics()
			chunks = append(chunks, *batch)
			max = maxRest
		}
		// Skip past what we've examined
		si = ei + 1
	}
	if sumMetrics != b.numMetrics {
		panic(makeErrorBatch(ErrBatchMetricCount, b))
	}
	return chunks
}

type backlog struct {
	batchSize    int             // in runes, maximum
	batchMetrics int             // in pts, accumulating
	prec         time.Duration   // precision for encoded metrics
	sb           strings.Builder // holder for accumulating records
	encoder      *lp.Encoder     // converter bound to sb
	batches      []*Batch        // complete batches
	card         Cardinality     // pts and runes summed over complete batches
	cap          int             // in runes, overall backlog
}

func makeEncoder(sb *strings.Builder, prec time.Duration) *lp.Encoder {
	enc := lp.NewEncoder(sb)
	enc.SetFieldTypeSupport(lp.UintSupport)
	enc.SetPrecision(prec)
	enc.FailOnFieldErr(true)
	return enc
}

func newBacklog(bs, cap int, prec time.Duration) *backlog {
	rv := &backlog{
		batchSize: bs,
		cap:       cap,
		prec:      prec,
	}
	rv.encoder = makeEncoder(&rv.sb, prec)
	return rv
}

func (bl *backlog) String() string {
	return fmt.Sprintf("acc: %d (%d) of %d; cpl %d (%d) of %d in %d\n",
		bl.sb.Len(), bl.batchMetrics, bl.batchSize,
		bl.card.Runes, bl.card.Metrics, bl.cap, len(bl.batches))
}

// Cardinality for accumulating batch
func (bl *backlog) accCard() Cardinality {
	return Cardinality{
		Metrics: bl.batchMetrics,
		Runes:   bl.sb.Len(),
	}
}

// Total metrics and runes for the backlog, including accumulating.
func (bl *backlog) sizes() Cardinality {
	c := bl.accCard()
	c.Add(bl.card)
	return c
}

// Extract the pending metrics from the accumulating batch and return them as a
// Batch instance.  The accumulating batch remains unchanged.
func (bl *backlog) snapshotBatch() *Batch {
	ba := &Batch{
		numMetrics: bl.batchMetrics,
		lpData:     bl.sb.String(),
		prec:       bl.prec,
	}
	return ba
}

// Return the accumulating batch and reset for a new accumulation.
func (bl *backlog) finalizeBatch() *Batch {
	ba := bl.snapshotBatch()
	bl.batchMetrics = 0
	bl.sb.Reset()
	return ba
}

type metricChecker func(m lp.Metric) error

// Add a batch to the backlog, if this can be done without overflowing.
// inFlight represents the number of runes held by pending writes, which may
// need to be requeued.
//
// sawEmpty indicates that the accumlating batch was observed to be empty,
// either on entry or due to completing a new batch.
//
// unflushed indicates that on exit new metrics were added that remain in the
// accumulating batch.  When combined with empty this means the flush timer
// needs to be reset.
//
// err is non-nil only when adding the batch would result in overflowing the
// backlog.  In this case the error is ErrBacklogOverflow, no metrics are
// added, and sawEmpty and unflushed are to be ignored.
func (bl *backlog) addBatch(batch *Batch, inFlight int) (sawEmpty, unflushed bool, numMetricss int, err error) {
	sawEmpty = bl.batchMetrics == 0
	lpData := batch.lpData
	require := len(lpData)
	if require == 0 {
		return
	}
	if lpData[require-1] != '\n' {
		panic(makeErrorBatch(ErrBatchNotTerminated, batch))
	}

	sb := &bl.sb

	// Confirm room for what we'll add
	avail := bl.cap - (sb.Len() + bl.card.Runes + inFlight)
	if require > avail {
		err = fmt.Errorf("%w: %d of %d available, need %d", ErrBacklogOverflow, avail, bl.cap, require)
		return
	}

	// Fast-track the common case where the batch fits into the
	// accumulating area.
	if sb.Len()+require <= bl.batchSize {
		numMetricss = batch.numMetrics
		sb.WriteString(batch.lpData)
		bl.batchMetrics += numMetricss
		if sb.Len() == bl.batchSize {
			ba := bl.finalizeBatch()
			bl.queue(ba)
			sawEmpty = true
		}
		unflushed = bl.batchMetrics > 0
		return
	}

	var batches []*Batch // completed batches resulting from additions
	var card Cardinality // of completed batches

	part := batch.Partition(bl.batchSize-sb.Len(), bl.batchSize)
	for i, ba := range part {
		if !ba.Empty() {
			sb.WriteString(ba.lpData)
			bl.batchMetrics += ba.numMetrics
			numMetricss += ba.numMetrics
		}
		// If there are more batches, or this is the last one and it's
		// hit the limit, flush it.
		if i+1 < len(part) || sb.Len() >= bl.batchSize {
			sawEmpty = true
			ba := bl.finalizeBatch()
			batches = append(batches, ba)
			card.Add(ba.Cardinality())
		}
	}

	bl.batches = append(bl.batches, batches...)
	bl.card.Add(card)
	unflushed = bl.haveAccumulating()
	return
}

func (bl *backlog) haveAccumulating() bool {
	return bl.batchMetrics > 0
}

func (bl *backlog) haveComplete() bool {
	return len(bl.batches) > 0
}

func (bl *backlog) empty() bool {
	return !(bl.haveAccumulating() || bl.haveComplete())
}

func (bl *backlog) reset() {
	bl.sb.Reset()
	bl.batchMetrics = 0
	bl.batches = nil
	bl.card.Reset()
}

func (bl *backlog) flush() bool {
	if bl.batchMetrics == 0 {
		return false
	}
	bl.queue(bl.finalizeBatch())
	return true
}

// Add the batch to the end of the backlog and return it
func (bl *backlog) queue(batch *Batch) *Batch {
	bl.card.Add(batch.Cardinality())
	bl.batches = append(bl.batches, batch)
	return batch
}

// Pull batches out of the backlog up to the maximum batch size, and return a
// batch that contains the aggregate.
func (bl *backlog) dequeue() *Batch {
	if !bl.haveComplete() {
		return nil
	}

	// Aggregate batches up to the size limit
	batch := bl.batches[0]
	br := 1
	for br < len(bl.batches) {
		ok, _ := batch.Merge(*bl.batches[br], bl.batchSize)
		if !ok {
			break
		}
		br++
	}

	// Shift the remainder down to the front of the backlog, discard
	// references in what we're dropping, truncate the backlog, and update
	// its pt and rune counts.
	nr := len(bl.batches) - br
	if nr > 0 {
		copy(bl.batches[:nr], bl.batches[br:])
	}
	for i := nr; i < len(bl.batches); i++ {
		bl.batches[i] = nil
	}
	bl.batches = bl.batches[:nr]
	bl.card.Sub(batch.Cardinality())
	return batch
}

// Pull everything out of the backlog into a single batch, disregarding size
// limits.
func (bl *backlog) dequeueAll() *Batch {
	rv := &Batch{}
	bl.flush()
	if !bl.haveComplete() {
		return rv
	}

	var sb strings.Builder
	for _, batch := range bl.batches {
		sb.WriteString(batch.lpData)
	}
	rv = &Batch{
		numMetrics: bl.card.Metrics,
		prec:       bl.prec,
		lpData:     sb.String(),
	}

	bl.reset()

	return rv
}

type pendingWrite struct {
	batch  *Batch
	ctx    context.Context
	cancel context.CancelFunc
	err    error
}

// SchemaMap maps schema names to the corresponding schema.
type SchemaMap map[string]*Schema

// Bucket accesses information about a specific bucket, including its
// structure and data to be written to it.
//
// All public fields and methods in this type are safe for concurrent use.
type Bucket struct {
	log                 lw.Logger
	conn                *Connection
	name                string
	org                 string
	m                   sync.Mutex // protects sch
	sch                 SchemaMap
	flushInterval       time.Duration
	pendingLimit        int
	pendingTimeout      time.Duration
	allowUnknown        bool
	validateTags        bool
	bypassNormalization bool
	checkMetrics        bool
	state               BucketState
	wsc                 WriteSettledCallback
	prec                time.Duration
	backlog             *backlog
	pending             []*pendingWrite // metrics being transmitted
	stch                chan ConnectionState
	qbch                chan *queueBatch
	cmdch               chan *commandData
	shutdownOnce        sync.Once
}

type queueBatch struct {
	batch *Batch
	blch  chan BucketState
	err   error
}

func newBucket(c *Connection, cfg *svcInfluxCfg.Bucket, newLog lw.LogMaker) *Bucket {
	b := &Bucket{
		conn:                c,
		prec:                c.precision,
		name:                cfg.Name,
		org:                 cfg.Organization,
		sch:                 make(SchemaMap),
		flushInterval:       time.Duration(cfg.Options.FlushInterval),
		allowUnknown:        cfg.Options.AllowUnknown,
		pendingLimit:        cfg.Options.PendingLimit,
		pendingTimeout:      time.Duration(cfg.Options.PendingTimeout),
		validateTags:        cfg.Options.ValidateTags,
		bypassNormalization: cfg.Options.BypassNormalization,
		stch:                make(chan ConnectionState, 1),
		qbch:                make(chan *queueBatch, 1),
		cmdch:               make(chan *commandData),
	}
	// Record if there's any validation we need to do
	b.pending = make([]*pendingWrite, 0, b.pendingLimit)
	b.checkMetrics = !b.allowUnknown || b.validateTags || !b.bypassNormalization
	b.backlog = newBacklog(cfg.Options.BatchRuneCapacity, cfg.Options.HeldRuneCapacity, b.prec)

	c.bucket[b.name] = b
	for _, mc := range cfg.Measurements {
		b.newSchema(mc)
	}

	b.initializeState()
	b.log = newLog(b)
	return b
}

func (b *Bucket) initializeState() {
	b.state = BucketState{
		Bucket:            b,
		ConnState:         newConnectionState(),
		BatchRuneCapacity: b.backlog.batchSize,
		HeldRuneCapacity:  b.backlog.cap,
		PendingCapacity:   b.pendingLimit,
	}
	b.updateStateSizes()
}

// Capacity returns the capacity of the bucket in runes.
func (b *Bucket) Capacity() int {
	return b.backlog.cap
}

func (b *Bucket) updateStateSizes() {
	b.state.BatchSize = b.backlog.accCard()
	b.state.ReadySize = b.backlog.card
	b.state.ReadyBatchCount = len(b.backlog.batches)
	// Pending{Size,Count} maintained by main directly
	b.state.HeldSize = b.state.BatchSize
	b.state.HeldSize.Add(b.state.ReadySize)
	b.state.HeldSize.Add(b.state.PendingSize)
}

// Name returns the bucket name.
func (b *Bucket) Name() string {
	return b.name
}

// Organization returns the bucket organization.
func (b *Bucket) Organization() string {
	return b.org
}

func (s *Schema) initFieldSets() {
	for f, t := range s.fieldTypes {
		s.fields = s.fields.Add(f)
		if t == svcInfluxCfg.Integer {
			s.intFields = s.intFields.Add(f)
		} else if t == svcInfluxCfg.UInteger {
			s.uintFields = s.uintFields.Add(f)
		}
	}
}

func (b *Bucket) newSchema(cfg *svcInfluxCfg.Schema) *Schema {
	s := NewSchema(cfg)
	if b != nil {
		s.bkt = b
		b.m.Lock()
		b.sch[s.Name()] = s
		b.m.Unlock()
	}
	return s
}

func (b *Bucket) shutdown(cs *ConnectionState, ctx context.Context) BucketState {
	b.shutdownOnce.Do(func() {
		sdch := make(chan BucketState)
		sd := &shutdownData{
			ctx:  ctx,
			sdch: sdch,
		}
		cd := &commandData{
			cmd: cmdShutdown,
			prm: sd,
		}
		b.cmdch <- cd
		<-sd.sdch
	})
	return b.State()
}

// Set a callback used to inform the application of the disposition of
// batches successfully queued for transmission to the server.  This
// is the only data-specific indicator of success or failure, and is
// likely to be the initial indication of server communications
// problems (followed by a notification through a Connection state
// channel).
//
// Only one WriteSettledCallback is supported per bucket.
func (b *Bucket) SetWriteSettledCallback(wsc WriteSettledCallback) {
	cd := commandData{
		cmd: cmdSetWSC,
		prm: wsc,
	}
	cd.wg.Add(1)
	b.cmdch <- &cd
	cd.wg.Wait()
}

// FindSchema locates a schema with the given measurement name within the
// bucket.  If there is no measurement with the given name a nil pointer is
// returned.
func (b *Bucket) FindSchema(name string) *Schema {
	b.m.Lock()
	defer b.m.Unlock()
	return b.sch[name]
}

// Precision returns the timestamp precision used for all encoded metrics sent
// to this bucket.
func (b *Bucket) Precision() time.Duration {
	return b.prec
}

// MakeBatch converts the provided metrics into a line protocol batch.  The
// produced batch has no length limits, and will incorporate all metrics that
// aren't rejected.
//
// The returned RejectedMetrics map identifies submitted metrics that cannot
// be added to the batch, e.g. because they are malformed or do not pass
// schema validation.  The corresponding error explains the reason for
// rejection.  The map will be nil if no metrics were rejected.
//
// If the metric includes fields that would be rejected due to
// ErrMetricFieldValue errors, a metric that implements AddField will instead
// be updated to use the corrected values.
//
// NOTE: It is the responsibility of the caller to ensure that the Metric
// implementations produce sorted tags, if database performance is to be
// optimized.  Most metric constructors like lp.New() and write.NewPoint() do
// this for you.
func (b *Bucket) MakeBatch(m ...lp.Metric) (Batch, RejectedMetrics) {
	var checker metricChecker
	if b.checkMetrics {
		checker = b.checkMetric
	}

	var sb strings.Builder
	prec := b.prec
	enc := makeEncoder(&sb, prec)

	var rej RejectedMetrics
	npt := 0
	for _, m := range m {
		var err error
		if checker != nil {
			err = checker(m)
		}
		if err == nil {
			_, err = enc.Encode(m)
		}
		if err != nil {
			// Record the metric-specific error
			if rej == nil {
				rej = make(RejectedMetrics)
			}
			rej[m] = err
			continue
		}
		npt++
	}
	return Batch{
		numMetrics: npt,
		prec:       prec,
		lpData:     sb.String(),
	}, rej
}

// QueueBatch adds batch to the outgoing data.
//
// If adding the metrics would result in overflowing the bucket backlog no
// metrics are added, and ErrBacklogOverflow is returned.  The only other
// possible error return is ErrBucketTerminated.  The success or failure of
// writes in non-error invocations is communicated through the bucket's
// WriteSettledCallback.
//
// NOTE: The relationship between calls to QueueBatch and receipt of a
// WriteSettledCallback is many-to-one: Buckets aggregate queued batches until
// a size limit or hold time has been exceeded.
func (b *Bucket) QueueBatch(batch Batch) (BucketState, error) {
	qb := queueBatch{
		batch: &batch,
		blch:  make(chan BucketState),
	}
	b.qbch <- &qb
	bs := <-qb.blch
	return bs, qb.err
}

// FlushBatch moves the accumulating batch contents into the ready backlog for
// transmission as soon as possible.  The call returns before the flush is
// performed.
func (b *Bucket) FlushBatch() {
	b.cmdch <- &commandData{
		cmd: cmdFlush,
	}
}

// State returns a snapshot of the bucket state.
func (b *Bucket) State() BucketState {
	rv := BucketState{}
	cd := commandData{
		cmd: cmdState,
		prm: &rv,
	}
	cd.wg.Add(1)
	b.cmdch <- &cd
	cd.wg.Wait()
	return rv
}

// Drain invokes FlushBatch() then blocks until ctx is done or the bucket's
// backlog is zero.
func (b *Bucket) Drain(ctx context.Context) error {
	done := make(chan struct{})
	cd := commandData{
		cmd: cmdDrain,
		prm: done,
	}
	b.cmdch <- &cd
	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case <-done:
	}
	return err
}

// LogSetPriority changes the priority of logging by the bucket.
func (b *Bucket) LogSetPriority(pri lw.Priority) {
	cd := &commandData{
		cmd: cmdSetLogPriority,
		prm: pri,
	}
	cd.wg.Add(1)
	b.cmdch <- cd
	cd.wg.Wait()
}

// LogPriority returns priority of logging by the bucket.
func (b *Bucket) LogPriority() lw.Priority {
	cd := &commandData{
		cmd: cmdLogPriority,
	}
	cd.wg.Add(1)
	b.cmdch <- cd
	cd.wg.Wait()
	return cd.prm.(lw.Priority)
}

// Issue cancel operations for all in-progress writes
func (b *Bucket) cancelPending() {
	for _, pd := range b.pending {
		pd.cancel()
	}
}

func (b *Bucket) main(wg *sync.WaitGroup) {
	b.log.SetId(fmt.Sprintf("inflconn.%s/%s ", b.conn.id, b.name))
	lpr := lw.MakePriPr(b.log)

	lpr.N("started")

	bsp := &b.state

	var drainers []*commandData

	wrt := b.conn.Client().WriteAPIBlocking(b.org, b.name)

	flushTmr := time.NewTimer(0)
	var flushTmrC <-chan time.Time
	if !flushTmr.Stop() {
		<-flushTmr.C
	}

	pwch := make(chan *pendingWrite)

	allowWrites := true

	var sdChan chan BucketState
	var sdCtx context.Context
	var sdCtxDone <-chan struct{}

	stch := b.stch
	errTerminated := fmt.Errorf("%w: %s", ErrBucketTerminated, b.name)

	loop := sdCtx == nil
	for loop {
		flush := false
		select {
		case <-flushTmrC:
			lpr.D("flushTmr fired")
			flushTmrC = nil
			flush = true
		case cd := <-b.cmdch:
			lpr.I("cmd: %s", cd.cmd)
			switch cd.cmd {
			case cmdSetLogPriority:
				b.log.SetPriority(cd.prm.(lw.Priority))
				cd.wg.Done()
			case cmdLogPriority:
				cd.prm = b.log.Priority()
				cd.wg.Done()
			case cmdSetWSC:
				b.wsc = cd.prm.(WriteSettledCallback)
				cd.wg.Done()
			case cmdFlush:
				flush = true
			case cmdShutdown:
				if sdChan != nil {
					panic("shutdown invoked multiple times")
				}
				sd := cd.prm.(*shutdownData)
				sdCtx = sd.ctx
				if sdCtx.Err() != nil {
					lpr.N("shutdown: immediate")
					b.cancelPending()
					allowWrites = false
				} else {
					lpr.N("shutdown: pending ctx")
				}
				sdCtxDone = sdCtx.Done()
				sdChan = sd.sdch
				flush = true
			case cmdDrain:
				flush = true
				drainers = append(drainers, cd)
			case cmdState:
				rp := cd.prm.(*BucketState)
				*rp = *bsp
				cd.wg.Done()
			}
		case qb := <-b.qbch:
			var sawEmpty, unflushed bool
			var npts int
			var err error
			// Reject new queue operation if we're shutting down.
			if sdCtx != nil {
				err = errTerminated
			} else {
				sawEmpty, unflushed, npts, err = b.backlog.addBatch(qb.batch, bsp.PendingSize.Runes)
			}
			bsp.MetricsSubmitted += npts
			lpr.D("qb %s %v", qb.batch.Cardinality(), err)
			if err == nil {
				b.updateStateSizes()
				if sawEmpty {
					if flushTmrC != nil && !flushTmr.Stop() {
						<-flushTmr.C
					}
					flushTmrC = nil
					if unflushed {
						flushTmr.Reset(b.flushInterval)
						flushTmrC = flushTmr.C
						lpr.D("flushTmr reset %s", b.flushInterval)
					}
				}
			}
			qb.err = err
			qb.blch <- *bsp
		case <-sdCtxDone:
			sdCtxDone = nil
			lpr.N("shutCtx cancelling %d: %s", len(b.pending), sdCtx.Err().Error())
			b.cancelPending()
			allowWrites = false
		case cs, ok := <-stch:
			if ok {
				lpr.I("state: %s", cs)
				bsp.ConnState = cs
			} else {
				lpr.D("state: channel closed")
				stch = nil
			}
		case pd := <-pwch:
			// Remove the batch from Pending.
			lpr.D("removed pending %s", pd.batch.Cardinality())
			bsp.PendingSize.Sub(pd.batch.Cardinality())
			li := len(b.pending) - 1
			for i, v := range b.pending {
				if pd == v {
					if i < li {
						b.pending[i] = b.pending[li]
					}
					b.pending[li] = nil
					b.pending = b.pending[:li]
					break
				}
			}
			bsp.PendingCount = len(b.pending)

			err := pd.err

			if err != nil {
				// If the write failed due to a network or
				// http error then trigger a recheck of the
				// connection status.
				var nerr net.Error
				var herr *http2.Error
				if errors.As(err, &nerr) || errors.As(err, &herr) {
					b.conn.checkConnection(false)
				}

				// If the bucket is terminating tell the
				// application so it doesn't try to resubmit
				// the batch.
				if sdCtx != nil {
					err = &errBucketTerminated{
						parent: errTerminated,
						err:    err,
					}
				}

			}
			if dropped := b.handleWriteSettled(pd.batch, err); dropped != 0 {
				lpr.I("write: dropped %d: %v", dropped, err)
			}
		}

		if flush && b.backlog.flush() {
			b.updateStateSizes()
			if flushTmrC != nil && !flushTmr.Stop() {
				<-flushTmr.C
			}
			flushTmrC = nil
		}

		// Send from the backlog as long as the connection's good,
		// there's a backlog, there're write resources available, and
		// we're allowing new writes.
		for bsp.ConnState.Ready() && b.backlog.haveComplete() && len(b.pending) < cap(b.pending) && allowWrites {
			batch := b.backlog.dequeue()
			lpr.D("writing %s", batch.Cardinality())
			pd := &pendingWrite{
				batch: batch,
			}
			b.pending = append(b.pending, pd)
			bsp.PendingSize.Add(pd.batch.Cardinality())
			bsp.PendingCount = len(b.pending)
			b.updateStateSizes()
			pd.ctx, pd.cancel = context.WithTimeout(context.Background(), b.pendingTimeout)
			go b.writePending(wrt, pd, pwch)
		}

		// If there are no pending writes we can check for loop termination
		if len(b.pending) == 0 {
			// If the backlog is empty we need to notify any
			// drainers.
			if b.backlog.empty() && len(drainers) > 0 {
				for _, bc := range drainers {
					done := bc.prm.(chan struct{})
					close(done)
				}
				drainers = nil
			}
			// If we're shutting down we've done everything that
			// requires the loop.
			if sdCtx != nil {
				loop = false
			}
		}
	}

	lpr.I("exiting: %s", bsp)
	bsp.Terminated = true

	// Send the remaining backlog back to the application.
	batch := b.backlog.dequeueAll()
	dropped := b.handleWriteSettled(batch, errTerminated)
	lpr.I("terminal notification of %s %d\n%s", batch.Cardinality(), dropped, bsp)

	// Leave something that will fail attempts to queue data to terminated
	// buckets.
	go func(b *Bucket, err error) {
		for {
			select {
			case qb := <-b.qbch:
				qb.err = err
				qb.blch <- b.state
			case cd := <-b.cmdch:
				lpr.I("cmd-post-shutdown: %s", cd.cmd)
				switch cd.cmd {
				case cmdDrain:
					close(cd.prm.(chan struct{}))
				case cmdFlush:
					// no action required
				case cmdLogPriority:
					cd.prm = b.log.Priority()
					cd.wg.Done()
				case cmdSetLogPriority:
					// Changing the log priority
					// introduces a race condition with
					// the main goroutine if it hasn't
					// terminated yet.  So this does
					// nothing.
					cd.wg.Done()
				case cmdSetWSC:
					cd.wg.Done()
				case cmdState:
					rp := cd.prm.(*BucketState)
					*rp = *bsp
					cd.wg.Done()
				default:
				}
			}
		}
	}(b, errTerminated)

	sdChan <- *bsp
	wg.Done()
	lpr.N("exited")
}

func (b *Bucket) writePending(wrt api.WriteAPIBlocking, pd *pendingWrite, pwch chan<- *pendingWrite) {
	pd.err = wrt.WriteRecord(pd.ctx, pd.batch.lpData)
	pwch <- pd
}

func (b *Bucket) handleWriteSettled(batch *Batch, err error) int {
	bsp := &b.state
	dropped := 0
	if err == nil {
		bsp.MetricsWritten += batch.numMetrics
	} else {
		dropped = batch.numMetrics
		bsp.MetricsDropped += dropped
	}
	b.updateStateSizes()
	if b.wsc != nil {
		b.wsc(*batch, err, *bsp)
	}
	return dropped
}

func (b *Bucket) checkMetric(m lp.Metric) error {
	sch := b.FindSchema(m.Name())
	if sch == nil {
		if b.allowUnknown {
			return nil
		}
		if nm := m.Name(); nm != "" {
			return fmt.Errorf("%w: %s", ErrMetricUnknown, nm)
		}
		return ErrMetricUnknown
	}

	if b.validateTags {
		if err := sch.ValidateMetric(m); err != nil {
			return err
		}
	}

	if !b.bypassNormalization {
		if upd := sch.NormalizeMetric(m); upd != nil {
			if um, ok := m.(AddField); ok {
				for _, f := range upd {
					um.AddField(f.Key, f.Value)
				}
			} else {
				return &ErrorMetricFieldValue{
					Metric:  m,
					Updates: upd,
				}
			}
		}
	}
	return nil
}

// Schema holds information about a specific measurement recorded in a bucket,
// including requirements used to validate metrics against the schema before
// they're written.  Such requirements may include verifying that all required
// tags are present, any remaining tags are in the optional tags set, and that
// field values have the correct data type.
type Schema struct {
	bkt        *Bucket
	name       string
	reqTags    set.Set[string]
	optTags    set.Set[string]
	fieldTypes map[string]svcInfluxCfg.FieldType
	fields     set.Set[string]
	intFields  set.Set[string]
	uintFields set.Set[string]
}

// NewSchema constructs a Schema from its configuration structure without
// associating it with a bucket.  Such a schema allows for validation of
// metrics independent of a complete influxdb configuration.
func NewSchema(cfg *svcInfluxCfg.Schema) *Schema {
	s := &Schema{
		name:       cfg.Name,
		reqTags:    set.MakeSet[string](cfg.RequiredTags...),
		fieldTypes: make(map[string]svcInfluxCfg.FieldType, len(cfg.Fields)),
	}
	if s.reqTags != nil {
		s.optTags = set.MakeSet[string](cfg.OptionalTags...)
	}
	for f, t := range cfg.Fields {
		s.fieldTypes[f] = t
	}
	s.initFieldSets()
	return s
}

// Schemas returns a map for the Schema objects in the bucket.
func (b *Bucket) Schemas() SchemaMap {
	b.m.Lock()
	rv := make(SchemaMap, len(b.sch))
	for k, v := range b.sch {
		rv[k] = v
	}
	b.m.Unlock()
	return rv
}

// Name returns the string used as the measurement identifier for data
// described by the schema.
func (s *Schema) Name() string {
	return s.name
}

// ValidateMetric ensures that the measurement name is correct, all required
// tags are present, any additional tags are from the optional set, and if
// fields are defined that the metric contains only defined fields.
//
// Validation does not check the types of fields, which are assumed to be
// consistent with the InfluxDB line protocol data types.
func (s *Schema) ValidateMetric(m lp.Metric) error {
	if m == nil {
		return ErrMetricNil
	}
	if m.Name() != s.Name() {
		return fmt.Errorf("%w: %s not %s", ErrMetricMeasurement, m.Name(), s.Name())
	}
	if s.reqTags != nil {
		tl := m.TagList()
		ht := make(set.Set[string], len(tl))
		for _, tp := range tl {
			ht = ht.Add(tp.Key)
		}
		mt := s.reqTags.Minus(ht)
		if len(mt) != 0 {
			se := mt.Elements()
			sort.Strings(se)
			return fmt.Errorf("%w: %s", ErrMetricTagsMissing,
				strings.Join(se, " "))
		}
		ut := ht.Minus(s.reqTags).Minus(s.optTags)
		if len(ut) != 0 {
			ue := ut.Elements()
			sort.Strings(ue)
			return fmt.Errorf("%w: %s", ErrMetricTagsUnknown,
				strings.Join(ue, " "))
		}
	}
	if s.fields != nil {
		fl := m.FieldList()
		hf := make(set.Set[string], len(fl))
		for _, fp := range fl {
			hf = hf.Add(fp.Key)
		}
		uf := hf.Minus(s.fields)
		if len(uf) != 0 {
			ue := uf.Elements()
			sort.Strings(ue)
			return fmt.Errorf("%w: %s", ErrMetricFieldsUnknown,
				strings.Join(ue, " "))
		}
	}
	return nil
}

// NormalizeMetric checks that field values are consistent with line protocol
// type required for that field by the schema.
//
// The implementation may assume that field values have been converted to the
// standard Go type associated with a specific InfluxDB line protocol data
// type: for example all floating point values are float64, and all signed
// integer values are int64.  This may be done for you when using metric
// constructors like lp.New() or write.NewPoint().
//
// The return value will be nil if no fields require conversion.
//
// NOTE: At this time the checks are only for values of type float64 in fields
// that are required to be Integer (int64) or UInteger (uint64).  The Value in
// the returned lp.Field is the float64 converted to the required integral
// type, regardless of the effects of integral truncation or overflow.
func (s *Schema) NormalizeMetric(m lp.Metric) (updates []*lp.Field) {
	if s.intFields == nil && s.uintFields == nil {
		return
	}

	for _, f := range m.FieldList() {
		if v, ok := f.Value.(float64); ok {
			if s.intFields.Has(f.Key) {
				updates = append(updates, &lp.Field{
					Key:   f.Key,
					Value: int64(v),
				})
			} else if s.uintFields.Has(f.Key) {
				updates = append(updates, &lp.Field{
					Key:   f.Key,
					Value: uint64(v),
				})
			}
		}
	}
	return
}

// BucketSchemaMap is a map from bucket names to a map from schema names to
// schema that belong to the bucket.  This can be used for validation in
// applications that construct metrics but delegate writing to Influx to
// another process.
type BucketSchemaMap map[string]SchemaMap

// BucketSchemaMapFromConfig constructs a BucketSchemaMap from the given
// configuration structure.
func BucketSchemaMapFromConfig(cfg svcInfluxCfg.BucketSchemaMap) (BucketSchemaMap, error) {
	rv := make(map[string]SchemaMap, len(cfg))
	for bk, csm := range cfg {
		if bk == "" {
			return nil, fmt.Errorf("BucketSchemaMap: %w", ErrBucketSchemaMapKey)
		}
		sm := make(SchemaMap, len(csm))
		rv[bk] = sm
		for sk, sc := range csm {
			if sk == "" {
				return nil, fmt.Errorf("BucketSchemaMap: %s: %w", bk, ErrBucketSchemaMapSchema)
			}
			sc.Name = sk
			sm[sk] = NewSchema(sc)
		}
	}
	return rv, nil
}

// BucketSchemaMapFromConfig constructs a BucketSchemaMap from the given
// configuration structure expressed as JSON.
func BucketSchemaMapFromJSON(raw []byte) (BucketSchemaMap, error) {
	var cfg svcInfluxCfg.BucketSchemaMap
	err := json.Unmarshal(raw, &cfg)
	if err != nil {
		return nil, err
	}
	return BucketSchemaMapFromConfig(cfg)
}

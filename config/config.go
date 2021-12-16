// Copyright 2021-2022 Peter Bigot Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

// Package config holds types used to construct the runtime state objects for
// the influxdb service.
package config

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var (
	ErrConnection         = errors.New("connection")
	ErrBucketName         = errors.New("inconsistent bucket name")
	ErrBucketOrganization = errors.New("no organization for bucket")
	ErrConfig             = errors.New("invalid configuratation")
	ErrMeasurementName    = errors.New("invalid measurement name")
	ErrDurationInvalid    = errors.New("invalid duration text")
	ErrFieldType          = errors.New("unrecognized field type")

	// BackOffStop is the flag value to be returned by
	// BackOff.NextBackOff() to stop retrying.
	BackOffStop = time.Duration(-1)
)

// BackOff is the interface for injecting a back-off policy.  It conforms in
// all ways to the same interface from github.com/cenkalti/backoff/v4.
type BackOff interface {
	// NextBackOff returns the duration to wait before retrying.  Return
	// BackOffStop to stop retrying.
	NextBackOff() time.Duration

	// Reset the policy.
	Reset()
}

// Connection describes how to contact the InfluxDB server, provides
// authentication information, and outlines the structure of the buckets and
// measurements to be accessed.
type Connection struct {
	// Identifier used in log messages
	Id string

	// URL to the InfluxDB server.
	URL string

	// Organization associated with the buckets accessed through the
	// Connection.  Used for buckets that don't provide one of their own.
	Organization string `yaml:",omitempty"`

	// Token to use for connection.
	Token string `yaml:",omitempty"`

	// Buckets accessed through the Connection, indexed by bucket name.
	Buckets map[string]*Bucket

	// Specifies the precision for timestamps in metrics written through
	// the connection.  Accepted values are 1 s, 1 ms, 1 us, and 1 ns.
	// Default is 1 ns.
	Precision Duration `yaml:",omitempty"`

	// Specifies that payloads to/from the server may be compressed.
	UseGZip bool `yaml:"use-gzip,omitempty"`

	// BucketOptions provides a default set of options for buckets that
	// don't provide one.
	DefaultBucketOptions *BucketOptions `yaml:"default-bucket-options,omitempty"`

	// ReadyCheckTimeout indicates how long a connection attempt can remain
	// incomplete before it is cancelled.  Minimum is 500 ms.  Default is
	// 1 s.
	ReadyCheckTimeout Duration `yaml:"ready-check-timeout,omitempty"`

	// ReadyCheckInterval supports a default back-off policy where the
	// connection is retried at the specified interval forever.  Minimum
	// is ReadyCheckTimeout plus 1 s.  Default is ReadyCheckTimeout plus 5 s.
	ReadyCheckInterval Duration `yaml:"ready-check-interval,omitempty"`

	// BackOff allows injecting a back-off policy to use for reconnection
	// attempts.  If left nil the Connection will be given an
	// implementation equivalent to NewConstantBackoff(ReadyCheckInterval) from
	// github.com/cenkalti/backoff/v4.
	RetryPolicy BackOff `yaml:"-"`
}

// ValidatePrecision confirms that the provided precision is supported by line
// protocol, ie. 1s, 1ms, 1us, or 1ns.
func ValidatePrecision(prec time.Duration) (err error) {
	switch time.Duration(prec) {
	default:
		err = fmt.Errorf("%v must be 1s, 1ms, 1us, or 1ns", prec)
	case time.Nanosecond:
	case time.Microsecond:
	case time.Millisecond:
	case time.Second:
	}
	return err
}

type fixedBackoff struct {
	dur time.Duration
}

func (fb *fixedBackoff) NextBackOff() time.Duration {
	return fb.dur
}
func (fb *fixedBackoff) Reset() {}

// Validate confirms that the Connection deep structure has all required
// information without inconsistencies.
func (conn *Connection) Validate() error {
	if conn == nil {
		return fmt.Errorf("%w: nil", ErrConnection)
	}
	if conn.Id == "" {
		return fmt.Errorf("%w: id empty", ErrConnection)
	}
	if conn.DefaultBucketOptions != nil {
		if err := conn.DefaultBucketOptions.Validate(true); err != nil {
			return fmt.Errorf("%w in DefaultBucketOptions", err)
		}
	}
	if conn.Precision == Duration(0) {
		conn.Precision = Duration(time.Nanosecond)
	} else if err := ValidatePrecision(time.Duration(conn.Precision)); err != nil {
		return fmt.Errorf("%w: Precision %s restricted to 1s, 1ms, 1us, or 1ns",
			ErrConfig, conn.Precision)
	}

	rt := time.Duration(conn.ReadyCheckTimeout)
	mt := 500 * time.Millisecond
	if rt < 0 || (rt > 0 && rt < mt) {
		return fmt.Errorf("%w: ReadyCheckTimeout (%s) must be at least %v",
			ErrConfig, rt, mt)
	} else if rt == 0 {
		rt = time.Second
	}
	conn.ReadyCheckTimeout = Duration(rt)

	ri := time.Duration(conn.ReadyCheckInterval)
	mi := rt + time.Second
	if ri < 0 || (ri > 0 && ri < mi) {
		return fmt.Errorf("%w: ReadyCheckInterval (%s) must be at least %v",
			ErrConfig, ri, mi)
	} else if ri == 0 {
		ri = 5*time.Second + rt
	}
	conn.ReadyCheckInterval = Duration(ri)
	if conn.RetryPolicy == nil {
		conn.RetryPolicy = &fixedBackoff{
			dur: ri,
		}
	}

	for k, b := range conn.Buckets {
		if b == nil {
			b = &Bucket{}
			conn.Buckets[k] = b
		}
		if b.Name == "" {
			b.Name = k
		} else if b.Name != k {
			return fmt.Errorf("%w: %s under key %s", ErrBucketName,
				b.Name, k)
		}
		if b.Organization == "" {
			b.Organization = conn.Organization
			if b.Organization == "" {
				return fmt.Errorf("%w: %s", ErrBucketOrganization, b.Name)
			}
		}
		if b.Options == nil {
			var opt BucketOptions
			if dop := conn.DefaultBucketOptions; dop != nil {
				opt = *dop
			}
			b.Options = &opt
		}
		if err := b.Options.Validate(true); err != nil {
			return fmt.Errorf("%w in %s", err, b.Name)
		}
		for k, ms := range b.Measurements {
			if k == "" {
				return fmt.Errorf("%w: empty", ErrMeasurementName)
			}
			if ms == nil {
				ms = &Schema{}
				b.Measurements[k] = ms
			}
			if ms.Name == "" {
				ms.Name = k
			} else if ms.Name != k {
				return fmt.Errorf("%w: %s under key %s",
					ErrMeasurementName, ms.Name, k)
			}
		}
	}
	return nil
}

// Bucket describes buckets that are accessed through a connection.
type Bucket struct {
	// Name of the bucket.  When unmarshalled this is inferred from the
	// map key.
	Name string `yaml:"-"`

	// Organization for bucket.  If empty the organization from the parent
	// Connection will be used.
	Organization string `yaml:",omitempty"`

	// Measurements provides expectations about measurements stored in the
	// bucket.
	Measurements map[string]*Schema

	// Options controls behavior of the bucket interface.
	Options *BucketOptions `yaml:",omitempty"`
}

// Duration is an extension of time.Duration that decodes non-negative integer
// values without units as durations in milliseconds, and strings as with
// time.ParseDuration.
type Duration time.Duration

func (d *Duration) UnmarshalText(text []byte) error {
	s := string(text)
	td, err := time.ParseDuration(s)
	if err != nil && strings.Contains(err.Error(), "time: missing unit") {
		var i int
		i, err = strconv.Atoi(s)
		if err == nil {
			td = time.Duration(i) * time.Millisecond
		}
	}
	if err == nil {
		if td <= 0*time.Millisecond {
			err = fmt.Errorf("%w: %s", ErrDurationInvalid, s)
		} else {
			*d = Duration(td)
		}
	}
	return err
}

func (d Duration) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

func (d Duration) String() string {
	return time.Duration(d).String()
}

// BucketOptions controls how the bucket write behaves.
type BucketOptions struct {
	// FlushInterval is the maximum time a point will remain in the
	// accumulating batch before the batch is flushed to the backlog for
	// transmission to the server.  Minimum is 1 ms.  Default is 1 s.
	FlushInterval Duration `yaml:"flush-interval,omitempty"`

	// BatchRuneCapacity is the maximum length of the line protocol encoded
	// sequence of points allowed to be aggregated before an attempt is
	// made to transmit the batch to the server.  It is also the nominal
	// length of line encoded points sent to the server per call when
	// processing the backlog.  Default is 256 KiBy.
	BatchRuneCapacity int `yaml:"batch-size,omitempty"`

	// HeldRuneCapacity is the maximum length in runes of line-protocol
	// encoded point data allowed to be retained by the bucket for
	// transmission.  This includes accumulating metrics, batches of ready
	// metrics, and metrics that are in the process of being sent to the
	// server.  Default is 2 MiBy.
	HeldRuneCapacity int `yaml:"rune-capacity,omitempty"`

	// AllowUnknown when true allows encoding of metrics with measurements
	// that have no schema.  When false or absent such metrics are
	// rejected.
	AllowUnknown bool `yaml:"allow-unknown,omitempty"`

	// ValidateTags ensures that every point for which there is a
	// measurement schema satisfies the tags requirement for the schema.
	ValidateTags bool `yaml:"validate-tags,omitempty"`

	// BypassNormalization instructs the system to not attempt to convert
	// field values to the types specified for those fields in the
	// measurement schema.
	BypassNormalization bool `yaml:"bypass-normalization,omitempty"`

	// PendingLimit is the number of in-flight writes allowed at any time.
	// Default is 1.
	PendingLimit int `yaml:"pending-limit,omitempty"`

	// PendingTimeout specifies the maximum time to wait for a batch being
	// sent to the server to settle.  Writes that exceed the deadline will
	// fail.  Minimum is 1 s.  Default is 10 s.
	PendingTimeout Duration `yaml:"pending-deadline,omitempty"`
}

var defaultBucketOptions = BucketOptions{
	FlushInterval:     Duration(time.Second),
	BatchRuneCapacity: 256 * 1024,
	HeldRuneCapacity:  2 * 1024 * 1024,
	PendingLimit:      1,
	PendingTimeout:    Duration(10 * time.Second),
}

// DefaultBucketOptions returns a BucketOptions with all fields initialized to
// their default value.
func DefaultBucketOptions() BucketOptions {
	return defaultBucketOptions
}

// Validate checks the values in the given object, updating values that have
// not been initialized to their default if the zero value is not a valid
// value.
func (o *BucketOptions) Validate(useDefaults bool) error {
	if useDefaults {
		if o.FlushInterval == Duration(0) {
			o.FlushInterval = defaultBucketOptions.FlushInterval
		}
		if o.BatchRuneCapacity == 0 {
			o.BatchRuneCapacity = defaultBucketOptions.BatchRuneCapacity
		}
		if o.HeldRuneCapacity == 0 {
			o.HeldRuneCapacity = defaultBucketOptions.HeldRuneCapacity
		}
		if o.PendingLimit == 0 {
			o.PendingLimit = defaultBucketOptions.PendingLimit
		}
		if o.PendingTimeout == Duration(0) {
			o.PendingTimeout = defaultBucketOptions.PendingTimeout
		}
	}
	if o.FlushInterval < Duration(time.Millisecond) {
		return fmt.Errorf("%w: FlushInterval (%s) < 1 ms",
			ErrConfig, o.FlushInterval)
	}
	if o.BatchRuneCapacity <= 0 {
		return fmt.Errorf("%w: BatchRuneCapacity (%d) must be positive",
			ErrConfig, o.BatchRuneCapacity)
	}
	if o.HeldRuneCapacity < o.BatchRuneCapacity {
		return fmt.Errorf("%w: HeldRuneCapacity (%d) must be at least BatchRuneCapacity (%d)",
			ErrConfig, o.HeldRuneCapacity, o.BatchRuneCapacity)
	}
	if o.PendingTimeout < Duration(time.Second) {
		return fmt.Errorf("%w: PendingTimeout (%s) < 1 s",
			ErrConfig, o.PendingTimeout)
	}
	return nil
}

// FieldType specifies the InfluxDB Line Protocol type expected to be
// associated with values for a given field.
//
// If a metric constructor like lp.New() is used the Go types for field values
// will be normalized to the specified Go equivalent.  Other metric
// constructors should do the same.
type FieldType int8

const (
	undefinedType FieldType = iota
	// Float identifies the InfluxDB IEEE-754 64-bit floating point type.
	// Go equivalent is float64.
	Float
	// Integer identifies the InfluxDB 64-bit signed integer type.  Go
	// equivalent is int64.
	Integer
	// UInteger identifies the InfluxDB 64-bit unsigned integer type.  Go
	// equivalent is uint64.
	UInteger
	// String identifies the InfluxDB string type.  Go equivalent is
	// string
	String
	// Boolean identifies the InfluxDB boolean type.  Go equivalent is
	// bool
	Boolean
)

func (f FieldType) String() string {
	switch f {
	case undefinedType:
		return "?"
	case Float:
		return "float"
	case Integer:
		return "integer"
	case UInteger:
		return "uinteger"
	case String:
		return "string"
	case Boolean:
		return "boolean"
	}
	panic("unknown FieldType")
}

// ParseFieldType a string representation of an InfluxDB field type to the
// internal constant type identifier.  Matches are based on lower case
// identifier equivalence.
func ParseFieldType(s string) (ft FieldType, ok bool) {
	ok = true
	switch strings.ToLower(s) {
	case "float":
		ft = Float
	case "integer":
		ft = Integer
	case "uinteger":
		ft = UInteger
	case "string":
		ft = String
	case "boolean":
		ft = Boolean
	default:
		ok = false
	}
	return
}

func (ft *FieldType) UnmarshalYAML(unmarshal func(v interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	if v, ok := ParseFieldType(s); ok {
		*ft = v
		return nil
	}
	return fmt.Errorf("%w: %s", ErrFieldType, s)
}

func (ft FieldType) MarshalYAML() (interface{}, error) {
	return ft.String(), nil
}

// Schema describes the structure of metrics associated with a measurement.
type Schema struct {
	// Name of the measurement.  This is inferred from the map key.
	Name string `yaml:"-"`

	// RequiredTags lists tag keys used when validating a metric.  Metric
	// tags are not validated if this set is empty.  When required tags
	// are identified, metrics that lack any of them will fail validation.
	RequiredTags []string `yaml:"required-tags,flow,omitempty"`

	// OptionalTags lists tag keys used when validating metric tags.
	// Metrics may but do not have to include any or all of these tags.
	// Metrics that have tags that are neither in this set nor in
	// RequiredTags will fail validation.
	OptionalTags []string `yaml:"optional-tags,flow,omitempty"`

	// Fields provides types for values associated with each field.  These
	// are used to convert field values into the type required by the
	// schema, e.g. float64 values from JSON numbers into Integer or
	// UInteger values.
	//
	// If no fields are provided, metric fields are not validated.  If
	// fields are provided, metrics will be fail validation if they
	// include field keys not present in this map.
	Fields map[string]FieldType `yaml:",omitempty"`
}

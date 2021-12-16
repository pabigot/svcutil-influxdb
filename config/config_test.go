// Copyright 2021-2022 Peter Bigot Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pabigot/edcode"
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

func TestValidateConnection(t *testing.T) {
	var cp *Connection
	confirmError(t, cp.Validate(), ErrConnection, ": nil")

	cp = &Connection{
		Organization: "o",
		// missing id
		DefaultBucketOptions: &BucketOptions{
			BatchRuneCapacity: 100,
			HeldRuneCapacity:  50, // must be at least BatchRuneCapacity
		},
		Precision: edcode.Duration(5 * time.Second),
		Buckets: map[string]*Bucket{
			"b": nil,
			"b2": {
				Measurements: SchemaMap{
					"m": nil,
				},
				Options: &BucketOptions{
					FlushInterval: edcode.Duration(time.Nanosecond), // invalid
				},
			},
		},
	}
	confirmError(t, cp.Validate(), ErrConnection, "id empty")
	cp.Id = "i"

	confirmError(t, cp.Validate(), ErrConfig,
		"HeldRuneCapacity (50) must be at least BatchRuneCapacity (100) in DefaultBucketOptions")
	cp.DefaultBucketOptions.HeldRuneCapacity = 200

	confirmError(t, cp.Validate(), ErrConfig,
		"Precision 5s restricted to 1s, 1ms, 1us, or 1ns")
	cp.Precision = edcode.Duration(time.Nanosecond)

	cp.ReadyCheckTimeout = edcode.Duration(-1 * time.Second)
	confirmError(t, cp.Validate(), ErrConfig,
		"ReadyCheckTimeout (-1s) must be at least 500ms")
	cp.ReadyCheckTimeout = edcode.Duration(time.Nanosecond)
	confirmError(t, cp.Validate(), ErrConfig,
		"ReadyCheckTimeout (1ns) must be at least 500ms")
	cp.ReadyCheckTimeout = 0

	cp.ReadyCheckInterval = edcode.Duration(-1 * time.Second)
	confirmError(t, cp.Validate(), ErrConfig,
		"ReadyCheckInterval (-1s) must be at least 2s")
	cp.ReadyCheckInterval = edcode.Duration(time.Duration(cp.ReadyCheckTimeout) - 500*time.Millisecond)
	confirmError(t, cp.Validate(), ErrConfig,
		"ReadyCheckInterval (500ms) must be at least 2s")
	cp.ReadyCheckInterval = 0

	b2, ok := cp.Buckets["b2"]
	if !ok {
		t.Fatal("b2 not found")
	}
	confirmError(t, cp.Validate(), ErrConfig,
		"FlushInterval (1ns) < 1 ms in b2")
	b2.Options.FlushInterval = edcode.Duration(0)

	if err := cp.Validate(); err != nil {
		t.Fatal(err)
	}
	if rt := time.Duration(cp.ReadyCheckTimeout); rt != time.Second {
		t.Errorf("ReadyCheckInterval wrong: %s", rt)
	}
	if ri := time.Duration(cp.ReadyCheckInterval); ri != 6*time.Second {
		t.Errorf("ReadyCheckTimeout wrong: %s", ri)
	}
	if cp.RetryPolicy == nil {
		t.Errorf("RetryPolicy not inferred")
	} else if nbo := cp.RetryPolicy.NextBackOff(); nbo != time.Duration(cp.ReadyCheckInterval) {
		t.Errorf("RetryPolicy gives wrong interval: %s", nbo)
	}

	if d := b2.Options.FlushInterval; d != defaultBucketOptions.FlushInterval {
		t.Errorf("b2.Options.FlushInterval wrong: %s", d)
	}

	b, ok := cp.Buckets["b"]
	if !ok {
		t.Fatal("b not found")
	}
	if op := b.Options; op == nil {
		t.Fatal("b lacks options")
	}
	if !reflect.DeepEqual(b.Options, cp.DefaultBucketOptions) {
		t.Fatal("b option inference")
	}

	if op := b2.Options; op == nil {
		t.Fatal("b2 lacks options")
	}

	m, ok := b2.Measurements["m"]
	if !ok {
		t.Fatal("b2.m not found")
	}
	if m == nil {
		t.Fatal("b2.m not constructed")
	}
}

func TestValidateBucketName(t *testing.T) {
	ms := SchemaMap{
		"m": {},
	}
	cp := &Connection{
		Id:           "i",
		Organization: "o",
		Buckets: map[string]*Bucket{
			"b": {
				Measurements: ms,
			},
		},
	}
	if err := cp.Validate(); err != nil {
		t.Fatal(err)
	}
	if cp.Buckets["b"].Name != "b" {
		t.Error("Name not inferred")
	}

	cp.Buckets["b"] = &Bucket{
		Name:         "b",
		Measurements: ms,
	}
	if err := cp.Validate(); err != nil {
		t.Fatal(err)
	}

	cp.Buckets["b"] = &Bucket{Name: "b2"}
	confirmError(t, cp.Validate(), ErrBucketName,
		"inconsistent bucket name: b2 under key b")
}

func TestValidateBucketOrganization(t *testing.T) {
	organization := "MyOrganization"
	ms := SchemaMap{
		"m": {},
	}
	cp := &Connection{
		Id: "i",
		Buckets: map[string]*Bucket{
			"b": {},
		},
	}
	confirmError(t, cp.Validate(), ErrBucketOrganization, "no organization for bucket: b")

	cp.Buckets["b"] = &Bucket{
		Organization: organization,
	}
	if err := cp.Validate(); err != nil {
		t.Errorf("expected ok for no measurements")
	}

	cp.Buckets["b"].Measurements = ms
	if err := cp.Validate(); err != nil {
		t.Fatal(err)
	}
	if cp.Buckets["b"].Organization != organization {
		t.Error("Organization not applied")
	}

	cp.Organization = organization
	cp.Buckets["b"] = &Bucket{
		Measurements: ms,
	}
	if err := cp.Validate(); err != nil {
		t.Fatal(err)
	}
	if cp.Buckets["b"].Organization != organization {
		t.Error("Organization not inferred")
	}
}

func TestValidateMeasurementName(t *testing.T) {
	cp := &Connection{
		Id: "i",
		Buckets: map[string]*Bucket{
			"b": {
				Organization: "org",
			},
		},
	}
	b := cp.Buckets["b"]
	b.Measurements = make(SchemaMap)

	b.Measurements[""] = &Schema{}
	confirmError(t, cp.Validate(), ErrMeasurementName, "invalid measurement name: empty")

	delete(b.Measurements, "")
	m := &Schema{}
	b.Measurements["m"] = m
	if err := cp.Validate(); err != nil {
		t.Fatal(err)
	}
	if m.Name != "m" {
		t.Error("name not inferred")
	}

	b.Measurements["m"] = &Schema{Name: "m2"}
	confirmError(t, cp.Validate(), ErrMeasurementName,
		"invalid measurement name: m2 under key m")
}

func TestBucketOptionsValidate(t *testing.T) {
	b := &BucketOptions{}
	confirmError(t, b.Validate(false), ErrConfig,
		"FlushInterval (0s) < 1 ms")
	b.FlushInterval = edcode.Duration(5 * time.Millisecond)

	confirmError(t, b.Validate(false), ErrConfig,
		"BatchRuneCapacity (0) must be positive")
	b.BatchRuneCapacity = 10

	confirmError(t, b.Validate(false), ErrConfig,
		"HeldRuneCapacity (0) must be at least BatchRuneCapacity (10)")
	b.HeldRuneCapacity = 20

	confirmError(t, b.Validate(false), ErrConfig,
		"PendingTimeout (0s) < 1 s")
	b.PendingTimeout = edcode.Duration(time.Second)

	if err := b.Validate(false); err != nil {
		t.Error(err)
	}
}

func TestDefaultBucketOptions(t *testing.T) {
	bo := DefaultBucketOptions()
	if d := time.Duration(bo.FlushInterval); d != 1*time.Second {
		t.Errorf("FlushInterval %s", d)
	}
	if s := bo.BatchRuneCapacity; s != 256*1024 {
		t.Errorf("BatchRuneCapacity %d", s)
	}
	if s := bo.HeldRuneCapacity; s != 2*1024*1024 {
		t.Errorf("HeldRuneCapacity %d", s)
	}
	if n := bo.PendingLimit; n != 1 {
		t.Errorf("PendingLimit %d", n)
	}
	if d := time.Duration(bo.PendingTimeout); d != 10*time.Second {
		t.Errorf("PendingTimeout %d", d)
	}
	if bo.AllowUnknown {
		t.Error("AllowUnknown")
	}
	if bo.ValidateTags {
		t.Error("ValidateTags")
	}
	if bo.BypassNormalization {
		t.Error("BypassNormalization")
	}
}

func TestFieldType(t *testing.T) {
	fts := []FieldType{
		Float, Boolean, Integer, String, UInteger,
	}
	for _, v := range fts {
		sv := v.String()
		ft, ok := ParseFieldType(sv)
		if !ok || ft != v {
			t.Errorf("failed %s: %s %v %t", v, sv, ft, ok)
		}
	}
}

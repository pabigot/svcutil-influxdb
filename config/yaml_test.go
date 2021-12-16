package config

import (
	"encoding"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/pabigot/edcode"
)

func TestYAMLConnection(t *testing.T) {
	var c, c2 Connection
	exp := Connection{
		Id:           "TestYAMLConnection",
		URL:          "http://host.domain:8086",
		Organization: "my-org",
		Token:        "my-token",
		DefaultBucketOptions: &BucketOptions{
			FlushInterval: edcode.Duration(250 * time.Millisecond),
			ValidateTags:  true,
		},
		Buckets: map[string]*Bucket{
			"buck1": {
				Organization: "org1",
				Measurements: SchemaMap{
					"meas1": {
						RequiredTags: []string{"id"},
						Fields: map[string]FieldType{
							"if1": Integer,
							"if2": Integer,
							"uf1": UInteger,
						},
					},
				},
			},
			"buck2": {
				Measurements: SchemaMap{
					"b2m1": nil,
					"b2m2": {
						RequiredTags: []string{"t1", "t2"},
						OptionalTags: []string{"ot3"},
					},
				},
				Options: &BucketOptions{
					FlushInterval: edcode.Duration(5 * time.Millisecond),
					AllowUnknown:  true,
				},
			},
		},
	}
	t.Logf("b1t %s\n", exp.Buckets["buck1"].Organization)
	y := `id: TestYAMLConnection
url: http://host.domain:8086
organization: my-org
token: my-token
default-bucket-options:
  flush-interval: 250ms
  validate-tags: true
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
  buck2:
    measurements:
      b2m1: null
      b2m2:
        required-tags: [t1, t2]
        optional-tags: [ot3]
    options:
      flush-interval: 5ms
      allow-unknown: true
`
	err := yaml.Unmarshal([]byte(y), &c)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c, exp) {
		t.Fatal("unmarshal mismatch")
	}
	y2, err := yaml.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(y2))
	err = yaml.Unmarshal(y2, &c2)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c, c2) {
		t.Fatal("round-trip mismatch")
	}
}

func TestYAMLDuration(t *testing.T) {
	type ds struct {
		D edcode.Duration
	}
	type ts struct {
		text   string
		dur    time.Duration
		exp    string
		err    error
		errstr string
	}
	tests := []ts{
		{
			text: "d: 5",
			dur:  5 * time.Millisecond,
			exp:  "5ms",
		},
		{
			text: "d: 123us",
			dur:  123 * time.Microsecond,
			exp:  "123µs",
		},
		{
			text:   "d: -1s",
			err:    edcode.ErrDurationInvalid,
			errstr: ": -1s",
		},
	}
	var v ds

	// First make sure we spelled everything so that the unmarshaler will
	// be found.  This panics if we screwed up.
	_ = interface{}(&v.D).(encoding.TextUnmarshaler)
	_ = interface{}(&v.D).(encoding.TextMarshaler)

	for _, x := range tests {
		s := x.text
		err := yaml.Unmarshal([]byte(s), &v)
		if x.err != nil {
			confirmError(t, err, x.err, x.errstr)
		} else if err != nil {
			t.Fatal(err)
		} else {
			if d := time.Duration(v.D); d != x.dur {
				t.Errorf("%s: wrong FlushInterval: %v", s, d)
			}
			if y := v.D.String(); y != x.exp {
				t.Errorf("%s: text FlushInterval wrong: %s", x.exp, y)
			}
		}
	}

}

func TestYAMLFieldType(t *testing.T) {
	var ft FieldType
	err := yaml.Unmarshal([]byte("integer"), &ft)
	if err != nil {
		t.Fatal(err)
	}
	if ft != Integer {
		t.Errorf("bad extraction: %d %s", ft, ft)
	}
	err = yaml.Unmarshal([]byte("x"), &ft)
	confirmError(t, err, ErrFieldType, "unrecognized field type: x")
	ft = UInteger
	b, err := yaml.Marshal(ft)
	if err != nil {
		t.Fatal(err)
	}
	if s := strings.TrimSpace(string(b)); s != "uinteger" {
		t.Errorf("bad value: %s", s)
	}
}

func TestYAMLBucketSchemaMap(t *testing.T) {
	y := `
buck1:
  meas1:
    required-tags: [id]
    fields:
      if1: integer
      if2: integer
      uf1: uinteger
buck2:
  b2m1: null
  b2m2:
    required-tags: [t1, t2]
    optional-tags: [ot3]
`
	var bsm BucketSchemaMap
	var rt BucketSchemaMap
	err := yaml.Unmarshal([]byte(y), &bsm)
	if err != nil {
		t.Fatal(err.Error())
	}
	t.Logf("%# v", bsm)
	t.Logf("%# v", bsm["buck2"])
	t.Logf("%# v", bsm["buck2"]["b2m2"])
	raw, err := yaml.Marshal(bsm)
	if err != nil {
		t.Fatal(err.Error())
	}
	err = yaml.Unmarshal(raw, &rt)
	if err != nil {
		t.Fatal(err.Error())
	}
	if !reflect.DeepEqual(bsm, rt) {
		t.Fatal("yaml rt mismatch")
	}

	raw, err = json.Marshal(bsm)
	if err != nil {
		t.Fatal(err.Error())
	}
	err = json.Unmarshal(raw, &rt)
	if err != nil {
		t.Fatal(err.Error())
	}
	if !reflect.DeepEqual(bsm, rt) {
		t.Fatal("json unmarshal mismatch")
	}
}

module github.com/pabigot/svcutil/influx/cli

go 1.18

replace github.com/pabigot/svcutil/influxdb => /home/pab/Go/github.com/pabigot/svcutil/influxdb

replace github.com/pabigot/svcutil/redis => /home/pab/Go/github.com/pabigot/svcutil/redis

require (
	github.com/go-test/deep v1.0.8
	github.com/gomodule/redigo v1.8.9-0.20220324232115-5b789c6cfe82
	github.com/kr/pretty v0.3.0
	github.com/pabigot/logwrap v0.3.0
	github.com/pabigot/svcutil/influxdb v0.0.0-00010101000000-000000000000
	github.com/pabigot/svcutil/redis v0.0.0-00010101000000-000000000000
	gopkg.in/yaml.v2 v2.4.0
)

require (
	github.com/deepmap/oapi-codegen v1.9.1 // indirect
	github.com/influxdata/influxdb-client-go/v2 v2.8.1 // indirect
	github.com/influxdata/line-protocol v0.0.0-20210922203350-b1ad95c89adf // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/pabigot/done v0.0.1 // indirect
	github.com/pabigot/edcode v0.0.1 // indirect
	github.com/pabigot/set v0.1.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/rogpeppe/go-internal v1.8.0 // indirect
	golang.org/x/net v0.0.0-20220401154927-543a649e0bdd // indirect
)

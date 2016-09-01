# tarantool-proxy
------------------------------------------------------------
tarantool-proxy for 15 to 16

repo: https://github.com/shveenkov/tarantool-proxy.git

## Features

* proxy for tarantool iproto 1.5 to tarantool msgpack 1.6
* optional sharding, for scale

## Migrate problems to tarantool 1.6
1. select where id in (...), you should be rewritten like:
```
for id in (...):
    select where id=X
```
2. sharding specifics


## Dependencies
```
$ go get github.com/tarantool/go-tarantool
$ go get gopkg.in/yaml.v2
```

## Run proxy for test
```
$ cd $GOPATH/src
$ git clone https://github.com/shveenkov/tarantool-proxy.git
$ cd tarantool-proxy
$ go build
$ ./tarantool-proxy -config config.yaml
```

## Example config.yaml

```yaml
# host:port for create tarantool 1.5 iproto connection and listen requests
listen:
- 127.0.0.1:22033

# host:port shard for proxy pass data into tarantool 1.6 connection
tarantool:
- ['127.0.0.1:3302']

# enable sharding
sharding_enabled: true

# schema space for 1.5 and 1.6 communicate
space:
- id: 0
  name: key_value
  fields: ['key:str', 'value:str']
  index:
  - id: 0
    name: pk
    columns: [0]
```

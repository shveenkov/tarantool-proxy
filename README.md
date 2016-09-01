# tarantool-proxy
------------------------------------------------------------
tarntool-proxy for 15 to 16

repo: https://github.com/shveenkov/tarantool-proxy.git

* Proxy for tarantool iproto 1.5 to tarantool msgpack 1.6
* Optional sharding, for scale

# Migrate problems to tarantool 1.6
1. select where id in (...), you should be rewritten like:
for id in (...):
    select where id=X

2. sharding specifics


# Dependencies
$ go get github.com/tarantool/go-tarantool
$ go get gopkg.in/yaml.v2


# Run proxy for test
$ cd $GOPATH/src
$ git clone https://github.com/shveenkov/tarantool-proxy.git
$ cd tarantool-proxy
$ go build
$ ./tarantool-proxy -config config.yaml

config.yaml

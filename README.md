A "lite" implementation of both the server and client ZooKeeper 3.4.5 protocol.

See [zookeeper-proxy](http://github.com/lisaglendenning/zookeeper-proxy) for a simple application of this library.

## Building

zookeeper-lite is a Maven project. Build requires [zookeeper-lite-deps](http://github.com/lisaglendenning/zookeeper-lite-deps).

Uses SLF4J for logging. Apache Log4J2 is configured as the SLF4J backend in test scope.

Uses [com.typesafe.config](https://github.com/typesafehub/config) for configuration. 

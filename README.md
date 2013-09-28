A "lite" implementation of both the server and client ZooKeeper 3.5.0 protocol.

See [project website](http://lisaglendenning.github.io/zookeeper-lite).

## Quickstart

<pre>
> tar xzf zklite-all-*.tar.gz && cd zklite-all-*
> bin/server.py --help
> bin/client.py --help
</pre>

## Building

zookeeper-lite is a [Maven project](http://maven.apache.org/).

## Configuration

Uses [Apache Log4J2](http://logging.apache.org/log4j/2.x/) for logging.

Uses [com.typesafe.config](https://github.com/typesafehub/config) for configuration. 

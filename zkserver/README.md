Java in-memory standalone ZooKeeper server.

## Quickstart

<pre>
> cd zkserver &&  mvn exec:java -Dexec.mainClass="edu.uw.zookeeper.server.Main" -Dexec.args="--help"
</pre>

## Non-goals

- Distribution
- Replication
- Persistence
- Authentication

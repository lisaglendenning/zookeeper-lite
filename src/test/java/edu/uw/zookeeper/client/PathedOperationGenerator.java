package edu.uw.zookeeper.client;

import java.util.Random;

import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.proto.Records;

public class PathedOperationGenerator implements Generator<Records.Request> {

    public static PathedOperationGenerator create(
            ZNodeViewCache<?,?,?> cache) {
        Random random = new Random();
        CachedPaths paths = CachedPaths.create(cache, random);
        Operations.PathBuilder<? extends Records.Request,?> operation = Operations.Requests.exists().setPath(ZNodeLabel.Path.root()).setWatch(false);
        return new PathedOperationGenerator(
                operation, paths);
    }

    protected final Generator<ZNodeLabel.Path> paths;
    protected final Operations.PathBuilder<? extends Records.Request,?> operation;

    public PathedOperationGenerator(
            Operations.PathBuilder<? extends Records.Request,?> operation,
            Generator<ZNodeLabel.Path> paths) {
        this.operation = operation;
        this.paths = paths;
    }
    
    @Override
    public synchronized Records.Request next() {
        return operation.setPath(paths.next()).build();
    }
}

package edu.uw.zookeeper.client;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.proto.Records;

public class PathToRequests implements Function<ZNodePath, List<Records.Request>> {

    public static PathToRequests forRequests(Operations.Builder<? extends Records.Request>... builders) {
        return forIterable(ImmutableList.copyOf(builders));
    }
    
    public static PathToRequests forIterable(Iterable<? extends Operations.Builder<? extends Records.Request>> builders) {
        return new PathToRequests(ImmutableList.copyOf(builders));
    }
    
    private final ImmutableList<? extends Operations.Builder<? extends Records.Request>> builders;
    
    protected PathToRequests(ImmutableList<? extends Operations.Builder<? extends Records.Request>> builders) {
        this.builders = builders;
    }
    
    public ImmutableList<? extends Operations.Builder<? extends Records.Request>> builders() {
        return builders;
    }
    
    @Override
    public List<Records.Request> apply(final ZNodePath path) {
        List<Records.Request> requests = Lists.newArrayListWithCapacity(builders.size());
        for (Operations.Builder<? extends Records.Request> builder: builders) {
            if (builder instanceof Operations.PathBuilder<?,?>) {
                ((Operations.PathBuilder<?,?>) builder).setPath(path);
            }
            requests.add(builder.build());
        }
        return requests;
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).addValue(builders).toString();
    }
}
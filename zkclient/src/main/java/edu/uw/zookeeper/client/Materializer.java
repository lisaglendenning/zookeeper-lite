package edu.uw.zookeeper.client;

import java.io.IOException;
import java.lang.reflect.Modifier;

import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.StrongConcurrentSet;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.Schema;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.StampedReference;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeLabelTrie;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.proto.ISetDataRequest;
import edu.uw.zookeeper.protocol.proto.Records;

public class Materializer<V extends Operation.ProtocolResponse<?>> extends ZNodeCacheTrie<Materializer.MaterializedNode, Records.Request, V> {

    public static <T extends Operation.ProtocolRequest<Records.Request>, V extends Operation.ProtocolResponse<?>> Materializer<V> newInstance(
            Schema schema, 
            Serializers.ByteCodec<Object> codec, 
            ClientExecutor<? super Records.Request, V, SessionListener> client) {
        return new Materializer<V>(schema, codec, client, new StrongConcurrentSet<CacheSessionListener<? super MaterializedNode>>(), MaterializedNode.root(schema));
    }
    
    public static <I extends Operation.Request, V extends Operation.ProtocolResponse<?>> 
    ListenableFuture<V> submit(ClientExecutor<I, V, ?> client, I request) {
        return client.submit(request);
    }
    
    protected final Schema schema;
    protected final Serializers.ByteCodec<Object> codec;
    protected final Operator operator;
    protected final MaterializeVisitor materializer;
    
    protected Materializer(
            Schema schema, 
            Serializers.ByteCodec<Object> codec, 
            ClientExecutor<? super Records.Request, V, SessionListener> client,
            IConcurrentSet<CacheSessionListener<? super MaterializedNode>> listeners,
            MaterializedNode root) {
        super(client, listeners, root);
        this.schema = schema;
        this.codec = codec;
        this.operator = new Operator();
        this.materializer = new MaterializeVisitor();
    }
    
    public Schema schema() {
        return schema;
    }
    
    public Serializers.ByteCodec<Object> codec() {
        return codec;
    }
    
    public Operator operator() {
        return operator;
    }

    @Override
    public void handleCacheUpdate(CacheEvent<? extends MaterializedNode> event) {
        // intercept data update events here to deserialize
        if ((event instanceof NodeUpdatedCacheEvent<?>) && ((NodeUpdatedCacheEvent<?>) event).getTypes().contains(Records.DataGetter.class)) {
            materializer.apply(event.getNode());
        }
        
        super.handleCacheUpdate(event);
    }

    public static class MaterializedNode extends ZNodeCacheTrie.AbstractCachedNode<MaterializedNode> {
    
        public static MaterializedNode root(Schema schema) {
            ZNodeLabelTrie.Pointer<MaterializedNode> pointer = ZNodeLabelTrie.strongPointer(ZNodeLabel.none(), null);
            return new MaterializedNode(pointer, schema.root());
        }
        
        protected final Schema.SchemaNode schemaNode;
        
        protected MaterializedNode(
                ZNodeLabelTrie.Pointer<? extends MaterializedNode> parent, 
                Schema.SchemaNode schemaNode) {
            super(parent);
            this.schemaNode = schemaNode;
        }
        
        public Schema.SchemaNode schemaNode() {
            return schemaNode;
        }
        
        public <T> StampedReference<T> getCached() {
            return getCached(schemaNode.schema().getType());
        }

        @Override
        protected MaterializedNode newChild(ZNodeLabel.Component label) {
            ZNodeLabelTrie.Pointer<MaterializedNode> pointer = ZNodeLabelTrie.weakPointer(label, this);
            Schema.SchemaNode node = (schemaNode != null) ? schemaNode.match(label) : null;
            return new MaterializedNode(pointer, node);
        }
        
        @Override
        public synchronized String toString() {
            return Objects.toStringHelper("")
                    .add("path", path())
                    .add("children", keySet())
                    .add("stamp", stamp)
                    .add("cache", cache.values())
                    .add("schema", schemaNode).toString();
        }
    }
    
    public static enum ObjectsEquivalence implements Equivalence<Object> {
        OBJECTS_EQUIVALENCE;
        
        @Override
        public boolean equals(Object a, Object b) {
            return Objects.equal(a, b);
        }
    }
    
    public class MaterializeVisitor implements Function<MaterializedNode, Optional<? extends StampedReference<?>>> {
        
        public MaterializeVisitor() {
        }
        
        @Override
        public Optional<? extends StampedReference<?>> apply(MaterializedNode node) {
            if (node != null) {
                Schema.SchemaNode schemaNode = node.schemaNode();
                if ((schemaNode != null) && (schemaNode.schema().getType() instanceof Class<?>)) {
                    Class<?> cls = (Class<?>) schemaNode.schema().getType();
                    int mod = cls.getModifiers();
                    if (! ((cls == Void.class) || Modifier.isInterface(mod) 
                            || Modifier.isAbstract(mod))) {
                        StampedReference<Records.DataGetter> data = node.getCached(Records.DataGetter.class);
                        if ((data != null) && (data.get() != null)) { 
                            Object value;
                            if ((data.get().getData() != null) && (data.get().getData().length > 0)) {
                                try {
                                    value = codec.fromBytes(data.get().getData(), cls);
                                } catch (IOException e) {
                                    throw Throwables.propagate(e);
                                }
                            } else {
                                value = null;
                            }
                            StampedReference<Object> materialized = StampedReference.<Object>of(data.stamp(), value);
                            if (new UpdateVisitor<Object>(cls, materialized, ObjectsEquivalence.OBJECTS_EQUIVALENCE).apply(node)) {
                                return Optional.of(materialized);
                            }
                        }
                    }
                }
            }
            return Optional.absent();
        }
    }

    public class Operator implements Reference<Materializer<V>> {
        
        public class Submitter<C extends Operations.Builder<? extends Records.Request>> implements Reference<C> {
            protected final C builder;
            protected final ClientExecutor<? super Records.Request, V, ?> client;
            
            public Submitter(C builder, ClientExecutor<? super Records.Request, V, ?> client) {
                this.builder = builder;
                this.client = client;
            }
            
            @Override
            public C get() {
                return builder;
            }
            
            public ListenableFuture<V> submit() {
                return client.submit(builder.build());
            }
        }
        
        public Operator() {}
    
        @Override
        public Materializer<V> get() {
            return Materializer.this;
        }
    
        public Submitter<Operations.Requests.SerializedData<Records.Request, Operations.Requests.Create, Object>> create(ZNodeLabel.Path path) {
            return create(path, null);
        }
        
        public Submitter<Operations.Requests.SerializedData<Records.Request, Operations.Requests.Create, Object>> create(ZNodeLabel.Path path, Object data) {
            Schema.SchemaNode node = get().schema().match(path);
            Operations.Requests.Create create = Operations.Requests.create().setPath(path);
            if (node != null) {
                create.setMode(node.schema().getCreateMode()).setAcl(Schema.inheritedAcl(node));
            }
            return new Submitter<Operations.Requests.SerializedData<Records.Request, Operations.Requests.Create, Object>>(
                    Operations.Requests.serialized(create, get().codec(), data), get());
        }
        
        public Submitter<Operations.Requests.Delete> delete(ZNodeLabel.Path path) {
            return new Submitter<Operations.Requests.Delete>(
                    Operations.Requests.delete().setPath(path), get());
        }
    
        public Submitter<Operations.Requests.Exists> exists(ZNodeLabel.Path path) {
            return exists(path, false);
        }
    
        public Submitter<Operations.Requests.Exists> exists(ZNodeLabel.Path path, boolean watch) {
            return new Submitter<Operations.Requests.Exists>(
                    Operations.Requests.exists().setPath(path).setWatch(watch), get());
        }
    
        public Submitter<Operations.Requests.GetAcl> getAcl(ZNodeLabel.Path path) {
            return new Submitter<Operations.Requests.GetAcl>(
                    Operations.Requests.getAcl().setPath(path), get());
        }
        
        public Submitter<Operations.Requests.GetChildren> getChildren(ZNodeLabel.Path path) {
            return getChildren(path, false);
        }
    
        public Submitter<Operations.Requests.GetChildren> getChildren(ZNodeLabel.Path path, boolean watch) {
            return new Submitter<Operations.Requests.GetChildren>(
                    Operations.Requests.getChildren().setPath(path).setWatch(watch), get());
        }
    
        public Submitter<Operations.Requests.GetData> getData(ZNodeLabel.Path path) {
            return getData(path, false);
        }
    
        public Submitter<Operations.Requests.GetData> getData(ZNodeLabel.Path path, boolean watch) {
            return new Submitter<Operations.Requests.GetData>(
                    Operations.Requests.getData().setPath(path).setWatch(watch), get());
        }
        
        public Submitter<Operations.Requests.Multi> multi() {
            return new Submitter<Operations.Requests.Multi>(
                    Operations.Requests.multi(), get());
        }
    
        public Submitter<Operations.Requests.SetAcl> setAcl(ZNodeLabel.Path path) {
            return new Submitter<Operations.Requests.SetAcl>(
                    Operations.Requests.setAcl().setPath(path), get());
        }
    
        public Submitter<Operations.Requests.SerializedData<ISetDataRequest, Operations.Requests.SetData, Object>> setData(ZNodeLabel.Path path, Object data) {
            Operations.Requests.SetData setData = Operations.Requests.setData().setPath(path);
            return new Submitter<Operations.Requests.SerializedData<ISetDataRequest, Operations.Requests.SetData, Object>>(
                    Operations.Requests.serialized(setData, get().codec(), data), get());
        }

        public Submitter<Operations.Requests.SerializedData<ISetDataRequest, Operations.Requests.SetData, Object>> setData(ZNodeLabel.Path path, Object data, int version) {
            Operations.Requests.SetData setData = Operations.Requests.setData().setPath(path).setVersion(version);
            return new Submitter<Operations.Requests.SerializedData<ISetDataRequest, Operations.Requests.SetData, Object>>(
                    Operations.Requests.serialized(setData, get().codec(), data), get());
        }
        
        public Submitter<Operations.Requests.Sync> sync(ZNodeLabel.Path path) {
            return new Submitter<Operations.Requests.Sync>(
                    Operations.Requests.sync().setPath(path), get());
        }
    }
}

package edu.uw.zookeeper.data;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.concurrent.Callable;

import org.apache.jute.Record;

import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.StrongConcurrentSet;

import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.proto.ISetDataRequest;
import edu.uw.zookeeper.protocol.proto.Records;

public class Materializer<E extends Materializer<E,V>.MaterializedNode, V extends Operation.ProtocolResponse<?>> extends ZNodeCache<E, Records.Request, V> {

    @SuppressWarnings("unchecked")
    public static <E extends Materializer<E,V>.MaterializedNode, V extends Operation.ProtocolResponse<?>> Materializer<E,V> fromHierarchy(
            Class<? extends E> rootType,
            Serializers.ByteCodec<Object> codec, 
            ClientExecutor<? super Records.Request, V, SessionListener> client) {
        SchemaElementLookup schema = SchemaElementLookup.fromHierarchy(rootType);
        E root = null;
        for (Constructor<?> ctor: rootType.getConstructors()) {
            if (ctor.getParameterTypes().length == 1) {
                try {
                    root = (E) ctor.newInstance(schema.get().root());
                } catch (Exception e) {
                    throw new IllegalArgumentException(String.format("Error calling constructor %s", ctor), e);
                }
                break;
            }
        }
        if (root == null) {
            throw new IllegalArgumentException(String.format("Can't find constructor in %s", rootType));
        }
        return newInstance(schema, codec, client, root);
    }
    
    public static <E extends Materializer<E,V>.MaterializedNode, V extends Operation.ProtocolResponse<?>> Materializer<E,V> newInstance(
            SchemaElementLookup schema, 
            Serializers.ByteCodec<Object> codec, 
            ClientExecutor<? super Records.Request, V, SessionListener> client,
            E root) {
        return new Materializer<E,V>(schema, codec, client, new StrongConcurrentSet<CacheSessionListener<? super E>>(), root);
    }
    
    public static <I extends Operation.Request, V extends Operation.ProtocolResponse<?>> 
    ListenableFuture<V> submit(ClientExecutor<I, V, ?> client, I request) {
        return client.submit(request);
    }
    
    protected final SchemaElementLookup schema;
    protected final Serializers.ByteCodec<Object> codec;
    
    protected Materializer(
            SchemaElementLookup schema, 
            Serializers.ByteCodec<Object> codec, 
            ClientExecutor<? super Records.Request, V, SessionListener> client,
            IConcurrentSet<CacheSessionListener<? super E>> listeners,
            E root) {
        super(client, listeners, root);
        this.schema = schema;
        this.codec = codec;
    }
    
    public SchemaElementLookup schema() {
        return schema;
    }
    
    public Serializers.ByteCodec<Object> codec() {
        return codec;
    }

    @Override
    protected ImmutableSet<Object> updateNode(E node, long stamp, Record...records) {
        ImmutableSet<Object> types = super.updateNode(node, stamp, records);
        
        // deserialize data if it changed
        if (types.contains(Records.DataGetter.class)) {
            ValueNode<ZNodeSchema> schemaNode = node.schema();
            if ((schemaNode != null) && (schemaNode.get().getDataType() instanceof Class<?>)) {
                Class<?> cls = (Class<?>) schemaNode.get().getDataType();
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
                        if (updateNode(node, cls, materialized, ObjectsEquivalence.OBJECTS_EQUIVALENCE)) {
                            return ImmutableSet.builder().addAll(types).add(cls).build();
                        }
                    }
                }
            }
        }
        
        return types;
    }
    
    public abstract class MaterializedNode extends ZNodeCache.AbstractCachedNode<E> {
    
        protected final ValueNode<ZNodeSchema> schema;
        
        protected MaterializedNode(
                NameTrie.Pointer<? extends E> parent, 
                ValueNode<ZNodeSchema> schema) {
            super(parent);
            this.schema = schema;
        }
        
        public ValueNode<ZNodeSchema> schema() {
            return schema;
        }
        
        public <T> StampedReference<T> getCachedData() {
            return getCached(schema.get().getDataType());
        }

        public Operator<Operations.Requests.SerializedData<Records.Request, Operations.Requests.Create, Object>> create() {
            return create(null);
        }
        
        public Operator<Operations.Requests.SerializedData<Records.Request, Operations.Requests.Create, Object>> create(Object data) {
            Operations.Requests.Create create = Operations.Requests.create().setPath(path());
            if (schema != null) {
                create.setMode(schema.get().getCreateMode()).setAcl(ZNodeSchema.inheritedAcl(schema));
            }
            return new Operator<Operations.Requests.SerializedData<Records.Request, Operations.Requests.Create, Object>>(
                    Operations.Requests.serialized(create, codec, data));
        }
        
        public Operator<Operations.Requests.Delete> delete() {
            return new Operator<Operations.Requests.Delete>(
                    Operations.Requests.delete().setPath(path()));
        }
    
        public Operator<Operations.Requests.GetData> getData() {
            return new Operator<Operations.Requests.GetData>(
                    Operations.Requests.getData().setPath(path()));
        }

        public Operator<Operations.Requests.GetChildren> getChildren() {
            return new Operator<Operations.Requests.GetChildren>(
                    Operations.Requests.getChildren().setPath(path()));
        }

        public Operator<Operations.Requests.Exists> exists() {
            return new Operator<Operations.Requests.Exists>(
                    Operations.Requests.exists().setPath(path()));
        }
    
        public Operator<Operations.Requests.GetAcl> getAcl() {
            return new Operator<Operations.Requests.GetAcl>(
                    Operations.Requests.getAcl().setPath(path()));
        }
        
        public Operator<Operations.Requests.SetAcl> setAcl() {
            return new Operator<Operations.Requests.SetAcl>(
                    Operations.Requests.setAcl().setPath(path()));
        }

        public Operator<Operations.Requests.SerializedData<ISetDataRequest, Operations.Requests.SetData, Object>> setData(Object data) {
            Operations.Requests.SetData setData = Operations.Requests.setData().setPath(path());
            return new Operator<Operations.Requests.SerializedData<ISetDataRequest, Operations.Requests.SetData, Object>>(
                    Operations.Requests.serialized(setData, codec, data));
        }
        
        public Operator<Operations.Requests.Sync> sync() {
            return new Operator<Operations.Requests.Sync>(
                    Operations.Requests.sync().setPath(path()));
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper("")
                    .add("path", path())
                    .add("children", keySet())
                    .add("stamp", stamp)
                    .add("cache", cache.values())
                    .add("schema", schema).toString();
        }
    }

    public class SimpleMaterializedNode extends MaterializedNode {

        public SimpleMaterializedNode(
                NameTrie.Pointer<? extends E> parent, 
                ValueNode<ZNodeSchema> schema) {
            super(parent, schema);
        }
        
        @SuppressWarnings("unchecked")
        @Override
        protected E newChild(ZNodeName name) {
            NameTrie.Pointer<E> pointer = SimpleNameTrie.weakPointer(name, (E) this);
            ValueNode<ZNodeSchema> node = (schema != null) ? ZNodeSchema.matchChild(schema, name) : null;
            return (E) new SimpleMaterializedNode(pointer, node);
        }
    }

    public static enum ObjectsEquivalence implements Equivalence<Object> {
        OBJECTS_EQUIVALENCE;
        
        @Override
        public boolean equals(Object a, Object b) {
            return Objects.equal(a, b);
        }
    }
    

    public class Operator<C extends Operations.Builder<? extends Records.Request>> implements Supplier<C>, Callable<ListenableFuture<V>> {
        protected final C builder;
        
        public Operator(C builder) {
            this.builder = builder;
        }
        
        @Override
        public C get() {
            return builder;
        }

        @Override
        public ListenableFuture<V> call() {
            return submit(builder.build());
        }
    }
}

package edu.uw.zookeeper.data;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.Callable;

import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.proto.ISetDataRequest;
import edu.uw.zookeeper.protocol.proto.Records;

public class Materializer<E extends Materializer.MaterializedNode<E,?>, O extends Operation.ProtocolResponse<?>> implements ClientExecutor<Records.Request,O, SessionListener> {

    public static <E extends Materializer.MaterializedNode<E,?>, O extends Operation.ProtocolResponse<?>> Materializer<E,O> fromHierarchy(
            Class<? extends E> rootType,
            Serializers.ByteCodec<Object> codec, 
            ClientExecutor<? super Records.Request, O, SessionListener> client) {
        SchemaElementLookup schema = SchemaElementLookup.fromHierarchy(rootType);
        return fromSchema(schema, rootType, codec, client);
    }

    @SuppressWarnings("unchecked")
    public static <E extends Materializer.MaterializedNode<E,?>, O extends Operation.ProtocolResponse<?>> Materializer<E,O> fromSchema(
            SchemaElementLookup schema, 
            Class<? extends E> rootType,
            Serializers.ByteCodec<Object> codec, 
            ClientExecutor<? super Records.Request, O, SessionListener> client) {
        E root = null;
        for (Constructor<?> ctor: rootType.getConstructors()) {
            if (ctor.getParameterTypes().length == 2) {
                try {
                    root = (E) ctor.newInstance(schema.get().root(), codec);
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
    
    public static <E extends Materializer.MaterializedNode<E,?>, O extends Operation.ProtocolResponse<?>> Materializer<E,O> newInstance(
            SchemaElementLookup schema, 
            Serializers.ByteCodec<Object> codec, 
            ClientExecutor<? super Records.Request,O,SessionListener> client,
            E root) {
        return new Materializer<E,O>(schema, codec, LockableZNodeCache.<E,Records.Request,O>newInstance(client, root));
    }
    
    public static <I extends Operation.Request, V extends Operation.ProtocolResponse<?>> 
    ListenableFuture<V> submit(ClientExecutor<? super I, V, ?> client, I request) {
        return client.submit(request);
    }
    
    protected final LockableZNodeCache<E,Records.Request,O> cache;
    protected final SchemaElementLookup schema;
    protected final Serializers.ByteCodec<Object> codec;
    
    protected Materializer(
            SchemaElementLookup schema, 
            Serializers.ByteCodec<Object> codec, 
            LockableZNodeCache<E,Records.Request,O> cache) {
        this.schema = schema;
        this.codec = codec;
        this.cache = cache;
    }
    
    public LockableZNodeCache<E,Records.Request,O> cache() {
        return cache;
    }
    
    public SchemaElementLookup schema() {
        return schema;
    }
    
    public Serializers.ByteCodec<Object> codec() {
        return codec;
    }

    @Override
    public void subscribe(SessionListener listener) {
        cache.subscribe(listener);
    }

    @Override
    public boolean unsubscribe(SessionListener listener) {
        return cache.unsubscribe(listener);
    }

    @Override
    public ListenableFuture<O> submit(Records.Request request) {
        return cache.submit(request);
    }
    
    @Override
    public ListenableFuture<O> submit(Records.Request request, Promise<O> promise) {
        return cache.submit(request, promise);
    }
    
    public Operator<Operations.Requests.Create> create(ZNodePath path) {
        return operator(createParameters(path));
    }
    
    public <V> Operator<? extends Operations.DataBuilder<? extends Records.Request, ?>> create(ZNodePath path, V data) {
        if (data == null) {
            return create(path);
        } else {
            return operator(Operations.Requests.serialized(createParameters(path), codec, data));
        }
    }
    
    protected Operations.Requests.Create createParameters(ZNodePath path) {
        ValueNode<ZNodeSchema> schema = ZNodeSchema.matchPath(this.schema.get(), path);
        Operations.Requests.Create create = Operations.Requests.create()
                .setMode(schema.get().getCreateMode())
                .setAcl(ZNodeSchema.inheritedAcl(schema));
        if (create.getMode().contains(CreateFlag.SEQUENTIAL)) {
            if (path.toString().endsWith(Sequential.SUFFIX_PATTERN.toString())) {
                path = ZNodePath.fromString(path.toString().substring(0, path.toString().length()-Sequential.SUFFIX_PATTERN.toString().length()));
            }
        }
        return create.setPath(path);
    }
    
    public Operator<Operations.Requests.Delete> delete(ZNodePath path) {
        return operator(Operations.Requests.delete().setPath(path));
    }

    public Operator<Operations.Requests.GetData> getData(ZNodePath path) {
        return operator(Operations.Requests.getData().setPath(path));
    }

    public Operator<Operations.Requests.GetChildren> getChildren(ZNodePath path) {
        return operator(Operations.Requests.getChildren().setPath(path));
    }

    public Operator<Operations.Requests.Exists> exists(ZNodePath path) {
        return operator(Operations.Requests.exists().setPath(path));
    }

    public Operator<Operations.Requests.GetAcl> getAcl(ZNodePath path) {
        return operator(Operations.Requests.getAcl().setPath(path));
    }
    
    public Operator<Operations.Requests.SetAcl> setAcl(ZNodePath path) {
        return operator(Operations.Requests.setAcl().setPath(path));
    }

    public <V> Operator<Operations.Requests.SerializedData<ISetDataRequest, Operations.Requests.SetData, V>> setData(ZNodePath path, V data) {
        Operations.Requests.SetData setData = Operations.Requests.setData().setPath(path);
        return operator(Operations.Requests.serialized(setData, codec, data));
    }
    
    public Operator<Operations.Requests.Sync> sync(ZNodePath path) {
        return operator(Operations.Requests.sync().setPath(path));
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(((Class<?>) schema().get().root().get().getDeclaration()).getSimpleName()).toString();
    }
    
    protected <T extends Operations.Builder<? extends Records.Request>> Operator<T> operator(T builder) {
        return new Operator<T>(builder);
    }
    
    public class Operator<T extends Operations.Builder<? extends Records.Request>> implements Supplier<T>, Callable<ListenableFuture<O>> {
        
        protected final T builder;
        
        public Operator(T builder) {
            this.builder = builder;
        }

        @Override
        public ListenableFuture<O> call() {
            return cache().submit(get().build());
        }

        @Override
        public T get() {
            return builder;
        }
    }
    
    public abstract static class MaterializedNode<E extends MaterializedNode<E,?>,V> extends ZNodeCache.AbstractCacheNode<E,V> {
    
        protected final ValueNode<ZNodeSchema> schema;
        protected final Serializers.ByteCodec<Object> codec;

        protected MaterializedNode(
                ValueNode<ZNodeSchema> schema,
                Serializers.ByteCodec<Object> codec,
                NameTrie.Pointer<? extends E> parent) {
            this(schema, codec, null, null, -1L, parent);
        }

        protected MaterializedNode( 
                ValueNode<ZNodeSchema> schema,
                Serializers.ByteCodec<Object> codec,
                V data,
                Records.ZNodeStatGetter stat,
                long stamp,
                NameTrie.Pointer<? extends E> parent) {
            this(schema, codec, data, stat, stamp, parent, Maps.<ZNodeName, E>newHashMap());
        }

        protected MaterializedNode( 
                ValueNode<ZNodeSchema> schema,
                Serializers.ByteCodec<Object> codec,
                V data,
                Records.ZNodeStatGetter stat,
                long stamp,
                NameTrie.Pointer<? extends E> parent,
                Map<ZNodeName, E> children) {
            super(data, stat, stamp, parent, children);
            this.schema = schema;
            this.codec = codec;
        }
        
        public ValueNode<ZNodeSchema> schema() {
            return schema;
        }

        @SuppressWarnings("unchecked")
        @Override
        protected V transformData(byte[] data) {
            if ((data == null) || (data.length == 0)) {
                return null;
            }
            V materialized;
            Class<?> type = schema.get().getDataType();
            if (type == Void.class) {
                materialized = (V) data;
            } else {
                try {
                    materialized = (V) codec.fromBytes(data, type);
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
            return materialized;
        }

        @Override
        protected boolean equivalentData(V v1, V v2) {
            return Objects.equal(v1, v2);
        }
        
        @SuppressWarnings("unchecked")
        @Override
        protected E newChild(ZNodeName name) {
            NameTrie.Pointer<E> pointer = SimpleLabelTrie.weakPointer(name, (E) this);
            ValueNode<ZNodeSchema> childSchema = (schema != null) ? ZNodeSchema.matchChild(schema, name) : null;

            // find the right Constructor
            Class<?> type;
            if ((childSchema != null) && childSchema.get().getDeclaration() instanceof Class<?>) {
                type = (Class<?>) childSchema.get().getDeclaration();
            } else {
                // TODO ?
                type = getClass();
            }

            for (Constructor<?> ctor: type.getDeclaredConstructors()) {
                if (ctor.getParameterTypes().length == 3) {
                    try {
                        return (E) ctor.newInstance(childSchema, codec, pointer);
                    } catch (Exception e) {
                        throw new UnsupportedOperationException(e);
                    }
                }
            }
            
            throw new UnsupportedOperationException();
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper("")
                    .add("path", path())
                    .add("children", keySet())
                    .add("stamp", stamp())
                    .add("stat", stat())
                    .add("data", data())
                    .add("schema", schema()).toString();
        }
    }
}

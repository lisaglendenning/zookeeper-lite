package edu.uw.zookeeper.data;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.concurrent.Callable;

import net.engio.mbassy.common.StrongConcurrentSet;

import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.proto.ISetDataRequest;
import edu.uw.zookeeper.protocol.proto.Records;

public class Materializer<E extends Materializer.MaterializedNode<E,?>, V extends Operation.ProtocolResponse<?>> extends ZNodeCache<E, Records.Request, V> {

    @SuppressWarnings("unchecked")
    public static <E extends Materializer.MaterializedNode<E,?>, V extends Operation.ProtocolResponse<?>> Materializer<E,V> fromHierarchy(
            Class<? extends E> rootType,
            Serializers.ByteCodec<Object> codec, 
            ClientExecutor<? super Records.Request, V, SessionListener> client) {
        SchemaElementLookup schema = SchemaElementLookup.fromHierarchy(rootType);
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
    
    public static <E extends Materializer.MaterializedNode<E,?>, V extends Operation.ProtocolResponse<?>> Materializer<E,V> newInstance(
            SchemaElementLookup schema, 
            Serializers.ByteCodec<Object> codec, 
            ClientExecutor<? super Records.Request, V, SessionListener> client,
            E root) {
        return new Materializer<E,V>(schema, codec, client, new CacheEvents(new StrongConcurrentSet<CacheListener>()), SimpleNameTrie.forRoot(root));
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
            CacheEvents events,
            NameTrie<E> trie) {
        super(client, events, trie);
        this.schema = schema;
        this.codec = codec;
    }
    
    public SchemaElementLookup schema() {
        return schema;
    }
    
    public Serializers.ByteCodec<Object> codec() {
        return codec;
    }
    
    public Operator<Operations.Requests.SerializedData<Records.Request, Operations.Requests.Create, Object>> create(ZNodePath path) {
        return create(null);
    }
    
    public Operator<Operations.Requests.SerializedData<Records.Request, Operations.Requests.Create, Object>> create(ZNodePath path, Object data) {
        Operations.Requests.Create create = Operations.Requests.create().setPath(path);
        ValueNode<ZNodeSchema> schema = ZNodeSchema.matchPath(this.schema.get(), path);
        create.setMode(schema.get().getCreateMode()).setAcl(ZNodeSchema.inheritedAcl(schema));
        return operator(Operations.Requests.serialized(create, codec, data));
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

    public Operator<Operations.Requests.SerializedData<ISetDataRequest, Operations.Requests.SetData, V>> setData(ZNodePath path, V data) {
        Operations.Requests.SetData setData = Operations.Requests.setData().setPath(path);
        return operator(Operations.Requests.serialized(setData, codec, data));
    }
    
    public Operator<Operations.Requests.Sync> sync(ZNodePath path) {
        return operator(Operations.Requests.sync().setPath(path));
    }
    
    protected <T extends Operations.Builder<? extends Records.Request>> Operator<T> operator(T builder) {
        return new Operator<T>(builder);
    }
    
    public class Operator<T extends Operations.Builder<? extends Records.Request>> implements Supplier<T>, Callable<ListenableFuture<V>> {
        
        protected final T builder;
        
        public Operator(T builder) {
            this.builder = builder;
        }

        @Override
        public ListenableFuture<V> call() throws Exception {
            return submit(get().build());
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
            super(data, stat, stamp, parent);
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
            try {
                materialized = (V) codec.fromBytes(data, type);
            } catch (IOException e) {
                throw Throwables.propagate(e);
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
            NameTrie.Pointer<E> pointer = SimpleNameTrie.weakPointer(name, (E) this);
            ValueNode<ZNodeSchema> childSchema = (schema != null) ? ZNodeSchema.matchChild(schema, name) : null;

            // find the right Constructor
            Class<?> type;
            if (childSchema.get().getDeclaration() instanceof Class<?>) {
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

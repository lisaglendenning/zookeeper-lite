package edu.uw.zookeeper.client;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Map;

import org.apache.jute.Record;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.Schema;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.StampedReference;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeLabelTrie;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.ISetDataRequest;
import edu.uw.zookeeper.protocol.proto.Records;

public class Materializer<V extends Operation.ProtocolResponse<?>> extends ZNodeViewCache<Materializer.MaterializedNode, Records.Request, V> {

    public static <T extends Operation.ProtocolRequest<Records.Request>, V extends Operation.ProtocolResponse<?>> Materializer<V> newInstance(
            Schema schema, 
            Serializers.ByteCodec<Object> codec, 
            ClientExecutor<? super Records.Request, V> client) {
        return new Materializer<V>(schema, codec, client, client);
    }
    
    public static <I extends Operation.Request, V extends Operation.ProtocolResponse<?>> 
    ListenableFuture<V> submit(ClientExecutor<I, V> client, I request) {
        return client.submit(request);
    }
    
    protected final Schema schema;
    protected final Serializers.ByteCodec<Object> codec;
    protected final Operator operator;
    
    public Materializer(
            Schema schema, 
            Serializers.ByteCodec<Object> codec, 
            Publisher publisher,
            ClientExecutor<? super Records.Request, V> client) {
        super(publisher, client, ZNodeLabelTrie.of(MaterializedNode.root(schema, codec)));
        this.schema = schema;
        this.codec = codec;
        this.operator = new Operator();
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

    public static class MaterializedNode extends ZNodeViewCache.AbstractNodeCache<MaterializedNode> implements Reference<StampedReference<? extends Object>> {
    
        public static MaterializedNode root(Schema schema, Serializers.ByteCodec<Object> codec) {
            return new MaterializedNode(Optional.<ZNodeLabelTrie.Pointer<MaterializedNode>>absent(), schema.root(), codec);
        }
        
        protected final StampedReference.Updater<Object> instance;
        protected final Schema.SchemaNode schemaNode;
        protected final Serializers.ByteCodec<Object> codec;
        
        protected MaterializedNode(
                Optional<ZNodeLabelTrie.Pointer<MaterializedNode>> parent, 
                Schema.SchemaNode schemaNode, 
                Serializers.ByteCodec<Object> codec) {
            this(parent, schemaNode, codec, StampedReference.of(0L, null));
        }
    
        protected MaterializedNode(
                Optional<ZNodeLabelTrie.Pointer<MaterializedNode>> parent, 
                Schema.SchemaNode schemaNode, 
                Serializers.ByteCodec<Object> codec,
                StampedReference<Object> instance) {
            super(parent);
            this.instance = StampedReference.Updater.newInstance(instance);
            this.schemaNode = schemaNode;
            this.codec = codec;
        }
        
        public Schema.SchemaNode schemaNode() {
            return schemaNode;
        }
        
        public Serializers.ByteCodec<Object> codec() {
            return codec;
        }
    
        @Override
        public StampedReference<Object> get() {
            return instance.get();
        }
    
        @Override
        public <T extends Records.ZNodeView> StampedReference<T> update(View view, StampedReference<T> value) {
            StampedReference<T> result = super.update(view, value);
            if ((View.DATA == view) && (instance.get().stamp() < value.stamp())) {
                Object newInstance = instance.get().get();
                if (schemaNode != null) {
                    Object type = schemaNode.get().getType();
                    if (type instanceof Class<?>) {
                        Class<?> cls = (Class<?>) type;
                        int mod = cls.getModifiers();
                        if (! ((type == Void.class) || Modifier.isInterface(mod) 
                                || Modifier.isAbstract(mod))) {
                            if (instance.get().stamp().equals(Long.valueOf(0L)) 
                                    || (result.stamp().compareTo(value.stamp()) < 0)) {
                                Records.DataGetter prev = (Records.DataGetter) result.get();
                                byte[] updated = ((Records.DataGetter) value.get()).getData();
                                if (instance.get().stamp().equals(Long.valueOf(0L))
                                        || (prev == null) || ! Arrays.equals(prev.getData(), updated)) {
                                    if (updated.length > 0) { 
                                        try {
                                            newInstance = codec().fromBytes(updated, cls);
                                        } catch (IOException e) {
                                            throw Throwables.propagate(e);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                StampedReference<Object> ref = StampedReference.of(value.stamp(), newInstance);
                instance.setIfGreater(ref);
            }
            return result;
        }
        
        @Override
        protected MaterializedNode newChild(ZNodeLabel.Component label) {
            ZNodeLabelTrie.Pointer<MaterializedNode> pointer = ZNodeLabelTrie.SimplePointer.of(label, this);
            Schema.SchemaNode node = (schemaNode != null) ? schemaNode.match(label) : null;
            return new MaterializedNode(Optional.of(pointer), node, codec);
        }
        
        @Override
        public String toString() {
            Map<View, String> viewStr = Maps.newHashMap();
            for (Map.Entry<View, StampedReference.Updater<? extends Records.ZNodeView>> entry: views.entrySet()) {
                viewStr.put(
                        entry.getKey(), 
                        String.format("(%s, %s)", 
                                entry.getValue().get().stamp(), 
                                Records.toString((Record) entry.getValue().get().get())));
            }
            return Objects.toStringHelper(this)
                    .add("path", path())
                    .add("children", keySet())
                    .add("instance", get())
                    .add("schema", schemaNode())
                    .add("stamp", stamp())
                    .add("views", viewStr).toString();
        }
    }

    public class Operator implements Reference<Materializer<V>> {
        
        public class Submitter<C extends Operations.Builder<? extends Records.Request>> implements Reference<C> {
            protected final C builder;
            protected final ClientExecutor<? super Records.Request, V> client;
            
            public Submitter(C builder, ClientExecutor<? super Records.Request, V> client) {
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
                create.setMode(node.get().getCreateMode()).setAcl(Schema.inheritedAcl(node));
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
        
        public Submitter<Operations.Requests.Sync> sync(ZNodeLabel.Path path) {
            return new Submitter<Operations.Requests.Sync>(
                    Operations.Requests.sync().setPath(path), get());
        }
    }
}

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

import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.Schema;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.StampedReference;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeLabelTrie;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.ISetDataRequest;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.Reference;

public class Materializer<T extends Operation.ProtocolRequest<?>, V extends Operation.ProtocolResponse<?>> extends ZNodeViewCache<Materializer.MaterializedNode, T, V> {

    public static <T extends Operation.ProtocolRequest<?>, V extends Operation.ProtocolResponse<?>> Materializer<T,V> newInstance(
            Schema schema, 
            Serializers.ByteCodec<Object> codec, 
            Publisher publisher,
            ClientExecutor<Operation.Request, T, V> client) {
        return new Materializer<T,V>(schema, codec, publisher, client);
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
    
    public static class Operator<T extends Operation.ProtocolRequest<?>, V extends Operation.ProtocolResponse<?>> implements Reference<Materializer<T,V>> {
        
        public static class Submitter<C extends Operations.Builder<? extends Operation.Request>, T extends Operation.ProtocolRequest<?>, V extends Operation.ProtocolResponse<?>> implements Reference<C> {
            protected final C builder;
            protected final ClientExecutor<Operation.Request, T, V> client;
            
            public Submitter(C builder, ClientExecutor<Operation.Request, T, V> client) {
                this.builder = builder;
                this.client = client;
            }
            
            @Override
            public C get() {
                return builder;
            }
            
            public ListenableFuture<Pair<T, V>> submit() {
                return client.submit(builder.build());
            }
        }
        
        protected final Materializer<T,V> materializer;
        
        public Operator(Materializer<T,V> materializer) {
            this.materializer = materializer;
        }

        @Override
        public Materializer<T,V> get() {
            return materializer;
        }

        public Submitter<Operations.Requests.SerializedData<Records.Request, Operations.Requests.Create, Object>, T, V> create(ZNodeLabel.Path path) {
            return create(path, null);
        }
        
        public Submitter<Operations.Requests.SerializedData<Records.Request, Operations.Requests.Create, Object>, T, V> create(ZNodeLabel.Path path, Object data) {
            Schema.SchemaNode node = get().schema().match(path);
            Operations.Requests.Create create = Operations.Requests.create().setPath(path);
            if (node != null) {
                create.setMode(node.get().getCreateMode()).setAcl(Schema.inheritedAcl(node));
            }
            return new Submitter<Operations.Requests.SerializedData<Records.Request, Operations.Requests.Create, Object>, T, V>(
                    Operations.Requests.serialized(create, get().codec(), data), get());
        }
        
        public Submitter<Operations.Requests.Delete, T, V> delete(ZNodeLabel.Path path) {
            return new Submitter<Operations.Requests.Delete, T, V>(
                    Operations.Requests.delete().setPath(path), get());
        }

        public Submitter<Operations.Requests.Exists, T, V> exists(ZNodeLabel.Path path) {
            return exists(path, false);
        }

        public Submitter<Operations.Requests.Exists, T, V> exists(ZNodeLabel.Path path, boolean watch) {
            return new Submitter<Operations.Requests.Exists, T, V>(
                    Operations.Requests.exists().setPath(path).setWatch(watch), get());
        }

        public Submitter<Operations.Requests.GetAcl, T, V> getAcl(ZNodeLabel.Path path) {
            return new Submitter<Operations.Requests.GetAcl, T, V>(
                    Operations.Requests.getAcl().setPath(path), get());
        }
        
        public Submitter<Operations.Requests.GetChildren, T, V> getChildren(ZNodeLabel.Path path) {
            return getChildren(path, false);
        }

        public Submitter<Operations.Requests.GetChildren, T, V> getChildren(ZNodeLabel.Path path, boolean watch) {
            return new Submitter<Operations.Requests.GetChildren, T, V>(
                    Operations.Requests.getChildren().setPath(path).setWatch(watch), get());
        }

        public Submitter<Operations.Requests.GetData, T, V> getData(ZNodeLabel.Path path) {
            return getData(path, false);
        }

        public Submitter<Operations.Requests.GetData, T, V> getData(ZNodeLabel.Path path, boolean watch) {
            return new Submitter<Operations.Requests.GetData, T, V>(
                    Operations.Requests.getData().setPath(path).setWatch(watch), get());
        }
        
        public Submitter<Operations.Requests.Multi, T, V> multi() {
            return new Submitter<Operations.Requests.Multi, T, V>(
                    Operations.Requests.multi(), get());
        }

        public Submitter<Operations.Requests.SetAcl, T, V> setAcl(ZNodeLabel.Path path) {
            return new Submitter<Operations.Requests.SetAcl, T, V>(
                    Operations.Requests.setAcl().setPath(path), get());
        }

        public Submitter<Operations.Requests.SerializedData<ISetDataRequest, Operations.Requests.SetData, Object>, T, V> setData(ZNodeLabel.Path path, Object data) {
            Operations.Requests.SetData setData = Operations.Requests.setData().setPath(path);
            return new Submitter<Operations.Requests.SerializedData<ISetDataRequest, Operations.Requests.SetData, Object>, T, V>(
                    Operations.Requests.serialized(setData, get().codec(), data), get());
        }
        
        public Submitter<Operations.Requests.Sync, T, V> sync(ZNodeLabel.Path path) {
            return new Submitter<Operations.Requests.Sync, T, V>(
                    Operations.Requests.sync().setPath(path), get());
        }
    }
    
    protected final Schema schema;
    protected final Serializers.ByteCodec<Object> codec;
    
    public Materializer(
            Schema schema, 
            Serializers.ByteCodec<Object> codec, 
            Publisher publisher,
            ClientExecutor<Operation.Request, T, V> client) {
        super(publisher, client, ZNodeLabelTrie.of(MaterializedNode.root(schema, codec)));
        this.schema = schema;
        this.codec = codec;
    }
    
    public Schema schema() {
        return schema;
    }
    
    public Serializers.ByteCodec<Object> codec() {
        return codec;
    }
    
    public Operator<T,V> operator() {
        return new Operator<T,V>(this);
    }
}

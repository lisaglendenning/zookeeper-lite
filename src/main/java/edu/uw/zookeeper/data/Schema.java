package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.checkArgument;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.zookeeper.CreateMode;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;

import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Reference;


public class Schema extends ZNodeLabelTrie<Schema.SchemaNode> {
    
    public static enum LabelType {
        NONE, LABEL, PATTERN;
    }
    
    public static class ZNodeSchema {
        
        public static class Builder {

            public static Builder from(Object obj) {
                Class<?> type;
                if (obj instanceof Class) {
                    type = (Class<?>) obj;
                } else {
                    type = obj.getClass();
                }
                String label = DEFAULT.getLabel();
                LabelType labelType = DEFAULT.getLabelType();
                CreateMode createMode = DEFAULT.getCreateMode();
                List<Acls.Acl> acl = DEFAULT.getAcl();
                ZNode annotation = type.getAnnotation(ZNode.class);
                if (annotation != null) {
                    createMode = annotation.createMode();
                    acl = annotation.acl().asList();
                    label = annotation.label();
                    labelType = annotation.labelType();

                    Label labelAnnotation = null;
                    
                    // Method label annotation overrides class and field
                    for (Method m: type.getMethods()) {
                        labelAnnotation = m.getAnnotation(Label.class);
                        if (labelAnnotation != null) {
                            labelType = labelAnnotation.type();
                            try {
                                label = (String) m.invoke(obj);
                            } catch (Exception e) {
                                throw Throwables.propagate(e);
                            }
                            break;
                        }
                    }
                    // Field Label annotation overrides class
                    if (labelAnnotation == null) {
                        for (Field f: type.getFields()) {
                            labelAnnotation = f.getAnnotation(Label.class);
                            if (labelAnnotation != null) {
                                labelType = labelAnnotation.type();
                                try {
                                    label = f.get(obj).toString();
                                } catch (IllegalAccessException e) {
                                    throw Throwables.propagate(e);
                                }
                                break;
                            }
                        }
                    }
                }
                return new Builder(label, labelType, createMode, acl, obj);
            }

            public static class ZNodeTraversal extends AbstractIterator<Pair<ZNodeLabel.Path, Builder>> {

                protected final LinkedList<Pair<ZNodeLabel.Path, ? extends Object>> pending;
                
                public ZNodeTraversal(Object root) {
                    this.pending = Lists.newLinkedList();
                    pending.add(Pair.create(ZNodeLabel.Path.root(), root));
                }

                @Override
                protected Pair<ZNodeLabel.Path, Builder> computeNext() {
                    if (! pending.isEmpty()) {
                        Pair<ZNodeLabel.Path, ? extends Object> next = pending.pop();
                        Class<?> type;
                        if (next.second() instanceof Class) {
                            type = (Class<?>) next.second();
                        } else {
                            type = next.second().getClass();
                        }
                        Builder builder = from(next.second());
                        ZNodeLabel.Path parentPath = ZNodeLabel.Path.of(next.first(), ZNodeLabel.of(builder.getLabel()));
                        for (Class<?> member: type.getClasses()) {
                            ZNode annotation = type.getAnnotation(ZNode.class);
                            if (annotation != null) {
                                pending.push(Pair.create(parentPath, member));
                            }
                        }
                        return Pair.create(next.first(), builder);
                    }
                    return endOfData();
                }
            }
            
            public static Iterator<Pair<ZNodeLabel.Path, Builder>> traverse(Object obj) {
                return new ZNodeTraversal(obj);
            }
            
            public static Builder newInstance() {
                return newInstance(DEFAULT);
            }

            public static Builder newInstance(ZNodeSchema schema) {
                return new Builder(schema.getLabel(), schema.getLabelType(), schema.getCreateMode(), schema.getAcl(), schema.getType());
            }
            
            protected static final ZNodeSchema DEFAULT = new ZNodeSchema("", LabelType.NONE, CreateMode.PERSISTENT, Acls.Definition.NONE.asList(), Void.class);
            
            protected String label;
            protected LabelType labelType;
            protected CreateMode createMode;
            protected List<Acls.Acl> acl;
            protected Object type;
            
            public Builder(
                    String label,
                    LabelType labelType, 
                    CreateMode createMode,
                    List<Acls.Acl> acl,
                    Object type) {
                this.label = label;
                this.labelType = labelType;
                this.createMode = createMode;
                this.acl = acl;
                this.type = type;
            }
            
            public String getLabel() {
                return label;
            }
            
            public Builder setLabel(String label) {
                this.label = label;
                return this;
            }

            public LabelType getLabelType() {
                return labelType;
            }
            
            public Builder setLabelType(LabelType labelType) {
                this.labelType = labelType;
                return this;
            }
            
            public CreateMode getCreateMode() {
                return createMode;
            }

            public Builder setCreateMode(CreateMode createMode) {
                this.createMode = createMode;
                return this;
            }
            
            public List<Acls.Acl> getAcl() {
                return acl;
            }

            public Builder setAcl(List<Acls.Acl> acl) {
                this.acl = acl;
                return this;
            }
            
            public Object getType() {
                return type;
            }
            
            public Builder setType(Object type) {
                this.type = type;
                return this;
            }
            
            public ZNodeSchema build() {
                return ZNodeSchema.newInstance(
                        getLabel(), getLabelType(), getCreateMode(), getAcl(), getType());
            }
        }
        
        public static ZNodeSchema getDefault() {
            return Builder.DEFAULT;
        }
        
        public static ZNodeSchema newInstance(
                String label,
                LabelType labelType, 
                CreateMode createMode,
                List<Acls.Acl> acl,
                Object type) {
            return new ZNodeSchema(label, labelType, createMode, acl, type);
        }
        
        protected final String label;
        protected final LabelType labelType;
        protected final CreateMode createMode;
        protected final List<Acls.Acl> acl;
        protected final Object type;
        
        protected ZNodeSchema(
                String label,
                LabelType labelType, 
                CreateMode createMode,
                List<Acls.Acl> acl,
                Object type) {
            this.label = label;
            this.labelType = labelType;
            this.createMode = createMode;
            this.acl = acl;
            this.type = type;
        }
        
        public String getLabel() {
            return label;
        }
        
        public LabelType getLabelType() {
            return labelType;
        }
        
        public CreateMode getCreateMode() {
            return createMode;
        }
        
        public List<Acls.Acl> getAcl() {
            return acl;
        }
        
        public Object getType() {
            return type;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("label", getLabel())
                    .add("labelType", getLabelType())
                    .add("createMode", getCreateMode())
                    .add("acl", getAcl())
                    .add("type", getType())
                    .toString();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (! (obj instanceof ZNodeSchema)) {
                return false;
            }
            ZNodeSchema other = (ZNodeSchema) obj;
            return Objects.equal(getLabel(), other.getLabel())
                    && Objects.equal(getLabelType(), other.getLabelType())
                    && Objects.equal(getCreateMode(), other.getCreateMode())
                    && Objects.equal(getAcl(), other.getAcl())
                    && Objects.equal(getType(), other.getType());
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(getLabel(), getLabelType(), getCreateMode(), getAcl(), getType());
        }
    }
    
    public static class SchemaNode extends ZNodeLabelTrie.AbstractNode<SchemaNode> implements Reference<ZNodeSchema> {

        public static SchemaNode root(ZNodeSchema schema) {
            return new SchemaNode(Optional.<Pointer<SchemaNode>>absent(), schema);
        }
        
        protected final ZNodeSchema schema;
        
        protected SchemaNode(Optional<ZNodeLabelTrie.Pointer<SchemaNode>> parent, ZNodeSchema schema) {
            super(parent);
            this.schema = schema;
        }
        
        @Override
        public ZNodeSchema get() {
            return schema;
        }
        
        public SchemaNode addSchema(ZNodeSchema schema) {
            return put(ZNodeLabel.Component.of(schema.getLabel()), schema);
        }
        
        public SchemaNode put(ZNodeLabel.Component label, ZNodeSchema schema) {
            checkArgument(label != null);
            SchemaNode child = children.get(label);
            if (child != null) {
                return child;
            }
            child = newChild(label, schema);
            SchemaNode prevChild = children.putIfAbsent(label, child);
            if (prevChild != null) {
                return prevChild;
            } else {
                return child;
            }
        }
        
        @Override
        protected SchemaNode newChild(ZNodeLabel.Component label) {
            return newChild(label, ZNodeSchema.getDefault());
        }

        protected SchemaNode newChild(ZNodeLabel.Component label, ZNodeSchema schema) {
            Pointer<SchemaNode> childPointer = SimplePointer.of(label, this);
            return new SchemaNode(Optional.of(childPointer), schema);
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("path", path())
                    .add("children", children.keySet())
                    .add("schema", get())
                    .toString();
        }
    }
    
    public static Schema of(ZNodeSchema root) {
        return new Schema(SchemaNode.root(root));
    }
    
    protected Schema(SchemaNode root) {
        super(root);
    }

    public SchemaNode addSchema(ZNodeSchema schema) {
        return put(ZNodeLabel.Path.of(schema.getLabel()), schema);
    }
    
    public SchemaNode put(ZNodeLabel.Path path, ZNodeSchema schema) {
        ZNodeLabel parentLabel = path.head();
        if (parentLabel == null) {
            return root();
        }
        SchemaNode parent;
        if (! (parentLabel instanceof ZNodeLabel.Path)) {
            parent = root();
        } else {
            parent = put((ZNodeLabel.Path)parentLabel);
        }
        
        SchemaNode next = parent.put(path.tail(), schema);
        return next;
    }
}

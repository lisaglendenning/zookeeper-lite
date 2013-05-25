package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.checkArgument;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.CreateMode;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Reference;


public class Schema extends ZNodeLabelTrie<Schema.SchemaNode> {
    
    public static enum LabelType {
        NONE, LABEL, PATTERN;
    }
    
    public static class ZNodeSchema {
        
        public static class Builder {

            public static Builder fromDefault() {
                return fromSchema(DEFAULT);
            }

            public static Builder fromSchema(ZNodeSchema schema) {
                return new Builder(schema.getLabel(), schema.getLabelType(), schema.getCreateMode(), schema.getAcl(), schema.getType());
            }

            public static Builder fromAnnotated(AnnotatedElement element) {
                ZNode annotation = element.getAnnotation(ZNode.class);
                if (annotation == null) {
                    return null;
                }
                Builder builder = fromAnnotation(annotation);
                if (Void.class == builder.getType()) {
                    builder.setType(element);
                }
                return builder;
            }
            
            public static Builder fromAnnotation(ZNode annotation) {
                String label = annotation.label();
                LabelType labelType = annotation.labelType();
                CreateMode createMode = annotation.createMode();
                List<Acls.Acl> acl = annotation.acl().asList();
                Object type = annotation.type();
                return new Builder(label, labelType, createMode, acl, type);
            }
            
            public static Builder fromClass(Object obj) {
                Class<?> type = (obj instanceof Class) ? (Class<?>)obj : obj.getClass();
                Builder builder = fromAnnotated(type);
                if (builder == null) {
                    return null;
                }
                if (type != obj) {
                    builder.setType(obj);
                }
                
                Pair<Label, ? extends Member> memberLabel = null;
                
                // Method label annotation overrides class and field
                Method[][] allMethods = {type.getDeclaredMethods(), type.getMethods()};
                for (Method[] methods: allMethods) {
                    if (memberLabel != null) {
                        break;
                    }
                    for (Method m: methods) {
                        Label labelAnnotation = m.getAnnotation(Label.class);
                        if (labelAnnotation != null) {
                            memberLabel = Pair.create(labelAnnotation, m);
                            break;
                        }
                    }
                }
                
                // Field Label annotation overrides class
                Field[][] allFields = {type.getDeclaredFields(), type.getFields()};
                for (Field[] fields: allFields) {
                    if (memberLabel != null) {
                        break;
                    }
                    for (Field f: fields) {
                        Label labelAnnotation = f.getAnnotation(Label.class);
                        if (labelAnnotation != null) {
                            memberLabel = Pair.create(labelAnnotation, f);
                            break;
                        }
                    }
                }

                if (memberLabel != null) {
                    builder.setLabelType(memberLabel.first().type());
                    Member member = memberLabel.second();
                    try {
                        builder.setLabel(
                                (member instanceof Method) 
                                ? (String) ((Method)member).invoke(obj)
                                : ((Field)member).get(obj).toString());
                    } catch (Exception e) {
                        throw Throwables.propagate(e);
                    }
                }
                
                return builder;
            }
            
            public static <T extends Member & AnnotatedElement> Builder fromAnnotatedMember(T member) {
                Builder builder = fromAnnotated(member);
                if (builder == null) {
                    return null;
                }
                if (builder.getLabel().length() == 0) {
                    builder.setLabel(member.getName());
                }
                return builder;
            }
            
            public static Iterator<Pair<ZNodeLabel.Path, Builder>> traverse(Object obj) {
                return new ZNodeTraversal(obj);
            }

            public static class ZNodeTraversal extends AbstractIterator<Pair<ZNodeLabel.Path, Builder>> {

                protected final LinkedList<Pair<ZNodeLabel.Path, Builder>> pending;
                
                public ZNodeTraversal(Object root) {
                    this.pending = Lists.newLinkedList();
                    Builder builder = fromClass(root);
                    if (builder != null) {
                        pending.add(Pair.create(ZNodeLabel.Path.root(), builder));
                    }
                }

                @Override
                protected Pair<ZNodeLabel.Path, Builder> computeNext() {
                    if (! pending.isEmpty()) {
                        Pair<ZNodeLabel.Path, Builder> next = pending.pop();
                        ZNodeLabel.Path path = next.first();
                        Builder parent = next.second();
                        if (parent.getLabel().length() > 0) {
                            path = ZNodeLabel.Path.of(next.first(), ZNodeLabel.of(parent.getLabel()));
                        }
                        
                        Object obj = parent.getType();
                        Class<?> type = (obj instanceof Class) ? (Class<?>)obj : obj.getClass();
                        
                        Set<Field> fields = Sets.newHashSet(type.getDeclaredFields());
                        fields.addAll(Arrays.asList(type.getFields()));
                        for (Field f: fields) {
                            Builder builder = fromAnnotatedMember(f);
                            if (builder != null) {
                                pending.push(Pair.create(path, builder));
                            }
                        }

                        Set<Method> methods = Sets.newHashSet(type.getDeclaredMethods());
                        methods.addAll(Arrays.asList(type.getMethods()));
                        for (Method m: methods) {
                            Builder builder = fromAnnotatedMember(m);
                            if (builder != null) {
                                pending.push(Pair.create(path, builder));
                            }
                        }
                        
                        for (Class<?> member: type.getClasses()) {
                            Builder builder = fromClass(member);
                            if (builder != null) {
                                pending.push(Pair.create(path, builder));
                            }
                        }
                        
                        return next;
                    }
                    return endOfData();
                }
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
                return ZNodeSchema.of(
                        getLabel(), getLabelType(), getCreateMode(), getAcl(), getType());
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
        }
        
        public static ZNodeSchema getDefault() {
            return Builder.DEFAULT;
        }
        
        public static ZNodeSchema of(
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
    
    public static List<Acls.Acl> inheritedAcl(Schema.SchemaNode node) {
        Iterator<Schema.SchemaNode> itr = ZNodeLabelTrie.parentIterator(node);
        List<Acls.Acl> none = Acls.Definition.NONE.asList();
        List<Acls.Acl> acl = none;
        while (itr.hasNext()) {
            Schema.SchemaNode next = itr.next();
            acl = next.get().getAcl();
            if (! none.equals(acl)) {
                break;
            }
        }
        return acl;
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

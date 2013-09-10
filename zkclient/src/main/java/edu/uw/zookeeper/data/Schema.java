package edu.uw.zookeeper.data;


import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.data.ZNodeLabel.Component;


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
                    if (element instanceof Field) {
                        builder.setType(((Field) element).getType());
                    } else if (element instanceof Method) {
                        builder.setType(((Method) element).getReturnType());
                    } else {
                        builder.setType(element);
                    }
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
            
            public static Iterator<ZNodeTraversal.Element> traverse(Object obj) {
                return new ZNodeTraversal(obj);
            }

            public static class ZNodeTraversal extends AbstractIterator<ZNodeTraversal.Element> {

                public static class Element {
                    private final ZNodeLabel.Path path;
                    private final Builder builder;
                    private final Object element;
                    
                    public Element(ZNodeLabel.Path path, Builder builder, Object element) {
                        this.path = path;
                        this.builder = builder;
                        this.element = element;
                    }

                    public ZNodeLabel.Path getPath() {
                        return path;
                    }

                    public Builder getBuilder() {
                        return builder;
                    }

                    public Object getElement() {
                        return element;
                    }
                }
                
                protected final LinkedList<Element> pending;
                
                public ZNodeTraversal(Object root) {
                    this.pending = Lists.newLinkedList();
                    Builder builder = fromClass(root);
                    if (builder != null) {
                        pending.add(new Element(ZNodeLabel.Path.root(), builder, root));
                    }
                }

                @Override
                protected Element computeNext() {
                    if (! pending.isEmpty()) {
                        Element next = pending.pop();
                        ZNodeLabel.Path path = next.path;
                        Builder parent = next.builder;
                        if (parent.getLabel().length() > 0) {
                            path = (ZNodeLabel.Path) ZNodeLabel.joined(path, parent.getLabel());
                        }
                        
                        Object obj = parent.getType();
                        Class<?> type = (obj instanceof Class) ? (Class<?>)obj : obj.getClass();
                        
                        Set<Field> fields = Sets.newHashSet(type.getDeclaredFields());
                        fields.addAll(Arrays.asList(type.getFields()));
                        for (Field f: fields) {
                            Builder builder = fromAnnotatedMember(f);
                            if (builder != null) {
                                pending.push(new Element(path, builder, f));
                            }
                        }

                        Set<Method> methods = Sets.newHashSet(type.getDeclaredMethods());
                        methods.addAll(Arrays.asList(type.getMethods()));
                        for (Method m: methods) {
                            Builder builder = fromAnnotatedMember(m);
                            if (builder != null) {
                                pending.push(new Element(path, builder, m));
                            }
                        }
                        
                        for (Class<?> c: type.getClasses()) {
                            Builder builder = fromClass(c);
                            if (builder != null) {
                                pending.push(new Element(path, builder, c));
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

        public static SchemaNode child(ZNodeLabel.Component label, SchemaNode parent) {
            return child(label, parent, ZNodeSchema.getDefault());
        }
        
        public static SchemaNode child(ZNodeLabel.Component label, SchemaNode parent, ZNodeSchema schema) {
            Pointer<SchemaNode> childPointer = SimplePointer.of(label, parent);
            return new SchemaNode(Optional.of(childPointer), schema);
        }

        protected final ZNodeSchema schema;
        
        protected SchemaNode(Optional<ZNodeLabelTrie.Pointer<SchemaNode>> parent, ZNodeSchema schema) {
            super(parent, new ConcurrentSkipListMap<ZNodeLabel.Component, SchemaNode>());
            this.schema = schema;
        }

        @Override
        protected ConcurrentSkipListMap<ZNodeLabel.Component, SchemaNode> delegate() {
            return (ConcurrentSkipListMap<Component, SchemaNode>) children;
        }
        
        @Override
        public ZNodeSchema get() {
            return schema;
        }
        
        public SchemaNode match(ZNodeLabel.Component label) {
            SchemaNode child = get(label);
            if (child == null) {
                String labelString = label.toString();
                for (Map.Entry<ZNodeLabel.Component, Schema.SchemaNode> entry: entrySet()) {
                    if (labelString.matches(entry.getKey().toString())) {
                        child = entry.getValue();
                        break;
                    }
                }
            }
            return child;
        }
        
        public SchemaNode add(ZNodeSchema schema) {
            return add(ZNodeLabel.Component.of(schema.getLabel()), schema);
        }
        
        public SchemaNode add(ZNodeLabel.Component label, ZNodeSchema schema) {
            delegate().putIfAbsent(label, child(label, this, schema));
            return get(label);
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

    public SchemaNode match(ZNodeLabel.Path path) {
        SchemaNode next = root();
        for (ZNodeLabel.Component component: path) {
            next = next.match(component);
            if (next == null) {
                break;
            }
        }
        return next;
    }

    public SchemaNode add(ZNodeSchema schema) {
        return add(ZNodeLabel.Path.of(schema.getLabel()), schema);
    }
    
    public SchemaNode add(ZNodeLabel.Path path, ZNodeSchema schema) {
        SchemaNode parent = get(path.head());
        if (parent == null) {
            throw new IllegalStateException();
        }
        return parent.add((ZNodeLabel.Component) path.tail(), schema);
    }
}

package edu.uw.zookeeper.data;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import edu.uw.zookeeper.common.Pair;

public class ZNodeSchema {

    public static class Builder {

        public static ZNodeSchema.Builder fromDefault() {
            return fromSchema(DEFAULT);
        }

        public static ZNodeSchema.Builder fromSchema(ZNodeSchema schema) {
            return new Builder(schema.getLabel(), schema.getLabelType(), schema.getCreateMode(), schema.getAcl(), schema.getType());
        }

        public static ZNodeSchema.Builder fromAnnotated(AnnotatedElement element) {
            ZNode annotation = element.getAnnotation(ZNode.class);
            if (annotation == null) {
                return null;
            }
            ZNodeSchema.Builder builder = fromAnnotation(annotation);
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
        
        public static ZNodeSchema.Builder fromAnnotation(ZNode annotation) {
            String label = annotation.label();
            LabelType labelType = annotation.labelType();
            CreateMode createMode = annotation.createMode();
            List<Acls.Acl> acl = annotation.acl().asList();
            Object type = annotation.type();
            return new Builder(label, labelType, createMode, acl, type);
        }
        
        public static ZNodeSchema.Builder fromClass(Object obj) {
            Class<?> type = (obj instanceof Class) ? (Class<?>)obj : obj.getClass();
            ZNodeSchema.Builder builder = fromAnnotated(type);
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
        
        public static <T extends Member & AnnotatedElement> ZNodeSchema.Builder fromAnnotatedMember(T member) {
            ZNodeSchema.Builder builder = fromAnnotated(member);
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
                private final ZNodeLabelVector path;
                private final ZNodeSchema.Builder builder;
                private final Object element;
                
                public Element(ZNodeLabelVector path, ZNodeSchema.Builder builder, Object element) {
                    this.path = path;
                    this.builder = builder;
                    this.element = element;
                }

                public ZNodeLabelVector getPath() {
                    return path;
                }

                public ZNodeSchema.Builder getBuilder() {
                    return builder;
                }

                public Object getElement() {
                    return element;
                }
            }
            
            protected final LinkedList<ZNodeTraversal.Element> pending;
            
            public ZNodeTraversal(Object root) {
                this.pending = Lists.newLinkedList();
                ZNodeSchema.Builder builder = fromClass(root);
                if (builder != null) {
                    pending.add(new Element(RootZNodePath.getInstance(), builder, root));
                }
            }

            @Override
            protected ZNodeTraversal.Element computeNext() {
                if (! pending.isEmpty()) {
                    ZNodeTraversal.Element next = pending.pop();
                    ZNodeLabelVector path = next.path;
                    ZNodeSchema.Builder parent = next.builder;
                    if (parent.getLabel().length() > 0) {
                        path = path.join(ZNodeLabel.fromString(parent.getLabel()));
                    }
                    
                    Object obj = parent.getType();
                    Class<?> type = (obj instanceof Class) ? (Class<?>)obj : obj.getClass();
                    
                    Set<Field> fields = Sets.newHashSet(type.getDeclaredFields());
                    fields.addAll(Arrays.asList(type.getFields()));
                    for (Field f: fields) {
                        ZNodeSchema.Builder builder = fromAnnotatedMember(f);
                        if (builder != null) {
                            pending.push(new Element(path, builder, f));
                        }
                    }

                    Set<Method> methods = Sets.newHashSet(type.getDeclaredMethods());
                    methods.addAll(Arrays.asList(type.getMethods()));
                    for (Method m: methods) {
                        ZNodeSchema.Builder builder = fromAnnotatedMember(m);
                        if (builder != null) {
                            pending.push(new Element(path, builder, m));
                        }
                    }
                    
                    for (Class<?> c: type.getClasses()) {
                        ZNodeSchema.Builder builder = fromClass(c);
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
        
        public ZNodeSchema.Builder setLabel(String label) {
            this.label = label;
            return this;
        }

        public LabelType getLabelType() {
            return labelType;
        }
        
        public ZNodeSchema.Builder setLabelType(LabelType labelType) {
            this.labelType = labelType;
            return this;
        }
        
        public CreateMode getCreateMode() {
            return createMode;
        }

        public ZNodeSchema.Builder setCreateMode(CreateMode createMode) {
            this.createMode = createMode;
            return this;
        }
        
        public List<Acls.Acl> getAcl() {
            return acl;
        }

        public ZNodeSchema.Builder setAcl(List<Acls.Acl> acl) {
            this.acl = acl;
            return this;
        }
        
        public Object getType() {
            return type;
        }
        
        public ZNodeSchema.Builder setType(Object type) {
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
        return new ZNodeSchema(label, labelType, createMode, ImmutableList.copyOf(acl), type);
    }
    
    private final String label;
    private final LabelType labelType;
    private final CreateMode createMode;
    private final List<Acls.Acl> acl;
    private final Object type;
    
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
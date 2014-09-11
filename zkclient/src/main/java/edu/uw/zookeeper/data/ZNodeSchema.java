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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import edu.uw.zookeeper.common.Pair;

public class ZNodeSchema {
    
    public static <E extends NameTrie.Node<E>> E matchPath(NameTrie<E> trie, ZNodePath path) {
        Iterator<ZNodeLabel> remaining = path.iterator();
        E node = trie.root();
        while (remaining.hasNext() && (node != null)) {
            ZNodeLabel next = remaining.next();
            node = matchChild(node, next);
        }
        return node;
    }

    public static <E extends NameTrie.Node<E>> E matchChild(E node, ZNodeName name) {
        E child = node.get(name);
        if (child == null) {
            final String labelString = name.toString();
            for (Map.Entry<ZNodeName, E> entry: node.entrySet()) {
                if (labelString.matches(entry.getKey().toString())) {
                    child = entry.getValue();
                    break;
                }
            }
        }
        return child;
    }

    public static List<Acls.Acl> inheritedAcl(ValueNode<ZNodeSchema> node) {
        List<Acls.Acl> none = Acls.Definition.NONE.asList();
        List<Acls.Acl> acl = node.get().getAcl();
        if (none.equals(acl)) {
            Iterator<NameTrie.Pointer<? extends ValueNode<ZNodeSchema>>> itr = SimpleLabelTrie.parentIterator(node.parent());
            while (itr.hasNext()) {
                ValueNode<ZNodeSchema> next = itr.next().get();
                acl = next.get().getAcl();
                if (! none.equals(acl)) {
                    break;
                }
            }
        }
        return acl;
    }

    public static Iterator<DeclarationTraversal.Element> fromHierarchy(Class<?> root) {
        return new DeclarationTraversal(root);
    }

    public static class DeclarationTraversal extends AbstractIterator<DeclarationTraversal.Element> {
    
        public static class Element {
            private final ZNodePath path;
            private final ZNodeSchema.Builder builder;
            private final Object element;
            
            public Element(ZNodePath path, ZNodeSchema.Builder builder, Object element) {
                this.path = path;
                this.builder = builder;
                this.element = element;
            }
    
            public ZNodePath getPath() {
                return path;
            }
    
            public ZNodeSchema.Builder getBuilder() {
                return builder;
            }
    
            public Object getElement() {
                return element;
            }
        }
        
        protected final LinkedList<DeclarationTraversal.Element> pending;
        
        public DeclarationTraversal(Class<?> root) {
            this.pending = Lists.newLinkedList();
            ZNodeSchema.Builder builder = Builder.fromClass(root);
            if (builder != null) {
                pending.add(new Element(RootZNodePath.getInstance(), builder, root));
            }
        }
    
        @Override
        protected DeclarationTraversal.Element computeNext() {
            if (pending.isEmpty()) {
                return endOfData();
            } else {
                DeclarationTraversal.Element next = pending.pop();
                ZNodePath path = next.path;
                ZNodeSchema.Builder parent = next.builder;
                if (parent.getName().length() > 0) {
                    path = path.join(ZNodeLabel.fromString(parent.getName()));
                }
                
                Object obj = parent.getDeclaration();
                Class<?> type = (obj instanceof Class) ? (Class<?>)obj : obj.getClass();
                
                Set<Field> fields = Sets.newHashSet(type.getFields());
                fields.addAll(Arrays.asList(type.getFields()));
                for (Field f: fields) {
                    ZNodeSchema.Builder builder = Builder.fromAnnotatedMember(f);
                    if (builder != null) {
                        pending.push(new Element(path, builder, f));
                    }
                }
    
                Set<Method> methods = Sets.newHashSet(type.getMethods());
                methods.addAll(Arrays.asList(type.getMethods()));
                for (Method m: methods) {
                    ZNodeSchema.Builder builder = Builder.fromAnnotatedMember(m);
                    if (builder != null) {
                        pending.push(new Element(path, builder, m));
                    }
                }
                
                for (Class<?> c: type.getClasses()) {
                    ZNodeSchema.Builder builder = Builder.fromClass(c);
                    if (builder != null) {
                        pending.push(new Element(path, builder, c));
                    }
                }
                
                return next;
            }
        }
    }

    public static class Builder {

        public static ZNodeSchema.Builder fromDefault() {
            return fromSchema(DEFAULT);
        }

        public static ZNodeSchema.Builder fromSchema(ZNodeSchema schema) {
            return new Builder(schema.getDeclaration(), schema.getName(), schema.getNameType(), schema.getCreateMode(), schema.getAcl(), schema.getDataType());
        }

        public static ZNodeSchema.Builder fromAnnotation(Object declaration, ZNode annotation) {
            String label = annotation.name();
            NameType labelType = annotation.nameType();
            CreateMode createMode = annotation.createMode();
            List<Acls.Acl> acl = annotation.acl().asList();
            Class<?> dataType = annotation.dataType();
            return new Builder(declaration, label, labelType, createMode, acl, dataType);
        }

        public static ZNodeSchema.Builder fromAnnotated(AnnotatedElement element) {
            ZNode annotation = element.getAnnotation(ZNode.class);
            if (annotation == null) {
                return null;
            }
            ZNodeSchema.Builder builder = fromAnnotation(element, annotation);
            if (Void.class == builder.getDataType()) {
                if (element instanceof Field) {
                    builder.setDataType(((Field) element).getType());
                } else if (element instanceof Method) {
                    builder.setDataType(((Method) element).getReturnType());
                }
            }
            return builder;
        }
        
        public static ZNodeSchema.Builder fromClass(Class<?> cls) {
            ZNodeSchema.Builder builder = fromAnnotated(cls);
            if (builder == null) {
                return null;
            }
            
            Pair<Name, ? extends Member> memberName = null;
            
            // Method Name annotation overrides class and field
            Method[][] allMethods = {cls.getDeclaredMethods(), cls.getMethods()};
            for (Method[] methods: allMethods) {
                if (memberName != null) {
                    break;
                }
                for (Method m: methods) {
                    Name annotation = m.getAnnotation(Name.class);
                    if (annotation != null) {
                        memberName = Pair.create(annotation, m);
                        break;
                    }
                }
            }
            
            // Field Name annotation overrides class
            Field[][] allFields = {cls.getDeclaredFields(), cls.getFields()};
            for (Field[] fields: allFields) {
                if (memberName != null) {
                    break;
                }
                for (Field f: fields) {
                    Name annotation = f.getAnnotation(Name.class);
                    if (annotation != null) {
                        memberName = Pair.create(annotation, f);
                        break;
                    }
                }
            }

            if (memberName != null) {
                builder.setNameType(memberName.first().type());
                Member member = memberName.second();
                try {
                    builder.setName(
                            (member instanceof Method) 
                            ? (String) ((Method)member).invoke(cls)
                            : ((Field)member).get(cls).toString());
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
            if (builder.getName().length() == 0) {
                builder.setName(member.getName());
            }
            return builder;
        }
        
        protected static final ZNodeSchema DEFAULT = new ZNodeSchema(null, "", NameType.NONE, CreateMode.PERSISTENT, Acls.Definition.NONE.asList(), Void.class);

        protected Object declaration;
        protected String name;
        protected NameType labelType;
        protected CreateMode createMode;
        protected List<Acls.Acl> acl;
        protected Class<?> dataType;
        
        public Builder(
                Object declaration,
                String name,
                NameType labelType, 
                CreateMode createMode,
                List<Acls.Acl> acl,
                Class<?> dataType) {
            this.declaration = declaration;
            this.name = name;
            this.labelType = labelType;
            this.createMode = createMode;
            this.acl = acl;
            this.dataType = dataType;
        }

        public Object getDeclaration() {
            return declaration;
        }
        
        public ZNodeSchema.Builder setDeclaration(Object declaration) {
            this.declaration = declaration;
            return this;
        }
        
        public String getName() {
            return name;
        }
        
        public ZNodeSchema.Builder setName(String label) {
            this.name = label;
            return this;
        }

        public NameType getNameType() {
            return labelType;
        }
        
        public ZNodeSchema.Builder setNameType(NameType labelType) {
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

        public Class<?> getDataType() {
            return dataType;
        }
        
        public ZNodeSchema.Builder setDataType(Class<?> dataType) {
            this.dataType = dataType;
            return this;
        }
        
        public ZNodeSchema build() {
            return ZNodeSchema.create(
                    getDeclaration(), getName(), getNameType(), getCreateMode(), getAcl(), getDataType());
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("declaration", getDeclaration())
                    .add("name", getName())
                    .add("nameType", getNameType())
                    .add("createMode", getCreateMode())
                    .add("acl", getAcl())
                    .add("dataType", getDataType())
                    .toString();
        }
    }
    
    public static ZNodeSchema getDefault() {
        return Builder.DEFAULT;
    }
    
    public static ZNodeSchema create(
            Object declaration,
            String name,
            NameType nameType, 
            CreateMode createMode,
            List<Acls.Acl> acl,
            Class<?> dataType) {
        return new ZNodeSchema(declaration, name, nameType, createMode, ImmutableList.copyOf(acl), dataType);
    }

    private final Object declaration;
    private final String name;
    private final NameType nameType;
    private final CreateMode createMode;
    private final List<Acls.Acl> acl;
    private final Class<?> dataType;
    
    protected ZNodeSchema(
            Object declaration,
            String name,
            NameType nameType, 
            CreateMode createMode,
            List<Acls.Acl> acl,
            Class<?> dataType) {
        this.declaration = declaration;
        this.name = name;
        this.nameType = nameType;
        this.createMode = createMode;
        this.acl = acl;
        this.dataType = dataType;
    }

    public Object getDeclaration() {
        return declaration;
    }
    
    public String getName() {
        return name;
    }
    
    public NameType getNameType() {
        return nameType;
    }
    
    public CreateMode getCreateMode() {
        return createMode;
    }
    
    public List<Acls.Acl> getAcl() {
        return acl;
    }
    
    public Class<?> getDataType() {
        return dataType;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("declaration", getDeclaration())
                .add("name", getName())
                .add("nameType", getNameType())
                .add("createMode", getCreateMode())
                .add("acl", getAcl())
                .add("dataType", getDataType())
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
        return Objects.equal(getDeclaration(), other.getDeclaration())
                && Objects.equal(getName(), other.getName())
                && Objects.equal(getNameType(), other.getNameType())
                && Objects.equal(getCreateMode(), other.getCreateMode())
                && Objects.equal(getAcl(), other.getAcl())
                && Objects.equal(getDataType(), other.getDataType());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getDeclaration(), getName());
    }
}
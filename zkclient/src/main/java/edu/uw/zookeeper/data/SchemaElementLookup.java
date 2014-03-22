package edu.uw.zookeeper.data;

import java.util.Iterator;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;

import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.ValueNode;
import edu.uw.zookeeper.data.SimpleLabelTrie;
import edu.uw.zookeeper.data.ZNodeSchema;
import edu.uw.zookeeper.data.ZNodeSchema.DeclarationTraversal;
import edu.uw.zookeeper.data.ZNodePath;

public class SchemaElementLookup extends Pair<NameTrie<ValueNode<ZNodeSchema>>, Map<Object, ValueNode<ZNodeSchema>>> implements Reference<NameTrie<ValueNode<ZNodeSchema>>>, Function<Object, ValueNode<ZNodeSchema>> {
        
    public static SchemaElementLookup fromHierarchy(Class<?> root) {
        NameTrie<ValueNode<ZNodeSchema>> schema = null;
        Iterator<DeclarationTraversal.Element> itr = 
                ZNodeSchema.fromHierarchy(root);
        ImmutableMap.Builder<Object, ValueNode<ZNodeSchema>> byElement = ImmutableMap.builder();
        while (itr.hasNext()) {
            DeclarationTraversal.Element next = itr.next();
            ZNodeSchema nextSchema = next.getBuilder().build();
            ZNodeName name = ZNodeName.fromString(nextSchema.getName());
            ZNodePath path = next.getPath().join(name);
            if (schema == null) {
                ValueNode<ZNodeSchema> node;
                if (path.isRoot()) {
                    node = ValueNode.root(nextSchema);
                } else {
                    node = ValueNode.root(ZNodeSchema.getDefault());
                }
                schema = SimpleLabelTrie.forRoot(node);
            }
            ValueNode<ZNodeSchema> node;
            if (path.isRoot()) {
                node = schema.root();
            } else {
                ValueNode<ZNodeSchema> parent = schema.get(next.getPath());
                node = ValueNode.child(name, parent, nextSchema);
                parent.put(node.parent().name(), node);
            }
            byElement.put(next.getElement(), node);
        }
        return new SchemaElementLookup(schema, byElement.build());
    }
    
    protected SchemaElementLookup(NameTrie<ValueNode<ZNodeSchema>> schema, Map<Object, ValueNode<ZNodeSchema>> byElement) {
        super(schema, byElement);
    }

    @Override
    public NameTrie<ValueNode<ZNodeSchema>> get() {
        return first;
    }

    @Override
    public ValueNode<ZNodeSchema> apply(Object type) {
        return second.get(type);
    }
}

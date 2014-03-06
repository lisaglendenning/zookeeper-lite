package edu.uw.zookeeper.data;


import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;


public class SchemaNode extends SimpleNameTrie.SimpleNode<SchemaNode> {
    
    public static SchemaNode root(ZNodeSchema schema) {
        return new SchemaNode(SimpleNameTrie.<SchemaNode>rootPointer(), schema);
    }

    public static SchemaNode child(SchemaNode parent, ZNodeSchema schema) {
        return child(ZNodeName.fromString(schema.getLabel()), parent, schema);
    }

    public static SchemaNode child(ZNodeName label, SchemaNode parent, ZNodeSchema schema) {
        NameTrie.Pointer<SchemaNode> childPointer = SimpleNameTrie.weakPointer(label, parent);
        return new SchemaNode(childPointer, schema);
    }

    public static SchemaNode match(NameTrie<SchemaNode> trie, ZNodePath path) {
        Iterator<ZNodeLabel> remaining = path.iterator();
        SchemaNode node = trie.root();
        while (remaining.hasNext() && (node != null)) {
            ZNodeLabel component = remaining.next();
            node = SchemaNode.match(node, component);
        }
        return node;
    }

    public static SchemaNode match(SchemaNode node, ZNodeName label) {
        SchemaNode child = node.get(label);
        if (child == null) {
            String labelString = label.toString();
            for (Map.Entry<ZNodeName, SchemaNode> entry: node.entrySet()) {
                if (labelString.matches(entry.getKey().toString())) {
                    child = entry.getValue();
                    break;
                }
            }
        }
        return child;
    }

    public static List<Acls.Acl> inheritedAcl(SchemaNode node) {
        List<Acls.Acl> none = Acls.Definition.NONE.asList();
        List<Acls.Acl> acl = node.schema.getAcl();
        if (none.equals(acl)) {
            Iterator<NameTrie.Pointer<? extends SchemaNode>> itr = SimpleNameTrie.parentIterator(node.parent());
            while (itr.hasNext()) {
                SchemaNode next = itr.next().get();
                acl = next.schema().getAcl();
                if (! none.equals(acl)) {
                    break;
                }
            }
        }
        return acl;
    }
        
    private final ZNodeSchema schema;
    
    protected SchemaNode(NameTrie.Pointer<SchemaNode> parent, ZNodeSchema schema) {
        super(SimpleNameTrie.pathOf(parent), parent, Maps.<ZNodeName, SchemaNode>newHashMap());
        this.schema = schema;
    }

    public ZNodeSchema schema() {
        return schema;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper("")
                .add("path", path())
                .add("children", keySet())
                .add("schema", schema())
                .toString();
    }
}

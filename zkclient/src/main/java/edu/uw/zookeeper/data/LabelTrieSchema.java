package edu.uw.zookeeper.data;


import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;


public class LabelTrieSchema extends SimpleNameTrie.SimpleNode<LabelTrieSchema> {
    
    public static LabelTrieSchema root(ZNodeSchema schema) {
        return new LabelTrieSchema(SimpleNameTrie.<LabelTrieSchema>rootPointer(), schema);
    }

    public static LabelTrieSchema child(LabelTrieSchema parent, ZNodeSchema schema) {
        return child(ZNodeName.fromString(schema.getLabel()), parent, schema);
    }

    public static LabelTrieSchema child(ZNodeName label, LabelTrieSchema parent, ZNodeSchema schema) {
        NameTrie.Pointer<LabelTrieSchema> childPointer = SimpleNameTrie.weakPointer(label, parent);
        return new LabelTrieSchema(childPointer, schema);
    }

    public static LabelTrieSchema match(NameTrie<LabelTrieSchema> trie, ZNodePath path) {
        Iterator<ZNodeLabel> remaining = path.iterator();
        LabelTrieSchema node = trie.root();
        while (remaining.hasNext() && (node != null)) {
            ZNodeLabel component = remaining.next();
            node = LabelTrieSchema.match(node, component);
        }
        return node;
    }

    public static LabelTrieSchema match(LabelTrieSchema node, ZNodeName label) {
        LabelTrieSchema child = node.get(label);
        if (child == null) {
            String labelString = label.toString();
            for (Map.Entry<ZNodeName, LabelTrieSchema> entry: node.entrySet()) {
                if (labelString.matches(entry.getKey().toString())) {
                    child = entry.getValue();
                    break;
                }
            }
        }
        return child;
    }

    public static List<Acls.Acl> inheritedAcl(LabelTrieSchema node) {
        List<Acls.Acl> none = Acls.Definition.NONE.asList();
        List<Acls.Acl> acl = node.schema.getAcl();
        if (none.equals(acl)) {
            Iterator<NameTrie.Pointer<? extends LabelTrieSchema>> itr = SimpleNameTrie.parentIterator(node.parent());
            while (itr.hasNext()) {
                LabelTrieSchema next = itr.next().get();
                acl = next.schema().getAcl();
                if (! none.equals(acl)) {
                    break;
                }
            }
        }
        return acl;
    }
        
    private final ZNodeSchema schema;
    
    protected LabelTrieSchema(NameTrie.Pointer<LabelTrieSchema> parent, ZNodeSchema schema) {
        super(SimpleNameTrie.pathOf(parent), parent, Maps.<ZNodeName, LabelTrieSchema>newHashMap());
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

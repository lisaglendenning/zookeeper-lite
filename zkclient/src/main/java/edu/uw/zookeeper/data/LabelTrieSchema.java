package edu.uw.zookeeper.data;


import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;

import edu.uw.zookeeper.data.ZNodePath.AbsoluteZNodePath;


public class LabelTrieSchema extends SimpleLabelTrie.SimpleNode<LabelTrieSchema> {
    
    public static LabelTrieSchema root(ZNodeSchema schema) {
        return new LabelTrieSchema(SimpleLabelTrie.<LabelTrieSchema>rootPointer(), schema);
    }

    public static LabelTrieSchema child(LabelTrieSchema parent, ZNodeSchema schema) {
        return child(ZNodeLabel.of(schema.getLabel()), parent, schema);
    }

    public static LabelTrieSchema child(ZNodeLabel label, LabelTrieSchema parent, ZNodeSchema schema) {
        LabelTrie.Pointer<LabelTrieSchema> childPointer = SimpleLabelTrie.weakPointer(label, parent);
        return new LabelTrieSchema(childPointer, schema);
    }

    public static LabelTrieSchema match(LabelTrie<LabelTrieSchema> trie, AbsoluteZNodePath path) {
        Iterator<ZNodePathComponent> remaining = path.iterator();
        LabelTrieSchema node = trie.root();
        while (remaining.hasNext() && (node != null)) {
            ZNodePathComponent component = remaining.next();
            node = LabelTrieSchema.match(node, component);
        }
        return node;
    }

    public static LabelTrieSchema match(LabelTrieSchema node, ZNodeLabel label) {
        LabelTrieSchema child = node.get(label);
        if (child == null) {
            String labelString = label.toString();
            for (Map.Entry<ZNodeLabel, LabelTrieSchema> entry: node.entrySet()) {
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
            Iterator<LabelTrie.Pointer<? extends LabelTrieSchema>> itr = SimpleLabelTrie.parentIterator(node.parent());
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
    
    protected LabelTrieSchema(LabelTrie.Pointer<LabelTrieSchema> parent, ZNodeSchema schema) {
        super(SimpleLabelTrie.pathOf(parent), parent, Maps.<ZNodeLabel, LabelTrieSchema>newHashMap());
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

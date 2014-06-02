package edu.uw.zookeeper;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.Collection;
import java.util.Comparator;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ForwardingSortedSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.Serializes;

public class EnsembleView<E> extends ForwardingSortedSet<E> {

    protected static final char TOKEN_SEP = ',';
    protected static final char TOKEN_START = '[';
    protected static final char TOKEN_END = ']';

    protected static final Joiner JOINER = Joiner.on(TOKEN_SEP);
    protected static final Splitter SPLITTER = Splitter.on(TOKEN_SEP).trimResults();

    @Serializes(from=EnsembleView.class, to=String.class)
    public static String toString(Collection<?> input) {
        return toStringBuilder(new StringBuilder(), input).toString();
    }
    
    public static StringBuilder toStringBuilder(StringBuilder sb, Iterable<?> members) {
        return JOINER.appendTo(sb, Iterables.transform(members, Serializers.ToString.getInstance()));
    }
    
    public static String toDelimitedString(Iterable<?> members) {
        return toStringBuilder(new StringBuilder().append(TOKEN_START), members).append(TOKEN_END).toString();
    }
    
    public static <E> Comparator<E> comparator(Class<E> type) {
        return new Comparator<E>() {
            @Override
            public int compare(E arg0, E arg1) {
                return Serializers.ToString.getInstance().apply(arg0).compareTo(Serializers.ToString.getInstance().apply(arg1));
            }};
    }
    
    public static <E> ImmutableSortedSet<E> fromString(String input, Class<E> type) {
        ImmutableList.Builder<E> members = ImmutableList.builder();
        input = input.trim();
        if (input.length() > 0) {
            if (input.charAt(0) == TOKEN_START) {
                checkArgument(input.charAt(input.length() - 1) == TOKEN_END);
                input = input.substring(1, input.length() - 1).trim();
            }
            String[] fields = Iterables.toArray(SPLITTER.split(input), String.class);
            for (String field : fields) {
                E member = Serializers.getInstance().toClass(field, type);
                members.add(member);
            }
        }
        return ImmutableSortedSet.copyOf(comparator(type), members.build());
    }

    public static <E> EnsembleView<E> empty() {
        return create(ImmutableSortedSet.<E>of());
    }

    public static <E> EnsembleView<E> copyOf(Comparator<E> comparator, E...members) {
        return copyOf(comparator, ImmutableList.copyOf(members));
    }
    
    public static <E> EnsembleView<E> copyOf(Comparator<E> comparator, Collection<E> members) {
        return create(ImmutableSortedSet.copyOf(comparator, members));
    }

    public static <E extends Comparable<E>> EnsembleView<E> copyOf(E...members) {
        return create(ImmutableSortedSet.copyOf(members));
    }
    
    public static <E extends Comparable<E>> EnsembleView<E> copyOf(Collection<E> members) {
        return create(ImmutableSortedSet.copyOf(members));
    }

    public static <E> EnsembleView<E> create(ImmutableSortedSet<E> members) {
        return new EnsembleView<E>(members);
    }

    private final ImmutableSortedSet<E> delegate;

    protected EnsembleView(ImmutableSortedSet<E> delegate) {
        this.delegate = delegate;
    }

    @Serializes(from=EnsembleView.class, to=String.class)
    @Override
    public String toString() {
        return toString(delegate());
    }

    @Override
    protected ImmutableSortedSet<E> delegate() {
        return delegate;
    }
}

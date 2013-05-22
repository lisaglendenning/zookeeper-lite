package edu.uw.zookeeper;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.ForwardingSortedSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.Serializes;

public class EnsembleView<E extends ServerView> extends ForwardingSortedSet<E> {

    public static final char TOKEN_SEP = ',';
    public static final char TOKEN_START = '[';
    public static final char TOKEN_END = ']';

    protected static final Joiner JOINER = Joiner.on(TOKEN_SEP);
    protected static final Splitter SPLITTER = Splitter.on(TOKEN_SEP).trimResults();

    @Serializes(from=EnsembleView.class, to=String.class)
    public static String toString(EnsembleView<?> input) {
        StringBuilder output = new StringBuilder();
        output.append(TOKEN_START);
        JOINER.appendTo(output, Iterables.transform(input, Serializers.ToString.TO_STRING));
        output.append(TOKEN_END);
        return output.toString();
    }

    @Serializes(from=String.class, to=EnsembleView.class)
    public static EnsembleView<? extends ServerView.Address<?>> fromString(String input) {
        return from(fromString(input, ServerAddressView.getDefaultType()));
    }
    
    public static <E> List<E> fromString(String input, Class<E> type) {
        List<E> members = Lists.newArrayList();
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
        return members;
    }

    public static <E extends ServerView> EnsembleView<E> empty() {
        return from(ImmutableList.<E>of());
    }

    public static <E extends ServerView> EnsembleView<E> of(E...members) {
        return from(Arrays.asList(members));
    }

    public static <E extends ServerView> EnsembleView<E> from(Collection<E> members) {
        return new EnsembleView<E>(members);
    }

    protected final ConcurrentSkipListSet<E> members;

    public EnsembleView(Collection<E> members) {
        super();
        this.members = new ConcurrentSkipListSet<E>(members);
    }

    @Override
    protected ConcurrentSkipListSet<E> delegate() {
        return members;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(delegate()).toString();
    }
}

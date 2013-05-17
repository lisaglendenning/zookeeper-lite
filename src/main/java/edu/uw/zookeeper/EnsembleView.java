package edu.uw.zookeeper;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class EnsembleView implements ServerView, Iterable<ServerQuorumView> {

    public static final char TOKEN_SEP = ',';
    public static final char TOKEN_START = '[';
    public static final char TOKEN_END = ']';

    public static final Optional<ServerQuorumView> LEADER_NONE = Optional
            .<ServerQuorumView> absent();

    public static String toString(EnsembleView input) {
        StringBuilder output = new StringBuilder();
        output.append(TOKEN_START);
        Joiner joiner = Joiner.on(TOKEN_SEP);
        joiner.appendTo(output, Iterables.transform(input,
                new Function<ServerQuorumView, String>() {
                    @Override
                    public String apply(ServerQuorumView e) {
                        return ServerQuorumView.toString(e);
                    }
                }));
        output.append(TOKEN_END);
        return output.toString();
    }
    
    public static EnsembleView fromString(String input) throws ClassNotFoundException {
        List<ServerQuorumView> members = Lists.newArrayList();
        input = input.trim();
        if (input.length() > 0) {
            if (input.charAt(0) == TOKEN_START) {
                checkArgument(input.charAt(input.length() - 1) == TOKEN_END);
                input = input.substring(1, input.length() - 1).trim();
            }
            Splitter splitter = Splitter.on(TOKEN_SEP).trimResults();
            String[] fields = Iterables.toArray(splitter.split(input), String.class);
            for (String field : fields) {
                ServerQuorumView member = ServerQuorumView.fromString(field);
                members.add(member);
            }
        }
        EnsembleView view = newInstance(members);
        return view;
    }

    public static EnsembleView of(ServerQuorumView...members) {
        return new EnsembleView(Arrays.asList(members));
    }

    public static EnsembleView newInstance() {
        return new EnsembleView();
    }

    public static EnsembleView newInstance(Iterable<ServerQuorumView> members) {
        return new EnsembleView(members);
    }

    protected final List<ServerQuorumView> members;

    public EnsembleView() {
        this(Collections.<ServerQuorumView>emptyList());
    }

    public EnsembleView(Iterable<ServerQuorumView> members) {
        super();
        this.members = Lists.newCopyOnWriteArrayList(members);
    }

    protected List<ServerQuorumView> delegate() {
        return members;
    }

    @Override
    public Iterator<ServerQuorumView> iterator() {
        return delegate().iterator();
    }

    public Optional<ServerQuorumView> getLeader() {
        Optional<ServerQuorumView> leader = LEADER_NONE;
        for (ServerQuorumView e: members) {
            if (e.isLeading()) {
                leader = Optional.of(e);
                break;
            }
        }
        return leader;
    }

    public void setLeader(Optional<ServerQuorumView> leader) {
        for (ServerQuorumView e: members) {
            if (e.isLeading()) {
                if (! (leader.isPresent() && leader.get().equals(e))) {
                    e.apply(QuorumRole.LOOKING);
                }
            } else {
                if (leader.isPresent() && leader.get().equals(e)) {
                    e.apply(QuorumRole.LEADING);
                }
            }
        }
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(delegate()).toString();
    }
}

package edu.uw.zookeeper.common;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Lists;
import com.google.common.collect.Iterators;

import static com.google.common.base.Preconditions.*;

/**
 *  Simple command line argument parser.
 *  
 *  Only recognizes the format --NAME=VALUE or --NAME
 */
public class SimpleArguments implements Arguments {
        
    public static SimpleArguments defaults() {
        return create("", "", new String[0]);
    }

    public static SimpleArguments create(String programName, String description, String[] args) {
        return new SimpleArguments(ImmutableList.of(new SimpleOption(OPT_HELP)), programName, description, args);
    }

    protected static class SimpleOption implements Option {

        public static final String LONG_PREFIX = "--";
        public static final String VALUE_SEP = "=";
        public static final String TRUE_VALUE = "";

        private final String name;
        private final Optional<String> help;
        private final Optional<String> defaultValue;
        private Optional<String> value;

        public static String[] parse(String arg) {
            Splitter splitter = Splitter.on(SimpleOption.VALUE_SEP).limit(2)
                    .trimResults();
            if (arg.startsWith(SimpleOption.LONG_PREFIX)) {
                String theRest = arg.substring(SimpleOption.LONG_PREFIX
                        .length());
                checkArgument(theRest.length() > 0);
                String[] kv = Iterables.toArray(splitter.split(theRest),
                        String.class);
                return kv;
            }
            return new String[0];
        }

        public SimpleOption(String name) {
            this(name, Optional.<String> absent(), Optional.<String> absent());
        }

        public SimpleOption(String name, String help) {
            this(name, Optional.of(help), Optional.<String> absent());
        }

        public SimpleOption(String name, Optional<String> help,
                Optional<String> defaultValue) {
            this.name = checkNotNull(name);
            this.help = checkNotNull(help);
            this.defaultValue = checkNotNull(defaultValue);
            this.value = Optional.<String> absent();
        }

        @Override
        public synchronized boolean hasValue() {
            return (value.isPresent() || defaultValue.isPresent());
        }

        @Override
        public synchronized String getValue() {
            if (value.isPresent()) {
                return value.get();
            }
            return defaultValue.get();
        }

        @Override
        public synchronized void setValue(String value) {
            this.value = Optional.of(value);
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Optional<String> getHelp() {
            return help;
        }

        @Override
        public Optional<String> getDefaultValue() {
            return defaultValue;
        }

        @Override
        public String getUsage() {
            StringBuilder str = new StringBuilder(LONG_PREFIX).append(name);
            if (help.isPresent()) {
                str.append(VALUE_SEP).append(help.get());
            }
            if (defaultValue.isPresent()) {
            	str.append(' ').append(String.format("(%s)", defaultValue.get()));
            }
            return str.toString();
        }

        @Override
        public synchronized String toString() {
            String value = hasValue() ? String.format("\"%s\"", getValue()) : "";
            return String.format("%s=%s", name, value);
        }
    }

    protected static final String OPT_HELP = "help";

    private final SortedMap<String, SimpleOption> options;
    private String programName;
    private String description;
    private String[] args;

    protected SimpleArguments(Iterable<SimpleOption> options, String programName, String description, String[] args) {
        // Maybe could try to auto-detect programName with
        // System.property(sun.java.command)?
        this.options = Maps.newTreeMap();
        for (SimpleOption opt : checkNotNull(options)) {
            this.options.put(opt.getName(), opt);
        }
        this.programName = programName;
        this.description = description;
        this.args = args;
    }
    
    @Override
    public synchronized String getDescription() {
    	return description;
    }
    
    @Override
    public synchronized SimpleArguments setDescription(String description) {
    	this.description = checkNotNull(description);
    	return this;
    }

    @Override
    public synchronized Option addOption(String name, Optional<String> help,
            Optional<String> defaultValue) {
        if (options.containsKey(name)) {
            SimpleOption existing = options.get(name);
            if (!existing.getHelp().equals(help) || !existing.getDefaultValue().equals(defaultValue)) {
                throw new IllegalArgumentException(name);
            }
            return existing;
        } else {
            SimpleOption option = new SimpleOption(name, help, defaultValue);
            options.put(name, option);
            return option;
        }
    }

    @Override
    public Option addOption(String name, String help) {
        return addOption(name, Optional.of(help), Optional.<String>absent());
    }

    @Override
    public Option addOption(String name) {
        return addOption(name, Optional.<String>absent(), Optional.<String>absent());
    }

    @Override
    public synchronized boolean has(String name) {
        checkNotNull(name);
        return options.containsKey(name);
    }
    
    @Override
    public synchronized boolean hasValue(String name) {
        checkArgument(name != null);
        Option opt = options.get(name);
        checkArgument(opt != null, name);
        return opt.hasValue();
    }

    @Override
    public synchronized String getValue(String name) {
        checkArgument(name != null);
        Option opt = options.get(name);
        checkArgument(opt != null, name);
        return opt.getValue();
    }

    @Override
    public synchronized String getUsage() {
        StringBuilder str = new StringBuilder();
        str.append(String.format("Usage: %s ", programName));
        Joiner joiner = Joiner.on(' ').skipNulls();
        joiner.appendTo(str, Iterables.transform(options.values(),
                new Function<Option, String>() {
                    @Override
                    public String apply(Option opt) {
                        return String.format("[%s]", opt.getUsage());
                    }
                }));
        str.append('\n');
        if (! description.isEmpty()) {
        	str.append('\n').append(description).append('\n');
        }
        return str.toString();
    }

    @Override
    public synchronized void parse() {
        setArgs(parse(args));
    }

    @Override
    public void illegalValue(String name, String value) {
        throw new IllegalArgumentException(name + SimpleOption.VALUE_SEP
                + value);
    }

    @Override
    public boolean helpOptionSet() {
        return hasValue(OPT_HELP);
    }

    @Override
    public synchronized void setArgs(String[] args) {
        this.args = checkNotNull(args);
    }

    @Override
    public synchronized String[] getArgs() {
        return Arrays.copyOf(args, args.length);
    }

    @Override
    public synchronized String getProgramName() {
        return programName;
    }

    @Override
    public synchronized void setProgramName(String name) {
        this.programName = checkNotNull(name);
    }

    @Override
    public synchronized Iterator<Option> iterator() {
        return ImmutableList.<Option>copyOf(options.values()).iterator();
    }
    
    @Override
    public synchronized String toString() {
        return MoreObjects.toStringHelper(Arguments.class)
                .add("args", Arrays.toString(args))
                .add("options", Iterators.toString(iterator()))
                .toString();
    }

    private String[] parse(String... args) {
        List<String> unknown = Lists.newArrayList();
        for (String arg : args) {
            String[] kv = SimpleOption.parse(arg);
            if (kv.length == 0) {
                unknown.add(arg);
                continue;
            }
            assert (kv.length >= 1 && kv.length <= 2) : kv;
            String k = kv[0];
            Option opt = options.get(k);
            if (opt == null) {
                unknown.add(arg);
                continue;
            }
            assert (opt != null) : opt;
            String value = (kv.length > 1 && kv[1].length() > 0) ? kv[1]
                    : SimpleOption.TRUE_VALUE;
            opt.setValue(value);
        }
        return unknown.toArray(new String[0]);
    }
}

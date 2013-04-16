package edu.uw.zookeeper.util;

import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Lists;

import static com.google.common.base.Preconditions.*;

/**
 *  Simple command line argument parser.
 *  
 *  Only recognizes the format --NAME=VALUE or --NAME
 */
public class SimpleArguments implements Arguments {

    public static class SimpleOption implements Option {

        public static final String LONG_PREFIX = "--";
        public static final String VALUE_SEP = "=";
        public static final String TRUE_VALUE = "";

        private final String name;
        private final Optional<String> help;
        private final Optional<String> defaultValue;
        private Optional<String> value = Optional.<String> absent();

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

        public SimpleOption(String name, Optional<String> help,
                Optional<String> defaultValue) {
            this.name = checkNotNull(name);
            this.help = checkNotNull(help);
            this.defaultValue = checkNotNull(defaultValue);
        }

        public SimpleOption(String name, String help) {
            this(name, Optional.of(help), Optional.<String> absent());
        }

        public SimpleOption(String name) {
            this(name, Optional.<String> absent(), Optional.<String> absent());
        }

        @Override
        public boolean hasValue() {
            return (value.isPresent() || defaultValue.isPresent());
        }

        @Override
        public String getValue() {
            if (value.isPresent()) {
                return value.get();
            }
            return defaultValue.get();
        }

        @Override
        public void setValue(String value) {
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
            String str = LONG_PREFIX + getName();
            Optional<String> help = getHelp();
            if (help.isPresent()) {
                str += VALUE_SEP + help.get();
            }
            return str;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this).add("name", name)
                    .add("value", value).toString();
        }
    }

    public static final String OPT_HELP = "help";

    private final SortedMap<String, Option> options;
    private Class<?> mainClass;
    private String[] args;

    public SimpleArguments() {
        this(Optional.<Iterable<Option>> absent());
    }

    public SimpleArguments(Optional<Iterable<Option>> options) {
        // Maybe could try to auto-detect programName with
        // System.property(sun.java.command)?
        this.options = Maps.newTreeMap();
        checkArgument(options != null);
        if (options.isPresent()) {
            for (Option opt : options.get()) {
                add(opt);
            }
        }
        add(new SimpleOption(OPT_HELP));
    }

    @Override
    public Option newOption(String name, Optional<String> help,
            Optional<String> defaultValue) {
        return new SimpleOption(name, help, defaultValue);
    }

    @Override
    public Option newOption(String name, String help) {
        return new SimpleOption(name, help);
    }

    @Override
    public Option newOption(String name) {
        return new SimpleOption(name);
    }

    @Override
    public SimpleArguments add(Option option) {
        checkArgument(option != null);
        String name = option.getName();
        checkArgument(!(options.containsKey(name)));
        options.put(name, option);
        return this;
    }

    @Override
    public boolean hasValue(String name) {
        checkArgument(name != null);
        Option opt = options.get(name);
        checkState(opt != null);
        return opt.hasValue();
    }

    @Override
    public String getValue(String name) {
        checkArgument(name != null);
        Option opt = options.get(name);
        checkState(opt != null);
        return opt.getValue();
    }

    @Override
    public String getUsage() {
        StringBuilder str = new StringBuilder();
        String programName = getMainClass().getName();
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
        return str.toString();
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

    @Override
    public void parse() {
        String[] args = getArgs();
        if (args != null) {
            setArgs(parse(args));
        }
    }

    @Override
    public void illegalValue(String name, String value) {
        throw new IllegalArgumentException(name + SimpleOption.VALUE_SEP
                + value);
    }

    @Override
    public boolean helpOptionSet() {
        return hasValue(SimpleArguments.OPT_HELP);
    }

    @Override
    public void setArgs(String[] args) {
        this.args = checkNotNull(args, "args");
    }

    @Override
    public String[] getArgs() {
        return args;
    }

    @Override
    public Class<?> getMainClass() {
        return mainClass;
    }

    @Override
    public void setMainClass(Class<?> cls) {
        this.mainClass = checkNotNull(cls, "cls");
    }

    @Override
    public Iterator<Option> iterator() {
        return options.values().iterator();
    }
}

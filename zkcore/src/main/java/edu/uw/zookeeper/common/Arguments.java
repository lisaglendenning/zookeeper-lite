package edu.uw.zookeeper.common;

import com.google.common.base.Optional;

/**
 * Command-line argument parser.
 * 
 */
public interface Arguments extends Iterable<Arguments.Option> {

    public static interface Option {

        boolean hasValue();

        String getValue();

        void setValue(String value);

        String getName();

        Optional<String> getHelp();

        Optional<String> getDefaultValue();

        String getUsage();
    }

    Option addOption(String name, Optional<String> help,
            Optional<String> defaultValue);

    Option addOption(String name, String help);

    Option addOption(String name);

    boolean has(String name);
    
    boolean hasValue(String name);

    String getValue(String name);

    boolean helpOptionSet();

    String getUsage();

    void setArgs(String[] args);

    String[] getArgs();

    void parse();

    void illegalValue(String name, String value);

    String getProgramName();

    void setProgramName(String name);
}

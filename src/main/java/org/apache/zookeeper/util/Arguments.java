package org.apache.zookeeper.util;

import com.google.common.base.Optional;

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

    Option newOption(String name, Optional<String> help,
            Optional<String> defaultValue);

    Option newOption(String name, String help);

    Option newOption(String name);
    
    Arguments add(Option option);

    boolean hasValue(String name);

    String getValue(String name);
    
    boolean helpOptionSet();

    String getUsage();
    
    void setArgs(String[] args);
    
    String[] getArgs();
    
    void parse();

    void illegalValue(String name, String value);
    
    Class<?> getMainClass();
    
    void setMainClass(Class<?> cls);
}

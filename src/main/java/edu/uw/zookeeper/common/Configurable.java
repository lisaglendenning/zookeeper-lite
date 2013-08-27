package edu.uw.zookeeper.common;

import java.lang.annotation.*;

import com.typesafe.config.ConfigValueType;

@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface Configurable {
    String path() default "";
    String key() default "";
    String value() default "";
    String arg() default "";
    String help() default "";
    ConfigValueType type() default ConfigValueType.STRING;
}

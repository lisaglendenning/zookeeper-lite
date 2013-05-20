package edu.uw.zookeeper.data;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface Serializer {
    Class<?> input() default Void.class;
    Class<?> output() default Void.class;
}

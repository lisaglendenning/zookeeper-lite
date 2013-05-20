package edu.uw.zookeeper.data;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface Serializes {
    Class<?> from() default Void.class;
    Class<?> to() default Void.class;
}

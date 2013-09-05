package edu.uw.zookeeper.data;

import java.lang.annotation.*;

@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface Serializes {
    Class<?> from() default Void.class;
    Class<?> to() default Void.class;
}

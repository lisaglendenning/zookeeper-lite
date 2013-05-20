package edu.uw.zookeeper.data;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface ZNode {
    boolean ephemeral() default false;
    boolean sequential() default false;
    String label() default "";
}

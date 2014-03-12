package edu.uw.zookeeper.data;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface Name {
    NameType type() default NameType.STATIC;
}

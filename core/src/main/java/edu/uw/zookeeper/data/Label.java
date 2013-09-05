package edu.uw.zookeeper.data;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface Label {
    Schema.LabelType type() default Schema.LabelType.LABEL;
}

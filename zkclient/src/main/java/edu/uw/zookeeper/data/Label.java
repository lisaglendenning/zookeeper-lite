package edu.uw.zookeeper.data;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface Label {
    LabelType type() default LabelType.LABEL;
}

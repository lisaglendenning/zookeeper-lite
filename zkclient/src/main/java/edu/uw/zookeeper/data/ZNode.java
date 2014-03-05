package edu.uw.zookeeper.data;

import java.lang.annotation.*;

@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface ZNode {
    CreateMode createMode() default CreateMode.PERSISTENT;
    Acls.Definition acl() default Acls.Definition.NONE;
    String label() default "";
    LabelType labelType() default LabelType.LABEL;
    Class<?> type() default Void.class;
}

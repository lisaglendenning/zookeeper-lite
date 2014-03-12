package edu.uw.zookeeper.data;

import java.lang.annotation.*;

@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface ZNode {
    CreateMode createMode() default CreateMode.PERSISTENT;
    Acls.Definition acl() default Acls.Definition.NONE;
    String name() default "";
    NameType nameType() default NameType.STATIC;
    Class<?> dataType() default Void.class;
}

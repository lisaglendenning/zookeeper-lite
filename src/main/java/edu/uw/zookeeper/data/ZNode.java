package edu.uw.zookeeper.data;

import java.lang.annotation.*;

import org.apache.zookeeper.CreateMode;;

@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface ZNode {
    CreateMode createMode() default CreateMode.PERSISTENT;
    Acls.Definition acl() default Acls.Definition.NONE;
    String label() default "";
}

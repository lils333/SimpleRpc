package com.lee.rpc.annotation;


import com.lee.rpc.schema.Serializer;

import java.lang.annotation.*;

/**
 * @author Administrator
 */

@Target({ElementType.METHOD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Serialization {

    Class<? extends Serializer> value();
}

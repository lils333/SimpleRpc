package com.lee.rpc.spring.annotation;

import com.lee.rpc.spring.RpcConfiguration;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(RpcConfiguration.class)
public @interface EnableRpc {

}

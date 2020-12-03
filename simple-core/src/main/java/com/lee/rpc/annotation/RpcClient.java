package com.lee.rpc.annotation;

import java.lang.annotation.*;

/**
 * @author l46li
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RpcClient {

    /**
     * 客服端只需要知道服务器的地址就可以了，它可以是zookeeper，也可以是其他自定义的位置, 这个位置必须提供metadata的信息，所以
     * 如果需要自己提供，那么必须要实现metadata的提供
     * <p>
     * zookeeper://{127.0.0.1:8080}/{register-path}/service/serviceinfo
     * simple://{127.0.0.1:8080}
     *
     * @return 需要去哪个地方去获取meta-data的信息
     */
    String location();

    /**
     * 指定需要访问的服务，这个服务很重要，也就是指定的位置，不需要要包含发布的服务，如果不包含该服务，那么就直接报错误
     *
     * @return 返回需要访问的服务的名字
     */
    String service();
}

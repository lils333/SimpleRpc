package com.lee.rpc.spring;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RpcConfiguration {

    @Bean
    public RpcServerLifeCycle createRpcServerLifeCycle() {
        return new RpcServerLifeCycle();
    }
}

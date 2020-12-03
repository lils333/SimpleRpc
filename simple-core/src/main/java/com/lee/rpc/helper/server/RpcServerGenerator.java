package com.lee.rpc.helper.server;

import com.lee.rpc.RpcException;
import com.lee.rpc.RpcMethodUnit;
import com.lee.rpc.decoder.RpcServerDecoder;
import com.lee.rpc.helper.RpcHelper;
import io.netty.channel.ChannelHandler;
import io.netty.util.collection.ByteObjectMap;
import io.netty.util.collection.IntObjectMap;
import javassist.*;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.util.concurrent.Executor;

@Slf4j
public class RpcServerGenerator {

    private Class<ChannelHandler> decoderClass;

    public synchronized ChannelHandler createRpcServerDecoder() {
        try {
            if (decoderClass != null) {
                return decoderClass.getConstructor().newInstance();
            }

            ClassPool pool = ClassPool.getDefault();
            CtClass serverDecoder = pool.makeClass(RpcServerDecoder.class.getPackage().getName() + "." +
                    "RpcServerDecoderNew"
            );
            serverDecoder.setSuperclass(pool.get(RpcServerDecoder.class.getName()));

            pool.importPackage(RpcHelper.class.getName());

            StringBuilder rpcServer = new StringBuilder("switch ($1) { \n");
            IntObjectMap<RpcServiceServerUnit> rpcServerUnits = RpcHelper.getRpcServiceUnits();
            for (IntObjectMap.PrimitiveEntry<RpcServiceServerUnit> serviceUnit : rpcServerUnits.entries()) {
                int serviceId = serviceUnit.key();
                RpcServiceServerUnit unit = serviceUnit.value();

                CtField rpcServiceUnitFiled = new CtField(
                        pool.get(RpcServiceServerUnit.class.getName()), "rpcServiceUnit_" + serviceId, serverDecoder
                );
                rpcServiceUnitFiled.setModifiers(Modifier.PRIVATE);
                serverDecoder.addField(rpcServiceUnitFiled);

                CtMethod getRpcServiceUnit = new CtMethod(
                        pool.get(RpcServiceServerUnit.class.getName()), "getRpcServiceUnit_" + serviceId,
                        new CtClass[]{CtClass.intType}, serverDecoder
                );
                getRpcServiceUnit.setModifiers(Modifier.PRIVATE);
                getRpcServiceUnit.setBody(
                        "{if ($0.rpcServiceUnit_" + serviceId + " == null) { \n $0.rpcServiceUnit_" + serviceId +
                                " = RpcHelper.getRpcServiceUnit($1);\n} " +
                                "return $0.rpcServiceUnit_" + serviceId + ";}"
                );
                serverDecoder.addMethod(getRpcServiceUnit);

                rpcServer.append("case ").append(serviceId).append(": \n");

                StringBuilder sb = new StringBuilder("switch ($2) { \n");
                //$0 代表this， $1,$2,$2...代表参数
                for (ByteObjectMap.PrimitiveEntry<RpcMethodUnit> methodUnit : unit.getMethodIdMapping().entries()) {
                    //把所有的RpcMethodUnit 作为字段放到类里面去
                    String key = serviceId + "_" + methodUnit.key();
                    CtField methodIdFiled = new CtField(
                            pool.get(RpcMethodUnit.class.getName()), "rpcMethod_" + key, serverDecoder
                    );
                    methodIdFiled.setModifiers(Modifier.PRIVATE);

                    //添加字段
                    serverDecoder.addField(methodIdFiled);

                    //添加消息体
                    sb.append("case ").append(methodUnit.key()).append(" : \n")
                            .append("if ( $0.rpcMethod_").append(key).append(" == null )").append(" { \n")
                            .append("$0.rpcMethod_").append(key).append(" = ")
                            .append("$0.getRpcServiceUnit_").append(serviceId).append("($1).getMethodUnit($2);").append("\n")
                            .append(" } \n")
                            .append(" return ").append("$0.rpcMethod_").append(key).append(";\n");
                }
                sb.append("default : \n").
                        append("throw new UnsupportedOperationException(\"Can not get RpcMethodUnit with id \" + $1);\n");
                sb.append("}").append("\n");

                rpcServer.append(sb.toString());
                sb.setLength(0);
            }

            rpcServer.append("default : \n").
                    append("throw new UnsupportedOperationException(\"Can not get RpcServiceServerUnit with serverId \" + $1);\n");
            rpcServer.append("}").append("\n");

            //添加方法
            CtMethod ctMethod = new CtMethod(pool.get(RpcMethodUnit.class.getName()), "getRpcMethodUnit",
                    new CtClass[]{CtClass.intType, CtClass.byteType},
                    serverDecoder
            );

            ctMethod.setModifiers(Modifier.PROTECTED);
            ctMethod.setBody(rpcServer.toString());
            serverDecoder.addMethod(ctMethod);

            serverDecoder.writeFile("/home/lee");

            @SuppressWarnings("unchecked")
            Class<ChannelHandler> channelClass = (Class<ChannelHandler>) serverDecoder.toClass(
                    RpcServerDecoder.class.getClassLoader(),
                    RpcServerDecoder.class.getProtectionDomain()
            );
            decoderClass = channelClass;
            serverDecoder.detach();
            return decoderClass.getConstructor().newInstance();
        } catch (Exception e) {
            throw new RpcException("Can not generate RpcMethodUnit", e);
        }
    }

    public synchronized RpcMethodUnit generate(Class<?> inter,
                                               Method method,
                                               Object instance,
                                               Executor executor, byte methodId, String group, int serviceId) {
        try {
            ClassPool pool = ClassPool.getDefault();
            CtClass rpcUnit = pool.makeClass(RpcMethodUnit.class.getPackage().getName() + "." +
                    "RpcMethodUnit_" + serviceId + "_" + methodId
            );
            rpcUnit.setSuperclass(pool.get(RpcMethodUnit.class.getName()));

            CtField ctField = new CtField(pool.get(inter.getName()), "targetObject", rpcUnit);
            ctField.setModifiers(Modifier.PRIVATE);
            rpcUnit.addField(ctField);

            Class<?>[] parameterTypes = method.getParameterTypes();
            if (parameterTypes.length > 1) {
                throw new RpcException("Unsupported multiple parameters " + method + " in interface " + inter);
            }

            CtConstructor cons = new CtConstructor(new CtClass[]{pool.get(inter.getName())}, rpcUnit);
            cons.setBody("{$0.targetObject = $1;}");
            cons.setModifiers(Modifier.PUBLIC);
            rpcUnit.addConstructor(cons);

            boolean isVoidReturnValue = Void.TYPE.equals(method.getGenericReturnType());
            if (parameterTypes.length <= 0) {
                CtMethod invoker = new CtMethod(
                        pool.get(Object.class.getName()), "invoke", new CtClass[]{pool.get(Object.class.getName())},
                        rpcUnit
                );
                invoker.setModifiers(Modifier.PUBLIC);
                if (isVoidReturnValue) {
                    invoker.setBody("{$0.targetObject." + method.getName() + "();return null;}");
                } else {
                    invoker.setBody("{return $0.targetObject." + method.getName() + "();}");
                }
                rpcUnit.addMethod(invoker);
            } else {
                CtMethod converter = new CtMethod(
                        pool.get(parameterTypes[0].getName()), "convert", new CtClass[]{pool.get(Object.class.getName())},
                        rpcUnit
                );
                converter.setModifiers(Modifier.PRIVATE);
                converter.setBody("{return ($r)$1;}");
                rpcUnit.addMethod(converter);

                //invoker
                CtMethod invoker = new CtMethod(
                        pool.get(Object.class.getName()), "invoke", new CtClass[]{pool.get(Object.class.getName())},
                        rpcUnit
                );
                invoker.setModifiers(Modifier.PUBLIC);
                if (isVoidReturnValue) {
                    invoker.setBody("{$0.targetObject." + method.getName() + "($0.convert($1));return null;}");
                } else {
                    invoker.setBody("{return $0.targetObject." + method.getName() + "($0.convert($1));}");
                }
                rpcUnit.addMethod(invoker);
            }

            rpcUnit.writeFile("/home/lee");

            RpcMethodUnit unit = (RpcMethodUnit) rpcUnit.toClass().getConstructor(inter).newInstance(instance);

            rpcUnit.detach();

            return unit.withMethod(method, instance)
                    .withExecutor(executor)
                    .withMethodId(methodId)
                    .withGroup(group);
        } catch (Exception e) {
            throw new RpcException("Can not generate RpcMethodUnit", e);
        }
    }
}

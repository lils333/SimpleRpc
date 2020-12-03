package com.lee.rpc.sample;

import com.lee.rpc.helper.client.ClientHelper;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class HelloWorldClientTest {

    @Test
    public void testClient() {
        try {
            HelloWorldClient client = ClientHelper.getClient(HelloWorldClient.class);
            StopWatch stopwatch = StopWatch.createStarted();

            int processors = Runtime.getRuntime().availableProcessors();
            final CountDownLatch latch = new CountDownLatch(50);

            System.out.println("processors : " + processors);

//            client.sayHello(createUser(1));

            for (int j = 0; j < 50; j++) {
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        for (int i = 0; i < 100000; i++) {
                            client.sayHello(createUser(i));
                        }
                        latch.countDown();
                    }
                }).start();
            }

            latch.await();

            HelloWorldClient2 client1 = ClientHelper.getClient(HelloWorldClient2.class);
            for (int i = 0; i < 5; i++) {
                System.out.println(client1.saySay(i));
            }
            stopwatch.stop();
            System.out.println(stopwatch.getTime(TimeUnit.MILLISECONDS));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ClientHelper.shutdown();
        }
    }

    private User createUser(int i) {
        User user = new User();
        user.setAge(14);
        user.setTitle("DEV");
        user.setHireDate(new Date());
        user.setSalary(3.5D);
        user.setId(i);
        user.setAddress(Arrays.asList("D", "B"));
        user.setName("LEE" + i);
        return user;
    }
}
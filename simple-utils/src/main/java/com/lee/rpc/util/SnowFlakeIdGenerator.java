package com.lee.rpc.util;

/**
 * 0 - 0000000000 0000000000 0000000000 0000000000 0 - 00000 - 00000 - 000000000000
 * 1位标识部分，在java中由于long的最高位是符号位，正数是0，负数是1，一般生成的ID为正数，所以为0；
 * <p>
 * 41位时间戳部分，这个是毫秒级的时间，一般实现上不会存储当前的时间戳，而是时间戳的差值（当前时间-固定的开始时间），
 * 这样可以使产生的ID从更小值开始；41位的时间戳可以使用69年，(1L << 41) / (1000L * 60 * 60 * 24 * 365) = 69年；
 * <p>
 * 10位节点部分，Twitter实现中使用前5位作为数据中心标识，后5位作为机器标识，可以部署1024个节点；
 * <p>
 * 12位序列号部分，支持同一毫秒内同一个节点可以生成4096个ID；
 * <p>
 * <p>
 * :& Long.MAX_VALUE 只是用于去除掉最高位的符号，也就是保证最高位为0
 * 获取工作ID : (key << 41 & Long.MAX_VALUE) >> 51
 * 获取sequence ：(key << 53 & Long.MAX_VALUE) >> 53
 * 获取时间 ：(key >> 22) + START_TIME
 * <p>
 * ~(n - 1) & k --这个意思是按照n对其，也就是当 n是4096，那么这个就是按照4096对其，k值小于4096，那么就是0，大于4096 小于4096 *2 ，那么就取4096
 */

public class SnowFlakeIdGenerator {

    //一般是项目的开始时间
    public static final long START_TIME = 1597024838074L;

    //该方式可以获取当前位数能够表示的10进制的最大值
    private static final long WORK_CENTER_MAX = ~(-1L << 12);
    private static final long SEQUENCE_MAX = ~(-1L << 10);

    private final long workCenter;
    private final long startTime;

    private long dateTime;
    private long sequence;

    public SnowFlakeIdGenerator() {
        this(1, START_TIME);
    }

    public SnowFlakeIdGenerator(long workCenter) {
        this(workCenter, START_TIME);
    }

    public SnowFlakeIdGenerator(long workCenter, long startTime) {
        if (workCenter > WORK_CENTER_MAX || workCenter < 0) {
            throw new IllegalArgumentException("dateCenter valid value is 1 - " + WORK_CENTER_MAX);
        }
        this.startTime = startTime;
        this.workCenter = workCenter << 10;
    }

    /**
     * 不需要加 锁，用了threadlocal，每一个线程都有自己的SnowFlakeIdGenerator，所以默认就线程安全了
     *
     * @return 一个long类型的ID占用8个字节
     */
    public long generatorKey() {
        long currentTime = System.currentTimeMillis();
        if (currentTime < dateTime) {
            throw new IllegalStateException("Clock moved backwards, Refusing to generate id");
        }

        //同一毫秒内的序列生成
        if (currentTime == dateTime) {
            //这个是用 & 获取sequenceMax的最大值, 如果超过了就是0
            sequence = (sequence + 1) & SEQUENCE_MAX;
            if (sequence == 0) {
                //如果序列号已经用完，那么等待使用下一个毫秒的序列号
                while (currentTime <= dateTime) {
                    currentTime = System.currentTimeMillis();
                }
            }
        } else {
            sequence = 0L;
        }
        dateTime = currentTime;
        return (dateTime - startTime) << 22 | workCenter | sequence;
    }
}

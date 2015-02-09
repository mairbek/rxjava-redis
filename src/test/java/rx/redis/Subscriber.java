package rx.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action1;
import rx.functions.Func1;

public class Subscriber {

    public static void main(String[] args) throws InterruptedException {

        final JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(2);
        poolConfig.setBlockWhenExhausted(false);
        final JedisPool jedisPool = new JedisPool(poolConfig, "localhost", 6380);

        final Jedis j = jedisPool.getResource();
        j.connect();

        System.out.println("Subscribing...");

        final Observable<Integer> channel = RedisPubSub.observe(j, "channel").map(
                new Func1<String, Integer>() {
                    @Override
                    public Integer call(String s) {

                        return s.length();
                    }
                });
        jedisPool.returnResource(j);

        final Subscription subscription = channel.subscribe(new Action1<Integer>() {
            @Override
            public void call(Integer len) {

                System.out.println("first-" + len);
            }
        });

        final Jedis j2 = jedisPool.getResource();
        j2.connect();

        final Observable<Integer> secondChannel = RedisPubSub.observe(j2, "channel").map(
                new StringLength());

        jedisPool.returnResource(j2);

        final Subscription secondSubscription = secondChannel.subscribe(new Action1<Integer>() {
            @Override
            public void call(Integer len) {

                System.out.println("second-" + len);
            }
        });

        final Jedis j3 = jedisPool.getResource();
        j3.connect();
        final Observable<Integer> third = RedisPubSub.observe(j3, "channel").map(
                new StringLength());

        jedisPool.returnResource(j3);

        final Subscription thirdSubscription = third.subscribe(new Action1<Integer>() {
            @Override
            public void call(Integer len) {

                System.out.println("second-" + len);
            }
        });


        System.out.println("sleeping");
        Thread.sleep(10000L);
        System.out.println("woke up");

        subscription.unsubscribe();
        secondSubscription.unsubscribe();
        thirdSubscription.unsubscribe();

        System.out.println("unsubscribed");

        System.exit(0);

    }

    private static class StringLength implements Func1<String, Integer> {

        @Override
        public Integer call(final String s) {

            return s.length();
        }

    }
}

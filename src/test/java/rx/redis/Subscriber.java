package rx.redis;

import redis.clients.jedis.Jedis;
import rx.Subscription;
import rx.functions.Action1;
import rx.functions.Func1;

public class Subscriber {

    public static void main(String[] args) throws InterruptedException {

        Jedis j = new Jedis("localhost", 6380);
        j.connect();

        System.out.println("Subscribing...");
        final Subscription subscription = RedisPubSub.observe(j, "channel").map(
                new Func1<String, Integer>() {
                    @Override
                    public Integer call(String s) {

                        return s.length();
                    }
                }).subscribe(new Action1<Integer>() {
            @Override
            public void call(Integer len) {

                System.out.println(len);
            }
        });

        System.out.println("sleeping");
        Thread.sleep(10000L);
        System.out.println("woke up");

        subscription.unsubscribe();

    }

}

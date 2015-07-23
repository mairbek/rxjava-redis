package rx.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;
import rx.Observable;
import rx.Subscription;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Matteo Moci ( matteo (dot) moci (at) gmail (dot) com )
 */
public class RedisPubSubTestCase {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisPubSubTestCase.class);

    private final AtomicBoolean shouldRun;

    public RedisPubSubTestCase() {

        shouldRun = new AtomicBoolean(true);
    }

    @Test
    public void testName() throws Exception {

        final Jedis publisherJedis = new Jedis("localhost", 6380);

        final Thread publisher = new Thread(new Runnable() {

            @Override
            public void run() {

                while (shouldRun.get()) {

                    publisherJedis.publish("a-channel", "msg at:'" + System.nanoTime() + "'");

                    try {
                        Thread.sleep(100L);
                    } catch (InterruptedException e) {
                        LOGGER.warn("", e);
                    }

                }

            }
        });

        publisher.start();

        final Jedis subscriberJedis = new Jedis("localhost", 6380);

        final Observable<String> redisObservable = RedisPubSub.observe(subscriberJedis,
                "a-channel");

        final Subscription subscribe = redisObservable.subscribe();

        Thread.sleep(1000L);

        shouldRun.set(false);

        subscribe.unsubscribe();

        LOGGER.info("end");

    }
}

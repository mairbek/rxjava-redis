package rx.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * @author Matteo Moci ( matteo (dot) moci (at) gmail (dot) com )
 */
public class RedisPoolPubSubTestCase {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisPubSubTestCase.class);

    private final AtomicBoolean shouldRun;

    public RedisPoolPubSubTestCase() {

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
                        Thread.sleep(1L);
                    } catch (InterruptedException e) {
                        LOGGER.warn("", e);
                    }

                }

            }
        });

        publisher.start();

        final GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxIdle(1);
        poolConfig.setMaxTotal(1);
        poolConfig.setBlockWhenExhausted(false);

        final JedisPool subscriberJedisPool = new JedisPool(poolConfig, "localhost", 6380);

        final Observable<String> redisObservable = RedisPoolPubSub.observe(subscriberJedisPool,
                "a-channel");

        final AtomicBoolean atLeastAMessageWasConsumed = new AtomicBoolean(false);

        final Subscription subscribe = redisObservable.subscribe(new Action1<String>() {
            @Override
            public void call(final String s) {

                if (!atLeastAMessageWasConsumed.get()) {
                    atLeastAMessageWasConsumed.set(true);
                }
            }
        });

        Thread.sleep(10L);

        final AtomicBoolean secondObservableCalledOnError = new AtomicBoolean(false);

        final Observable<String> secondRedisObservable = RedisPoolPubSub.observe(
                subscriberJedisPool, "a-channel");

        final Subscription failingSubscribe = secondRedisObservable.subscribe(
                new Action1<String>() {
                    @Override
                    public void call(final String s) {
                        //should never happen
                        fail("should never happen");
                    }
                }, new Action1<Throwable>() {
                    @Override
                    public void call(final Throwable throwable) {

                        //should be called:
                        secondObservableCalledOnError.set(true);

                    }
                }, new Action0() {
                    @Override
                    public void call() {
                        //should never happen
                        fail("should never happen");
                    }
                });

        shouldRun.set(false);

        subscribe.unsubscribe();

        assertTrue(secondObservableCalledOnError.get());

        assertTrue(atLeastAMessageWasConsumed.get());

        assertEquals(subscriberJedisPool.getNumActive(), 1);
    }
}

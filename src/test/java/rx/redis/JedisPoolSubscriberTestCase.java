package rx.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action1;

/**
 * @author Matteo Moci ( matteo (dot) moci (at) gmail (dot) com )
 */
public class JedisPoolSubscriberTestCase {

    private static final Logger LOGGER = LoggerFactory.getLogger(JedisPoolSubscriberTestCase.class);

    @Test
    public void testName() throws Exception {

        final GenericObjectPoolConfig jedisPoolConfig = new GenericObjectPoolConfig();
        jedisPoolConfig.setMaxTotal(2);
        jedisPoolConfig.setMinIdle(1);
        jedisPoolConfig.setBlockWhenExhausted(false);

        final JedisPool jedisPool = new JedisPool(jedisPoolConfig, "localhost", 6380);

        final Jedis publisher = new Jedis("localhost", 6380);

        final Thread one = new Thread(new JedisSubscriber(jedisPool));
        one.start();

        final Thread two = new Thread(new JedisSubscriber(jedisPool));
        two.start();

        Thread.sleep(1000L);

        publisher.publish("channel", "a message");

        Thread.sleep(1000L);

        publisher.publish("channel", "another message");

        Thread.sleep(5000L);

    }

    private static class JedisSubscriber implements Runnable {

        private final JedisPool jedisPool;

        public JedisSubscriber(final JedisPool jedisPool) {

            this.jedisPool = jedisPool;
        }

        @Override
        public void run() {

            LOGGER.info("running");

            Jedis jedis = null;

            try {

                jedis = jedisPool.getResource();

                final Observable<String> valuesFromChannel = RedisPubSub.observe(jedis, "channel");

                final Subscription subscribe = valuesFromChannel.subscribe(new Action1<String>() {
                    @Override
                    public void call(String s) {

                        LOGGER.info("received: " + s);

                    }
                });

            } catch (final Exception e) {
                LOGGER.warn("can't get jedis", e);
            } finally {
                if (null != jedis) {
                    jedis.close();
                }
            }

        }
    }
}
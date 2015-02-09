package rx.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

/**
 * @author Matteo Moci ( matteo (dot) moci (at) gmail (dot) com )
 */
public class JedisPoolTestCase {

    private static final Logger LOGGER = LoggerFactory.getLogger(JedisPoolTestCase.class);

    @Test
    public void testName() throws Exception {

        final GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxIdle(1);
        poolConfig.setMaxTotal(1);
        poolConfig.setMinIdle(1);

        final JedisPool jedisPool = new JedisPool(poolConfig, "localhost", 6380);

        final Jedis jedis = jedisPool.getResource();
        LOGGER.info("got resource");

        final LocalPubSub jedisPubSub = new LocalPubSub();

        new Thread(new Runnable() {
            @Override
            public void run() {

                LOGGER.info("subscribing");
                jedis.subscribe(jedisPubSub, "a-channel");
                LOGGER.info("subscribed");
            }
        }).start();

        Thread.sleep(1000L);

        new Thread(new Runnable() {
            @Override
            public void run() {

                jedisPubSub.unsubscribe();

            }
        }).start();

        LOGGER.info("threads started");

        Thread.sleep(10000L);

        LOGGER.info("" + jedisPool.getNumActive());

        LOGGER.info("end");

    }

    private class LocalPubSub extends JedisPubSub {

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {

            LOGGER.info(
                    "channel: '" + channel + "' subscribed channels '" + subscribedChannels + "'");

        }

    }
}

package rx.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

/**
 * @author Matteo Moci ( matteo (dot) moci (at) gmail (dot) com )
 */
public class JedisPoolTestCase {

    private static final Logger LOGGER = LoggerFactory.getLogger(JedisPoolTestCase.class);


    /* TODO test this script:
    *
    * acquire jedis from pool
    * on exception, throw
    * register a pubsub on a channel using thread x
    * on websocketclose, unsubscribe pubsub
    * the method subscribe terminates, and we can release the jedis.
    *
    * */

    @Test (enabled = true)
    public void testName() throws Exception {

        final GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxIdle(1);
        poolConfig.setMaxTotal(1);
        poolConfig.setMinIdle(1);
        poolConfig.setBlockWhenExhausted(false);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestWhileIdle(true);

        final JedisPool jedisPool = new JedisPool(poolConfig, "localhost", 6380);

        LOGGER.info("pool before getResource " + jedisPool.getNumActive());

        final Jedis jedis = jedisPool.getResource();
        LOGGER.info("got resource");
        LOGGER.info("pool after getResource " + jedisPool.getNumActive());

        final LocalPubSub jedisPubSub = new LocalPubSub();

        new Thread(new Runnable() {
            @Override
            public void run() {

                LOGGER.info("subscribing");
                jedis.subscribe(jedisPubSub, "a-channel");
                LOGGER.info("subscribe finished, always after unsubscribing");


                //from here
                LOGGER.info("closing, releasing jedis");
                jedis.close();
                LOGGER.info("released jedis");
                //to here

            }
        }).start();

        LOGGER.info("first thread started");

        Thread.sleep(100L);

        new Thread(new Runnable() {
            @Override
            public void run() {

                LOGGER.info("unsubscribing");
                jedisPubSub.unsubscribe();
                LOGGER.info("unsubscribed");


            }
        }).start();

        LOGGER.info("second thread started");

        Thread.sleep(5000L);

        LOGGER.info("pool at end " + jedisPool.getNumActive());

        Jedis resource = null;
        try {
            resource = jedisPool.getResource();
        } catch (JedisConnectionException e) {
            assertNull(e);
        }
        assertNotNull(resource);

    }

    private static class LocalPubSub extends JedisPubSub {

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {

            LOGGER.info(
                    "channel: '" + channel + "' subscribed channels '" + subscribedChannels + "'");

        }

    }
}

package rx.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Func0;
import rx.subscriptions.Subscriptions;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Matteo Moci ( matteo (dot) moci (at) gmail (dot) com )
 */
public class RedisPoolPubSub {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisPoolPubSub.class);

    public static Observable<String> observe(final JedisPool jedisPool, final String channel) {

        return observe(jedisPool, channel, Executors.newSingleThreadExecutor());
    }

    public static Observable<String> observe(final JedisPool jedisPool, final String channel,
            final ExecutorService executor) {

        return Observable.defer(new Func0<Observable<String>>() {
            @Override
            public Observable<String> call() {

                return Observable.create(new RedisObservable(jedisPool, channel, executor));
            }
        });
    }

    private static class RedisObservable implements Observable.OnSubscribe<String> {

        private final JedisPool jedisPool;

        private final String channel;

        private final Executor executor;

        public RedisObservable(final JedisPool jedisPool, final String channel,
                final Executor executor) {

            this.jedisPool = jedisPool;

            this.channel = channel;

            this.executor = executor;

        }

        @Override
        public void call(final Subscriber<? super String> subscriber) {

            final JedisPubSub jedisPubSub = new OnMessageOnNext(subscriber);

            //the resource is returned to the pool by UnsubscribeAction
            Jedis jedis = null;
            try {
                jedis = jedisPool.getResource();

                final JedisSubscribe jedisSubscribe = new JedisSubscribe(jedis, jedisPubSub,
                        channel);
                executor.execute(jedisSubscribe);

                JedisUnsubscribe jedisUnsubscribe = new JedisUnsubscribe(jedis, jedisPubSub);
                final ExecuteCommand executeCommandAction = new ExecuteCommand(executor,
                        jedisUnsubscribe);
                // from http://stackoverflow.com/questions/26695125/how-to-get-notified-of-a-observers-unsubscribe-action-in-a-custom-observable-in
                subscriber.add(Subscriptions.create(executeCommandAction));

            } catch (Exception e) {
                subscriber.onError(e);
            }
        }

        private static class JedisSubscribe implements Runnable {

            private final Jedis jedis;

            private final JedisPubSub jedisPubSub;

            private final String channel;

            public JedisSubscribe(final Jedis jedis, final JedisPubSub jedisPubSub,
                    final String channel) {

                this.jedis = jedis;
                this.jedisPubSub = jedisPubSub;
                this.channel = channel;
            }

            @Override
            public void run() {

                this.jedis.subscribe(this.jedisPubSub, this.channel);
            }
        }

        private static class JedisUnsubscribe implements Runnable {

            private final Jedis jedis;
            private final JedisPubSub jedisPubSub;

            public JedisUnsubscribe(final Jedis jedis, final JedisPubSub jedisPubSub) {

                this.jedis = jedis;
                this.jedisPubSub = jedisPubSub;
            }

            @Override
            public void run() {

                this.jedisPubSub.unsubscribe();
                this.jedis.close();
            }
        }

        private static class ExecuteCommand implements Action0 {

            private final Executor executor;
            private Runnable command;

            public ExecuteCommand(final Executor executor, final Runnable command) {

                this.executor = executor;
                this.command = command;
            }

            @Override
            public void call() {

                this.executor.execute(command);

            }
        }

        private static class OnMessageOnNext extends JedisPubSub {

            private Subscriber<? super String> subscriber;

            public OnMessageOnNext(final Subscriber<? super String> subscriber) {

                this.subscriber = subscriber;
            }

            @Override
            public void onMessage(final String channel, final String message) {

                if (!subscriber.isUnsubscribed()) {
                    subscriber.onNext(message);
                }

            }

            @Override
            public void onPMessage(String pattern, String channel, String message) {

            }

            public void onSubscribe(String channel, int subscribedChannels) {
                //TODO?
                //                subscriber.onStart();

            }

            @Override
            public void onUnsubscribe(String channel, int subscribedChannels) {

                //TODO?
                //                subscriber.onCompleted();

            }

            @Override
            public void onPUnsubscribe(String pattern, int subscribedChannels) {

            }

            @Override
            public void onPSubscribe(String pattern, int subscribedChannels) {

            }

        }
    }

}


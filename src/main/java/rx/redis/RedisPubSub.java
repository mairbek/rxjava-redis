package rx.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Func0;
import rx.subscriptions.Subscriptions;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class RedisPubSub {

    public static Observable<String> observe(final Jedis jedis, final String channel) {

        return observe(jedis, channel, Executors.newSingleThreadExecutor());
    }

    public static Observable<String> observe(final Jedis jedis, final String channel,
            final ExecutorService executor) {

        return Observable.defer(new Func0<Observable<String>>() {
            @Override
            public Observable<String> call() {

                return Observable.create(new RedisObservable(jedis, channel, executor));
            }
        });
    }

    private static class RedisObservable implements Observable.OnSubscribe<String> {

        private final Jedis jedis;

        private final String channel;

        private final Executor executor;

        public RedisObservable(final Jedis jedis, final String channel, final Executor executor) {

            this.jedis = jedis;
            this.channel = channel;
            this.executor = executor;
        }

        @Override
        public void call(final Subscriber<? super String> subscriber) {

            final JedisPubSub pubSub = new JedisPubSub() {
                @Override
                public void onMessage(String channel, String message) {

                    subscriber.onNext(message);
                }

                @Override
                public void onPMessage(String pattern, String channel, String message) {

                }

                public void onSubscribe(String channel, int subscribedChannels) {

                }

                @Override
                public void onUnsubscribe(String channel, int subscribedChannels) {

                }

                @Override
                public void onPUnsubscribe(String pattern, int subscribedChannels) {

                }

                @Override
                public void onPSubscribe(String pattern, int subscribedChannels) {

                }
            };

            executor.execute(new Runnable() {
                @Override
                public void run() {

                    jedis.subscribe(pubSub, channel);
                }
            });

            // from http://stackoverflow.com/questions/26695125/how-to-get-notified-of-a-observers-unsubscribe-action-in-a-custom-observable-in
            subscriber.add(Subscriptions.create(new Action0() {
                @Override
                public void call() {

                    pubSub.unsubscribe(channel);
                }
            }));

        }
    }

}

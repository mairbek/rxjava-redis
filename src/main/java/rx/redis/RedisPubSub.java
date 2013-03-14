package rx.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import rx.Observable;
import rx.Observer;
import rx.Subscription;
import rx.util.functions.Func0;
import rx.util.functions.Func1;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class RedisPubSub {

    public static Observable<String> observe(final Jedis jedis, final String channel) {
        return observe(jedis, channel, Executors.newSingleThreadExecutor());
    }

    private static Observable<String> observe(final Jedis jedis, final String channel, final ExecutorService executor) {
        return Observable.defer(new Func0<Observable<String>>() {
            @Override
            public Observable<String> call() {
                return Observable.create(new RedisObservable(jedis, channel, executor));
            }
        });
    }

    private static class RedisObservable implements Func1<Observer<String>, Subscription> {

        private final Jedis jedis;
        private final String channel;
        private final Executor executor;

        public RedisObservable(Jedis jedis, String channel, Executor executor) {
            this.jedis = jedis;
            this.channel = channel;
            this.executor = executor;
        }

        @Override
        public Subscription call(final Observer<String> observer) {
            final JedisPubSub pubSub = new JedisPubSub() {
                @Override
                public void onMessage(String channel, String message) {
                    observer.onNext(message);
                }

                @Override
                public void onPMessage(String pattern, String channel,
                                       String message) {

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

            return new Subscription() {
                @Override
                public void unsubscribe() {
                    pubSub.unsubscribe();
                }
            };
        }
    }

}

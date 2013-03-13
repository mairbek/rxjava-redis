RxJava + Redis PubSub

Quick example how to use RxJava with Jedis.

```java
    RedisPubSub.observe(jedis, "channel")
            .map(new Func1<String, Integer>() {
                @Override
                public Integer call(String s) {
                    return s.length();
                }
            })
            .subscribe(new Action1<Integer>() {
                @Override
                public void call(Integer len) {
                    System.out.println(len);
                }
            });
```

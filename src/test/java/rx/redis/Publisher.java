package rx.redis;

import redis.clients.jedis.Jedis;

import java.util.Arrays;

public class Publisher {

    public static void main(String[] args) throws Exception {
        Jedis j = new Jedis("localhost");
        j.connect();

        System.out.println("Publishing messages");
        for (String msg : Arrays.asList("one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten")) {
            Thread.sleep(1000);
            j.publish("channel", msg);
        }

        j.disconnect();

    }
}

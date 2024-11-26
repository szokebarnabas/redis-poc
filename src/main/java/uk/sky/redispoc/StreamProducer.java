package uk.sky.redispoc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

@Service
public class StreamProducer {

    private static final String STREAM_KEY = "bookmarks-events-stream";
    private Logger logger = LoggerFactory.getLogger(StreamProducer.class);
    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;

    public StreamProducer(ReactiveRedisTemplate<String, String> reactiveRedisTemplate) {
        this.reactiveRedisTemplate = reactiveRedisTemplate;
    }

    /**
     * Publish a message to the Redis Stream.
     *
     * @param key   The key for the message.
     * @param value The value of the message.
     */
    public void publishMessage(String key, String value) {
        long expiresAt = Instant.now().plus(1, ChronoUnit.SECONDS).toEpochMilli();

        Map<String, String> message = new HashMap<>();
        message.put("key", key);
        message.put("value", value);
        message.put("expires_at", String.valueOf(expiresAt));
        message.put("created_at", String.valueOf(System.currentTimeMillis()));

         reactiveRedisTemplate.opsForStream()
                 .add(STREAM_KEY, message)
                 .doOnSuccess(recordId -> {
                     logger.info("Published message with ID: {}", recordId);
                 })
                 .subscribe();

    }
}

package uk.sky.redispoc;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.Record;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;

import io.lettuce.core.RedisBusyException;
import reactor.core.publisher.Mono;

import java.util.Map;

@Service
public class BookmarkService {

    private static final String CONSUMER_GROUP = "your-consumer-group";
    private static final String CONSUMER_NAME = "your-consumer-name";
    private static final Logger log = LoggerFactory.getLogger(BookmarkService.class);
    private static final String BOOKMARKS_EVENTS_STREAM = "bookmarks-events-stream";
    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;

    public BookmarkService(ReactiveRedisTemplate<String, String> reactiveRedisTemplate) {
        this.reactiveRedisTemplate = reactiveRedisTemplate;
    }

    @PostConstruct
    public void init() {
        reactiveRedisTemplate.opsForStream()
                .createGroup(BOOKMARKS_EVENTS_STREAM, ReadOffset.latest(), CONSUMER_GROUP)
                .onErrorResume(e -> {
                    // Handle the case where the group already exists
                    if (e instanceof RedisSystemException && e.getCause() instanceof RedisBusyException) {
                        return Mono.empty();
                    }
                    return Mono.error(e);
                })
                .subscribe();
    }

    public Mono<Map<Object, Object>> getMostRecentBookmarkById(String bookmarkId) {
        return reactiveRedisTemplate.opsForStream()
                .reverseRange(BOOKMARKS_EVENTS_STREAM, Range.unbounded())
                .filter(entries -> entries.getValue()
                        .get("key")
                        .equals(bookmarkId))
                .map(Record::getValue)
                .doOnNext(bookmark -> log.info("Found bookmark: {}", bookmark))
                .next();
    }

    public void saveToCassandra() {
        reactiveRedisTemplate.opsForStream()
                .read(Consumer.from(CONSUMER_GROUP, CONSUMER_NAME),
                        StreamOffset.create(BOOKMARKS_EVENTS_STREAM, ReadOffset.lastConsumed()))
                .doOnNext(record -> {
                    log.info("Record: {}", record);
                    // INSERT TO CASSANDRA USING TIMESTAMP...
                })
                .flatMap(record -> reactiveRedisTemplate.opsForStream()
                        .acknowledge(CONSUMER_GROUP, record))
                .doOnError(e -> log.error("Error processing stream: ", e))
                .subscribe(
                    null,
                    e -> log.error("Subscription error: ", e),
                    () -> log.info("Stream processing completed")
                );
    }
}

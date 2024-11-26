package uk.sky.redispoc;

import java.util.Map;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/bookmarks")
public class StreamController {

    private final StreamProducer streamProducer;
    private final BookmarkService bookmarkService;

    public StreamController(StreamProducer streamProducer, BookmarkService bookmarkService) {
        this.streamProducer = streamProducer;
        this.bookmarkService = bookmarkService;
    }

    @PostMapping
    public String publishMessage(@RequestBody Map<String, String> payload) {
        String key = payload.get("key");
        String value = payload.get("value");
        streamProducer.publishMessage(key, value);
        return "Message published successfully. Id: " + key;
    }

    @GetMapping("/{id}")
    public Mono<Map<Object, Object>> getById(@PathVariable String id) {
        return bookmarkService.getMostRecentBookmarkById(id);
    }

    @GetMapping("/sync")
    public void getAll() {
        bookmarkService.saveToCassandra();
    }

}
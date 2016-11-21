import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Cache {
    private List<Document> cache;
    private int maxSize;

    public Cache(int maxSize) {
        cache = new ArrayList<Document>();
        this.maxSize = maxSize;
    }

    public void add(Document doc) {
        if (cache.size() == maxSize) {
            Random rand = new Random();
            if(rand.nextBoolean()) return;

            cache.remove(rand.nextInt(cache.size()));
            cache.add(doc);
        }
        cache.add(doc);
    }

    public void addAll(List<Document> list){
        if (list.size() + cache.size() > maxSize) {
            Random rand = new Random();
            for(int i = 0; i < maxSize - list.size() + cache.size(); i++) {
                this.add(list.get(rand.nextInt(list.size())));
            }
        }
        cache.addAll(list);
    }

    public Document getRandom() {
        return cache.get(new Random().nextInt(cache.size()));
    }
}

import org.elasticsearch.client.RestHighLevelClient;

import java.util.Collection;

public interface ItemService {

    public Item addItem(Item item, String index, String type, RestHighLevelClient restHighLevelClient) throws ItemException;

    public Item getItem(String id, String index, String type, RestHighLevelClient restHighLevelClient);

    public Item editItem(String id, Item item, String index, String type, RestHighLevelClient restHighLevelClient);

    public void deleteItem(String id, String index, String type, RestHighLevelClient restHighLevelClient);
}

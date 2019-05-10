
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ItemServiceImpl implements ItemService {

    private static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Item addItem(Item item, String index, String type, RestHighLevelClient restHighLevelClient) throws ItemException {
        Map<String, Object> dataMap = new HashMap<String, Object>();
        if(item==null){
            throw new ItemException("El item provisto es nulo");
        }
        if(item.getId().equals("")){
            throw new ItemException("El id del item esta vacio");
        }
        if(item.getTitle().equals("")){
            throw new ItemException("El title del item esta vacio");
        }
        dataMap.put("id", item.getId());
        dataMap.put("title", item.getTitle());
        IndexRequest indexRequest = new IndexRequest(index, type, item.getId())
                .source(dataMap);
        try {
            IndexResponse response = restHighLevelClient.index(indexRequest);
        } catch (ElasticsearchException e) {
            e.getDetailedMessage();
        } catch (IOException ex) {
            ex.getLocalizedMessage();
        }
        return item;
    }

    @Override
    public Item getItem(String id, String index, String type, RestHighLevelClient restHighLevelClient) {
        GetRequest getItemRequest = new GetRequest(index, type, id);
        GetResponse getResponse = null;
        try {
            getResponse = restHighLevelClient.get(getItemRequest);
        } catch (IOException e) {
            e.getLocalizedMessage();
        }
        return getResponse != null ?
                objectMapper.convertValue(getResponse.getSourceAsMap(), Item.class) : null;
    }

    @Override
    public Item editItem(String id,Item item, String index, String type, RestHighLevelClient restHighLevelClient){

        UpdateRequest updateRequest = new UpdateRequest(index, type, id)
                .fetchSource(true);    // Fetch Object after its update
        try {
            String itemJson = objectMapper.writeValueAsString(item);
            updateRequest.doc(itemJson, XContentType.JSON);
            UpdateResponse updateResponse = restHighLevelClient.update(updateRequest);
            return objectMapper.convertValue(updateResponse.getGetResult().sourceAsMap(), Item.class);
        } catch (JsonProcessingException e) {
            e.getMessage();
        } catch (IOException e) {
            e.getLocalizedMessage();
        }
        System.out.println("Unable to update item");
        return null;
    }

    @Override
    public void deleteItem(String id, String index, String type, RestHighLevelClient restHighLevelClient) {
        DeleteRequest deleteRequest = new DeleteRequest(index, type, id);
        try {
            DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest);
        } catch (IOException e) {
            e.getLocalizedMessage();
        }
    }
}

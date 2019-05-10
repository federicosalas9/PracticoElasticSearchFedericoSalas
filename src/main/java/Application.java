import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.google.gson.Gson;

import static spark.Spark.*;

public class Application {

    //The config parameters for the connection
    private static final String HOST = "localhost";
    private static final int PORT_ONE = 9200;
    private static final int PORT_TWO = 9201;
    private static final String SCHEME = "http";

    private static RestHighLevelClient restHighLevelClient;
    private static ObjectMapper objectMapper = new ObjectMapper();

    private static final String INDEX = "itemdata";
    private static final String TYPE = "item";

    /**
     * Implemented Singleton pattern here
     * so that there is just one connection at a time.
     *
     * @return RestHighLevelClient
     */
   private static synchronized RestHighLevelClient makeConnection() {

        if (restHighLevelClient == null) {
            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost(HOST, PORT_ONE, SCHEME),
                            new HttpHost(HOST, PORT_TWO, SCHEME)));
        }

        return restHighLevelClient;
    }

    private static synchronized void closeConnection() throws IOException {
        restHighLevelClient.close();
        restHighLevelClient = null;
    }

    private static final ItemService itemService = new ItemServiceImpl();
    public static void main(String[] args) throws IOException {

        makeConnection();
        //---------------------------------------------------------------
        port(8000);
        //Crear un item
        post("/items", (request, response) -> {
            try{
                response.type("application/json");
                Item item = new Gson().fromJson(request.body(), Item.class);
                item=itemService.addItem(item,INDEX,TYPE,restHighLevelClient);
                System.out.println("Item insertado --> " + item);
                response.status(201);
                return new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS, "El item fue creado"));
            } catch (Exception exception){
                response.status(400);
                return new Gson().toJson(new StandardResponse(StatusResponse.ERROR, exception.getMessage()));
            }

        });

        //Mostrar un item segun el id
        get("/items/:id", (request, response) -> {
            response.type("application/json");
            Item itemFromDB = itemService.getItem(request.params(":id"),INDEX,TYPE,restHighLevelClient);
            System.out.println("Item mostrado --> " + itemFromDB);
            return new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS,
                    new Gson().toJsonTree(itemFromDB)));
        });

        //Editar un item segun el id
        put("/items", (request, response) -> {
            try {
                response.type("application/json");
                Item item = new Gson().fromJson(request.body(), Item.class);
                itemService.editItem(item.getId(),item,INDEX,TYPE,restHighLevelClient);
                System.out.println("Item editado --> " + item);
                response.status(201);
                return new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS, new Gson().toJsonTree(item)));
            } catch (Exception exception) {
                response.status(400);
                return new Gson().toJson(new StandardResponse(StatusResponse.ERROR, exception.getMessage()));
            }
        });

        //Eliminar un item segun id
        delete("/items/:id", (request, response) -> {
            response.type("application/json");
            itemService.deleteItem(request.params(":id"),INDEX,TYPE,restHighLevelClient);
            System.out.println("Item eliminado");
            return new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS, "El item fue borrado"));
        });
        //---------------------------------------------------------------
    }
}

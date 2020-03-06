package tarun.ELKTestStack;

import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;


/**
 * Hello world!
 *
 */
public class App {
	// The config parameters for the connection
	private static final String HOST = "localhost";
	private static final int PORT_ONE = 9200;
	private static final String SCHEME = "http";

	private static RestHighLevelClient restHighLevelClient;
	public static void main(String[] args) {
		System.out.println("Hello World!");
		RestHighLevelClient client = makeConnection();

		try {
			//https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-document-index.html
			//create
			IndexRequest request = new IndexRequest("and-1"); 
			
			//request.id("5"); 
			Map<String, String> jsonMap = new HashMap<String, String>();
			jsonMap.put("firstName", "tarun");
			jsonMap.put("lastname", "chowdhry");
//			String jsonString = "{" +
//			        "\"firstName\":\"tarun\"," +
//			        "\"lastname\":\"chowdhry\"" +
//			        "}";
			//request.source(jsonString, XContentType.JSON);
			request.source(jsonMap);
			IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

			System.out.println(indexResponse);
			//https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-search.html
			SearchRequest searchRequest = new SearchRequest("and-1");
			
			
			SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); 
			searchSourceBuilder.query(QueryBuilders.matchAllQuery()); 
			searchRequest.source(searchSourceBuilder);
			SearchResponse searchResp = client.search(searchRequest, RequestOptions.DEFAULT);
			 if (searchResp.status().getStatus() != 200) {
	                throw new Exception(searchResp.toString());
	            }
			 System.out.println("Total search count " + searchResp.getHits().getHits().length );
			 for (SearchHit hit : searchResp.getHits().getHits()) {
				 System.out.println(hit);
			 }
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Implemented Singleton pattern here so that there is just one connection at a
	 * time.
	 * 
	 * @return RestHighLevelClient
	 */
	private static synchronized RestHighLevelClient makeConnection() {

		if (restHighLevelClient == null) {
			restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(HOST, PORT_ONE, SCHEME)));
		}
		return restHighLevelClient;
	}
}

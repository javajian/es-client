package com.es.client;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.GeoBoundingBoxFilterBuilder;
import org.elasticsearch.index.query.GeoDistanceFilterBuilder;
import org.elasticsearch.index.query.GeoPolygonFilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.search.geo.GeoDistanceFilter;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.highlight.HighlightField;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ESClient {

	private Client client;

	public ESClient() {
		client = new TransportClient()
				.addTransportAddress(new InetSocketTransportAddress(
						"localhost", 9300));
	}

	public void close() {
		client.close();
	}

	public void createIndex() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		for (int i = 0; i < 10; i++) {
			Map<String, Object> json = new HashMap<String, Object>();
			json.put("address", "北京市昌平区东小口镇太平家园23号楼3单元" + i + "室");
			json.put("latitude", "35.02");
			json.put("longtitude", "120.03");
			String jsonStr = mapper.writeValueAsString(json);
			client.prepareIndex("addrs", "address_type",
					UUID.randomUUID().toString()).setSource(jsonStr).execute()
					.actionGet();
		}
	}

	public void createBatchIndex() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		for (int i = 0; i < 10; i++) {
			Map<String, Object> json = new HashMap<String, Object>();
			json.put("id", UUID.randomUUID().toString());
			json.put("address", "北京市昌平区东小口镇太平家园26号楼3单元" + i + "室");
			Map<String, Object> location = new HashMap<String, Object>();
			location.put("lat", 35.02);
			location.put("lon", 120.03);
			Map<String, Object> pin = new HashMap<String, Object>();
			pin.put("location", location);
			String jsonStr = mapper.writeValueAsString(json);
			bulkRequest.add(client.prepareIndex("addrs", "address_type")
					.setSource(jsonStr));
		}
		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		System.out.println("是否失败：" + bulkResponse.hasFailures());
	}

	public void getIndex() throws Exception {
		GetResponse response = client.prepareGet("addrs", "", "1").execute()
				.actionGet();
		System.out.println(response.isExists());
	}

	public void deleteDocmentBy_id() throws Exception {
		String id = "6RYeArY5TmSn7Y-I22gmOQ";
		DeleteResponse response = client
				.prepareDelete("addrs", "address_type", id)
				.setOperationThreaded(false).execute().actionGet();
		System.out.println("是否失败：" + !response.getId().equals(id));
	}

	public void searchDocument() throws Exception {
		// QueryBuilder qb = QueryBuilders.multiMatchQuery("24号楼3单元",
		// "address","id");
		QueryBuilder qb = QueryBuilders.matchQuery("address", "24号楼3单元");
		// QueryBuilder qb1 = QueryBuilders.matchQuery("id", "24号楼3单元");

		GeoBoundingBoxFilterBuilder geoFilter = FilterBuilders
				.geoBoundingBoxFilter("pin.location").topLeft(40.73, -74.1)
				.bottomRight(40.717, -73.99);

		SearchResponse response = client.prepareSearch("addrs")
				.setTypes("address_type")
				.setQuery(qb)
				// 使用matchquery，上面可以2选一，如果有多个字段可以查询用第一种
				.addHighlightedField("address")
				.setHighlighterPreTags("<span style=\"color:red;\">")
				.setHighlighterPostTags("</span>").setPostFilter(geoFilter) // 比如日期类型范围限定
				.setFrom(0).setSize(100) // 分页，默认是10条
				.setExplain(true).execute().actionGet();

		SearchHits hits = response.getHits();
		for (int i = 0; i < hits.hits().length; i++) {

			// Map<String, HighlightField> result =
			// hits.getAt(i).highlightFields();
			// HighlightField titleField = result.get("address");
			// Text[] titleTexts = titleField.fragments();
			//
			// String address = "";
			// for(Text text : titleTexts){
			// address += text;
			// }
			// System.out.println(address);
			System.out.println(hits.getAt(i).getSourceAsString());
			// System.out.println(hits.getAt(i).getSource().get("address"));
		}
	}

	// public void createBatchIndex2() throws Exception {
	// ObjectMapper mapper = new ObjectMapper();
	// BulkRequestBuilder bulkRequest = client.prepareBulk();
	// for (int i = 0; i < 10; i++) {
	// Address addr = new Address(UUID.randomUUID().toString(),
	// "北京市昌平区东小口镇太平家园26号楼3单元"+i+"室", ((35.02d+i/10)+","+(120.03d+i/10)));
	// String jsonStr = mapper.writeValueAsString(addr);
	// System.out.println(jsonStr);
	// bulkRequest.add(client.prepareIndex("app2", "addr2").setSource(jsonStr));
	// }
	// BulkResponse bulkResponse = bulkRequest.execute().actionGet();
	// System.out.println("是否失败："+bulkResponse.hasFailures());
	// }

//	public void createBatchIndex2() throws Exception {
//		ObjectMapper mapper = new ObjectMapper();
//		BulkRequestBuilder bulkRequest = client.prepareBulk();
//		for (int i = 0; i < 10; i++) {
//			Address addr = new Address(UUID.randomUUID().toString(),
//					"北京市昌平区东小口镇太平家园26号楼3单元" + i + "室",
//					((35.02d + i) + "," + (120.03d + i)));
//			String jsonStr = mapper.writeValueAsString(addr);
//			System.out.println(jsonStr);
//			bulkRequest.add(client.prepareIndex("app2", "addr2").setSource(
//					jsonStr));
//		}
//		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
//		System.out.println("是否失败：" + bulkResponse.hasFailures());
//	}

	public void createAddressMapper() throws Exception {
		String idxs = "app2";
		String type = "addr2";
		client.admin().indices().prepareCreate(idxs).execute().actionGet();
		XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
				.startObject(type).startObject("properties").startObject("id")
				.field("type", "string").field("store", "yes").endObject()
				.startObject("address").field("type", "string")
				.field("store", "yes").endObject().startObject("location")
				.field("type", "geo_point").field("store", "yes").endObject()
				.endObject().endObject().endObject();
		PutMappingRequest pr = Requests.putMappingRequest(idxs).type(type)
				.source(mapping);
		client.admin().indices().putMapping(pr).actionGet();
		client.close();
	}

	public void searchGeo() {
		QueryBuilder qb = QueryBuilders.matchQuery("address", "24号楼3单元");

		// 四边形查询
		// GeoBoundingBoxFilterBuilder geoFilter =
		// FilterBuilders.geoBoundingBoxFilter("location")
		// .topLeft(40.02,121.03)
		// .bottomRight(36.02,128.03);

		// 园查询
		// GeoDistanceFilterBuilder geoFilter =
		// FilterBuilders.geoDistanceFilter("location")
		// .point(36.02,128.03)
		// .distance(200000, DistanceUnit.KILOMETERS)
		// .geoDistance(GeoDistance.ARC); //ARC=精准匹配，SLOPPY_ARC=快速匹配，PLANE=更快速匹配

		// 多边形查询
		GeoPolygonFilterBuilder geoFilter = FilterBuilders
				.geoPolygonFilter("location").addPoint(40.02, 121.03)
				.addPoint(36.02, 128.03).addPoint(37.02, 122.03)
				.addPoint(40.02, 121.03);

		SearchResponse response = client.prepareSearch("app2")
				.setTypes("addr2")
				.setQuery(qb)
				// 使用matchquery，上面可以2选一，如果有多个字段可以查询用第一种
				.addHighlightedField("address")
				.setHighlighterPreTags("<span style=\"color:red;\">")
				.setHighlighterPostTags("</span>").setPostFilter(geoFilter) // 比如日期类型范围限定
				.setFrom(0).setSize(100) // 分页，默认是10条
				.setExplain(true).execute().actionGet();

		SearchHits hits = response.getHits();
		for (int i = 0; i < hits.hits().length; i++) {
			System.out.println(hits.getAt(i).getSourceAsString());
		}

		client.close();
	}
}

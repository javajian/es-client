package com.es.client.test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.fasterxml.jackson.databind.ObjectMapper;

@RunWith(value=SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:beans.xml"})
public class TestClient {
	
	@Value("${es.cluster}")
	private String esCluster;
	
	@Autowired
	private Client client;
	
	@Test
	public void testProperties(){
		System.out.println("*************");
		System.out.println(esCluster);
		System.out.println("*************");
	}

	
	@Test
	public void testGet(){
//		Settings s = client.settings();
//		System.out.println(s);
	}
	
	@Test
	public void createIndex() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		for (int i = 0; i < 10; i++) {
			Map<String, Object> json = new HashMap<String, Object>();
			json.put("address", "北京市昌平区东小口镇太平家园24号楼3单元" + i + "室");
			json.put("latitude", "35.02");
			json.put("longtitude", "120.03");
			String jsonStr = mapper.writeValueAsString(json);
			client.prepareIndex("addrs", "address_type",
					UUID.randomUUID().toString()).setSource(jsonStr).execute()
					.actionGet();
		}
	}
	
}

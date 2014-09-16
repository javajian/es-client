package com.es.client;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TransportClient
 * @author wangjian
 * @date 2014-9-16 下午3:02:46 
 */
public class ElasticsearchTransportClientFactoryBean extends ElasticsearchAbstractFactoryBean{

	private final Logger logger = LoggerFactory.getLogger(ElasticsearchTransportClientFactoryBean.class);
	
	private String[] esNodes =  { "localhost:9300" };
	
	@Override
	protected Client buildClient() throws Exception {
		ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder();

        if (null != this.settings && null == properties) {
            builder.put(this.settings);
        }

        if (null != this.properties) {
            builder.put(this.properties);
        }

		TransportClient client = new TransportClient(builder.build());

		for (int i = 0; i < esNodes.length; i++) {
			client.addTransportAddress(toAddress(esNodes[i]));
		}

		return client;
	}
	
	private InetSocketTransportAddress toAddress(String address) {
		if (address == null) return null;
		
		String[] splitted = address.split(":");
		int port = 9300;
		if (splitted.length > 1) {
			port = Integer.parseInt(splitted[1]);
		}
		
		return new InetSocketTransportAddress(splitted[0], port);
	}

	public String[] getEsNodes() {
		return esNodes;
	}

	public void setEsNodes(String[] esNodes) {
		this.esNodes = esNodes;
	}
}

package com.es.client;

import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * ElasticSearch client factory bean
 * @author wangjian
 * @date 2014-9-16 下午2:48:16 
 */
public abstract class ElasticsearchAbstractFactoryBean extends ElasticsearchFactoryBean implements FactoryBean<Client>, InitializingBean, DisposableBean{

	private final Logger logger = LoggerFactory.getLogger(ElasticsearchAbstractFactoryBean.class);
	
	protected Client client;
	
	/**
	 * 需要实现获取client
	 * @author wangjian
	 * @date 2014-9-16 下午2:59:36
	 */
	abstract protected Client buildClient() throws Exception;
	
	@Override
	public void destroy() throws Exception {
		try {
			logger.info("Closing ElasticSearch client");
			if (client != null) {
				client.close();
			}
		} catch (final Exception e) {
			logger.error("Error closing ElasticSearch client: ", e);
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		logger.info("Starting ElasticSearch client");
		client = initialize();
	}
	
	private Client initialize() throws Exception {
		client = buildClient();
		return client;
	}

	@Override
	public Client getObject() throws Exception {
		return client;
	}

	@Override
	public Class<?> getObjectType() {
		return Client.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

}

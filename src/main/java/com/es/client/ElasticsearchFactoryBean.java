package com.es.client;

import java.util.Map;
import java.util.Properties;

/**
 * @author wangjian
 * @date 2014-9-16 下午2:55:32 
 */
public abstract class ElasticsearchFactoryBean {
	
	protected Properties properties;
	
	protected Map<String, String> settings;

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	public Map<String, String> getSettings() {
		return settings;
	}

	public void setSettings(Map<String, String> settings) {
		this.settings = settings;
	}
}

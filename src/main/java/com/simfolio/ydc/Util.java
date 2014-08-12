package com.simfolio.ydc;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.impl.HttpSolrServer;



/*
 * get shared elements such as Solr Server and servlet port
 * 
 */
class Util {
	private static Logger logger = Logger.getLogger(Util.class.getName());

	private static HttpSolrServer solr;
	private static int servletPort;
	private static String CONF_FILE = "config.properties";
	private static Properties properties;

	static {
		try {
			File f = new File(CONF_FILE);
			String confFile = f.exists() ? CONF_FILE : "resources/" + CONF_FILE;

			InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(confFile);
			Properties props = new Properties();
			props.load(inputStream);
			properties = props;
			solr = new HttpSolrServer(properties.getProperty("SOLR"));
			servletPort = Integer.parseInt(properties.getProperty("SERVLET_PORT"));
		} catch (IOException ioe) {	
			logger.error("Cannot load properties file.", ioe);
		
		}
	}
	
	
	/* there should only be one SolrServer object that is shared by all calls in this project
	 */
	static HttpSolrServer getSolrServer() {
		return solr;
	}
	
	/*
	 * solr server and the servlet server are not the same
	 */
	static int getServletPort() {
		return servletPort;
	}
}

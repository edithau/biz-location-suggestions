package com.simfolio.ydc;


import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;


/*
 * This is the main executable of the project runnable jar.  
 * java -jar -Dlog4j.configuration=resources/log4j.properties biz2.jar > biz2.log
 * 
 * 
 * getSolr: solr server proxy.  solr port can only be accessed locally.
 * recommend: run mahout recommendation engine.
 * 
 * 
 */
public class MinimalServer {
	
	//private static Logger logger = Logger.getLogger(MinimalServer.class.getName());

    public static void main(String[] args) throws Exception {
        Server server = new Server(Util.getServletPort());
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(com.simfolio.ydc.BusinessLocationRecommenderServlet.class, "/recommend");
        handler.addServletWithMapping(GetSolrServlet.class, "/getSolr");
        server.setHandler(handler);    
        server.start();
        server.join();
    }
}



package com.simfolio.ydc;


import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;

public class MinimalServer {
	
	//private static Logger logger = Logger.getLogger(MinimalServer.class.getName());

    public static void main(String[] args) throws Exception {
        Server server = new Server(8080);
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(BusinessLocationRecommenderServlet.class, "/recommend");
        server.setHandler(handler);    
        server.start();
        server.join();
    }
}



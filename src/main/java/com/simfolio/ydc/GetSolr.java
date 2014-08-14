package com.simfolio.ydc;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.Map;

import javax.servlet.ServletException;

import org.apache.log4j.Logger;


/*
 * a proxy to query Solr server via http
 */
public class GetSolr {

	private static Logger logger = Logger.getLogger(GetSolr.class.getName());
	private static int MAX_SOLR_RESULTS = 100;
	private static String urlTemplate = Util.getSolrServerString() + "/query?q=_MYQ_&wt=json&indent=true"; 
	


	private String myQ;
	private String fq;
	private String rows;
	
	public GetSolr(Map<String, String[]> params) throws ServletException { 
		try {
			myQ =  params.get("myQ")[0];
			myQ = myQ.replace("\\",  "");  // trim off extra slash from string escape
			fq = (params.get("fq") != null) ? params.get("fq")[0] : null;
			try {
				rows = params.get("rows")[0];
				if (Integer.parseInt(rows) > MAX_SOLR_RESULTS) {
					rows = MAX_SOLR_RESULTS + "";
				}
			} catch (Exception e) {
				rows = null;
			}
		} catch (Exception e) {
			logger.error("Invalid parameters: " + params.toString());
			throw new ServletException(e);
		}
	}
	
	public String get() {
		String retVal = "";
		BufferedReader br = null;
		String urlStr = "";
		
	    try {
	    	String encodedQ = URLEncoder.encode(myQ, "UTF-8");
	    	urlStr = urlTemplate.replaceAll("_MYQ_", encodedQ);
	    	if (rows != null) {
	    		urlStr += "&rows=" + rows;
	    	}
	    	
	    	if (fq != null) {
	    		urlStr += "&fq=" + fq;
	    	}
	    	
	        URL solrUrl = new URL(urlStr);
	        URLConnection uconn = solrUrl.openConnection();
	        br = new BufferedReader(new InputStreamReader(uconn.getInputStream()));
	        String inputLine;
	
	        while ((inputLine = br.readLine()) != null) { 
	            retVal += inputLine + '\n';
	        }
	        br.close();		
	    } catch (Exception e ) { 
	    	logger.error("Cannot get url " + urlStr , e);
	    } 
	    
	    return retVal;
	}
}

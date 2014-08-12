package com.simfolio.ydc;


import java.io.*;

import javax.servlet.*;
import javax.servlet.http.*;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;


@SuppressWarnings("serial")
public class BusinessLocationRecommenderServlet extends HttpServlet {
	private static Logger logger = Logger.getLogger(BusinessLocationRecommenderServlet.class.getName());

	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String businessId = "\"" + request.getParameter("id") + "\"";
		String dist = request.getParameter("dist");
		String hasLinks = request.getParameter("hasLinks");
		boolean shouldAddLinks = (hasLinks == null) ? false : request.getParameter("hasLinks").equalsIgnoreCase("true");
		if (!shouldAddLinks) {
			response.setContentType("application/json");
		} else {
			response.setContentType("text/html");
		}

		String results = "[]";
		
		try {
			results = (new BusinessLocationRecommender(businessId, dist)).recommend(shouldAddLinks);
		} catch (SolrServerException e) {
			logger.error("Solr Server is not running");
		}
		response.getWriter().println("{\"results\":" + results + "}");
	}
}

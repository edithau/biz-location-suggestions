package com.simfolio.ydc;

// Import required java libraries
import java.io.*;

import javax.servlet.*;
import javax.servlet.http.*;

import org.apache.solr.client.solrj.SolrServerException;

// Extend HttpServlet class
@SuppressWarnings("serial")
public class BusinessLocationRecommenderServlet extends HttpServlet {

	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String businessId = request.getParameter("id");
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
			e.printStackTrace();
		}
		response.getWriter().println("{\"results\":" + results + "}");
	}
}

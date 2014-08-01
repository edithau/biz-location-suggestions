package com.simfolio.ydc;

import java.io.IOException;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;



@SuppressWarnings("serial")
public class GetSolrServlet extends HttpServlet {

	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		Map<String, String[]> params = request.getParameterMap();
		
		String results = (new com.simfolio.ydc.GetSolr(params)).get();
		response.getWriter().println("{\"results\":" + results + "}");
	}

}

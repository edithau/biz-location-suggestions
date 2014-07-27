package com.simfolio.ydc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.model.BooleanPreference;
import org.apache.mahout.cf.taste.impl.model.BooleanUserPreferenceArray;
import org.apache.mahout.cf.taste.impl.model.GenericBooleanPrefDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.impl.model.MemoryIDMigrator;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;



public class MinimalServer {
	
	private static Logger logger = Logger.getLogger(MinimalServer.class.getName());

    public static void main(String[] args) throws Exception {
        Server server = new Server(8080);
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(SolrQueryServlet.class, "/hello");
        server.setHandler(handler);    
        server.start();
        server.join();
    }

    @SuppressWarnings("serial")
    public static class SolrQueryServlet extends HttpServlet {
    	static HttpSolrServer SOLR = new HttpSolrServer("http://localhost:8983/solr/ydc");
    	static int OPERATING_PERIODS = 21;
		

        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        	String results = "";
			try {
				results = testYDC(request, response);
			} catch (SolrServerException e) {
				e.printStackTrace();
			}
         //   response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().println("{\"results\":" + results + "}");
        }

        
        String testYDC(HttpServletRequest request, HttpServletResponse response) throws SolrServerException {
        	String id = request.getParameter("id");
        	String dist = request.getParameter("dist");
        	String hasLinks = request.getParameter("hasLinks");
        	SolrDocument tgtBiz = getTargetBusiness(id);       
        	String[] next10zips = getHigherReviewCountZipWithinDistance(tgtBiz, dist, 500);
        	SolrDocumentList businessesInNext10zips = getBusinessesWithCatsInZip(tgtBiz, next10zips);
        	MemoryIDMigrator idToThing = new MemoryIDMigrator();
        	HashMap<Long, String> mIdTocomboBizIdStrMap = new HashMap<Long, String>();
        	DataModel dataModel = generateDataModel(businessesInNext10zips, idToThing, mIdTocomboBizIdStrMap);
        	boolean shouldAddLinks = (hasLinks == null) ? false : request.getParameter("hasLinks").equalsIgnoreCase("true");
        	String recommendedZips = recommendBusinessLocation(dataModel, idToThing, tgtBiz, mIdTocomboBizIdStrMap, shouldAddLinks);
        	
        	if (!shouldAddLinks) {
        		response.setContentType("application/json");
        	} else {
        		response.setContentType("text/html");
        	}
        	
            return  recommendedZips;
        }
        
        String recommendBusinessLocation(DataModel model, MemoryIDMigrator idToThing, SolrDocument tgtBiz, 
        		HashMap<Long, String> mIdTocomboBizIdStrMap, boolean shouldAddLinks )  {
        	String retVal = "";
        	
        	try {
	        	LogLikelihoodSimilarity similarity = new LogLikelihoodSimilarity(model);
	        	String comboBusinessId = getComboBusinessId(tgtBiz); 
	        	long tgtBizMId = idToThing.toLongID(comboBusinessId);
	        	long[] similarBusinessMIds = (new ThresholdUserNeighborhood(0.5, similarity, model)).getUserNeighborhood(tgtBizMId);
	     		
	        	HashMap<String, Double>zipsAndScores = new HashMap<String, Double>();
	        	for (long mId : similarBusinessMIds) {
	        		double score = similarity.userSimilarity(tgtBizMId, mId);
	        		
	        		String[] comboId = idToThing.toStringID(mId).split("\\|");
	        		String zip = comboId[1];
	        		
	        		Double totBizScoreInZip =  zipsAndScores.get(zip);
	        		if (totBizScoreInZip != null) {
	        			totBizScoreInZip += score;
	        			zipsAndScores.put(zip, totBizScoreInZip);
	        		} else {
	        			zipsAndScores.put(zip, score);
	        		}
	        	}
	        	
	        	// sort by scores
	        	retVal = convertSortedListToJsonArray(sortByScore(zipsAndScores), shouldAddLinks, tgtBiz);

        	} catch (Exception e) {
        		logger.error("Exception: " + e.getMessage());
        	}
        	
        	//  return a list of {zip, accumulated score for the zip}
        	return retVal;
        }
        
        
        private List<Map.Entry<String, Double>> sortByScore(HashMap<String, Double> sortingMap) {        
        	List<Map.Entry<String, Double>> sortedList = new ArrayList<Map.Entry<String, Double>>();
        	sortedList.addAll(sortingMap.entrySet());
        	
            Comparator<Map.Entry<String, Double>> byMapValues = new Comparator<Map.Entry<String, Double>>() {
                public int compare(Map.Entry<String, Double> left, Map.Entry<String, Double> right) {
                    return right.getValue().compareTo(left.getValue());
                }
            };
            
        	Collections.sort(sortedList, byMapValues); 
        	return sortedList;
        }
        
        private String convertSortedListToJsonArray(List<Map.Entry<String, Double>>list, boolean shouldAddLinks, SolrDocument tgtBiz) {
        	String jsonArray = "[";
        	
        	for (Map.Entry<String, Double> item : list) {
        		String key = (shouldAddLinks) ? addLinkToZip(item.getKey(), tgtBiz) : item.getKey();
        		jsonArray += "{\"zip\":" + key + ",\"score\":" + item.getValue() + "},";
        	}
        	jsonArray = StringUtils.removeEnd(jsonArray, ",");
        	jsonArray += "]";
        	
        	return jsonArray;
        }
        
        
        
        private String addLinkToZip(String zip, SolrDocument tgtBiz) {
        	return zip;
        	// XXX
        	// static const all the  record numbers like 200 here, 100 limit ...
        /*	String myUrl = "http://"
        	String encodedHref = URLEncoder.encode(yourUrl, "UTF-8");
        	return "<a href=\"" + getCatQueryStr(tgtBiz) + "&" + "fq=zip:" + tgtBiz.get("zip") + "&rows=200&wt=json&indent=true\">" + zip + "</a>";
        	*/

        }
        
        
        // XXX server errors are not properly captured
       
 
        
        DataModel generateDataModel(SolrDocumentList businesses, MemoryIDMigrator idToThing, HashMap<Long, String> mIdTocomboBizIdStrMap) {	
    
        	// create ids for static operating schedule name.  schedule0 .. 21 represent 21 operating periods (ie. Tues evening, Wed morning)
        	
    		for (int i=0; i < OPERATING_PERIODS; i++) {
    			idToThing.storeMapping(idToThing.toLongID("schedule"+i), "schedule"+i);
    		}

    		FastByIDMap<PreferenceArray> featureMap = new FastByIDMap<PreferenceArray>();
        	for (SolrDocument business : businesses) {
        		List<Preference> features = new ArrayList<Preference>();
        		String mahoutBusinessStr = getComboBusinessId(business);
        		long mahoutBusinessId = idToThing.toLongID(mahoutBusinessStr);
        		idToThing.storeMapping(mahoutBusinessId, mahoutBusinessStr);
        		mIdTocomboBizIdStrMap.put(mahoutBusinessId, mahoutBusinessStr);
        		
        		
        		char[] opSchedule = business.get("op_schedule").toString().toCharArray();
        		for (int i=0; i<opSchedule.length; i++) {
        			if (opSchedule[i] == '1') {
        				features.add(new BooleanPreference(mahoutBusinessId, idToThing.toLongID("schedule" + i)));
        			}
        		}

        		ArrayList<String> cats = (ArrayList<String>)business.get("categories");
        		for (String cat : cats) {
        			long mahoutCatId = idToThing.toLongID(cat);
        			idToThing.storeMapping(mahoutCatId, cat);
        			features.add(new BooleanPreference(mahoutBusinessId, mahoutCatId));
        		}
        		
        		BooleanUserPreferenceArray businessArray = new BooleanUserPreferenceArray(features);
        		featureMap.put(mahoutBusinessId, businessArray);
        	}
        	
        	return new GenericDataModel(featureMap);
        }
        
        
        String getComboBusinessId(SolrDocument business) {
    		String businessId = business.get("business_id").toString();
    		String zip = business.get("zip").toString();
    		
    		return businessId + "|" + zip;
        }
        
        SolrDocument getTargetBusiness(String id) throws SolrServerException {
            SolrQuery query = new SolrQuery("*:*");
            //query.setQuery("*:*");
            query.addFilterQuery("business_id:" + id);
            query.setFields("zip","business_id", "categories","lat_lon","op_schedule");
            
            QueryResponse response = SOLR.query(query);
            return response.getResults().get(0);
        }
        
        
        String[] getHigherReviewCountZipWithinDistance(SolrDocument tgtBiz, String dist, int returnZipCount) throws SolrServerException {
        	String q = getCatQueryStr(tgtBiz);
            SolrQuery query = new SolrQuery(q);       
            query.addFilterQuery("{!bbox sfield=lat_lon}");
            query.add("pt",tgtBiz.get("lat_lon").toString() );
            query.add("d", dist);
            query.setRows(0);
            query.add("stats", "true");
            query.add("stats.field", "review_count");
            
            
            // XXX check here!
            query.add("stats.facet", "zip");
           // query.add("facet.limit", "250");
            
            QueryResponse response = SOLR.query(query);           
            String[] highReviewCountZips = parseResponseStats(response, returnZipCount);

            return highReviewCountZips;
        }
        
        
        private String[] parseResponseStats(QueryResponse response, int zipCount) {
        	// XXX check 100 limit?
        	String respStr = response.toString().substring(response.toString().indexOf("facets={"));
        	int index, i=0;
        	String zip, reviewCount;
        	
        	String[] segments = respStr.split("facets=\\{\\}\\},");
        	String[] zipsAndCounts = new String[segments.length];
        	
        	// first stats string has "facets={zip={" extra
        	segments[0] = segments[0].substring(13);
        	for (String stats : segments) {
        		zip = stats.substring(0, 5);
        		if ((index = stats.indexOf("sum=")) != -1) {
        			int commaPos = stats.indexOf(',', index);
        			reviewCount = stats.substring(index+4, commaPos);
        			zipsAndCounts[i++] = zip + "=" + reviewCount;
        		} else {
        			logger.error("QueryResponse: zip=" + zip + " in stats facet has no review_count.\n" + response.toString());
        		}
        		
        	}
        	
        	Arrays.sort(zipsAndCounts, new Comparator<String>() {
                public int compare(String left,String right) {
                    return right.substring(right.indexOf('=')).compareTo(left.substring(right.indexOf('=')));
                }
        	});
        	
        	
        	String[] zips = new String[zipsAndCounts.length];
        	for (i=0; i<zipsAndCounts.length; i++) {
        		zips[i] = zipsAndCounts[i].substring(0, 5); // take the zip code only
        	}
        	return zips;
        }
        
        
        SolrDocumentList getBusinessesWithCatsInZip(SolrDocument tgtBiz, String[] zips) throws SolrServerException {
        	String catQstr = getCatQueryStr(tgtBiz);
        	String zipQstr = "";
        	for (String zip : zips) {
        		zipQstr += "zip:" + zip + " ";
        	}
        	String q = "(" + zipQstr + ") AND (" + catQstr + ")";
            SolrQuery query = new SolrQuery(q);   
            query.add("fl", "business_id zip categories op_schedule");
            query.setRows(2000);
            
            QueryResponse response = SOLR.query(query); 
            
            return response.getResults();
        }

        
        String getCatQueryStr(SolrDocument tgtBiz) {
        	ArrayList<String> cats = (ArrayList<String>)tgtBiz.get("categories");
        	String q = "";
        	for (String cat : cats) {
        		q += "categories:\"" + cat + "\" ";
        	}
        	return q.trim();
        }
    }
}


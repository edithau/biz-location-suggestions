package com.simfolio.ydc;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.model.BooleanPreference;
import org.apache.mahout.cf.taste.impl.model.BooleanUserPreferenceArray;
import org.apache.mahout.cf.taste.impl.model.GenericBooleanPrefDataModel;
import org.apache.mahout.cf.taste.impl.model.MemoryIDMigrator;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;



/**
 * This is the business location recommendation engine.  It trains mahout data in memory and provide suggestions
 * 
 * Steps: 
 * 1. within  given distance, search for businesses with at least one common cat with the target
 * 2. group returned businesses by zip codes, aggregate the review counts 
 * 3. select zip codes with higher total review counts than target's zip code total review count
 * 
 * At this point, we have the zip codes with higher "foot traffic" than target's
 * Next to do is to calculate how similar these busineses are with the target
 * 
 * 4. calculate the similarity scores between target and the selected businesses in the selected zip codes.  
 * The input of the calculation is a boolean matrix where business categories and operating hours are columns and businesses
 * are rows.  '1' means the business has this category or open at this time segment of the day.  The matrix is then applied to 
 * an algorithm (ie. Loglikelihood) to compute the similarity scores between the target business and the selected businesses
 * 
 * 5. aggregate the similarity scores by zip code
 * 6. sort the aggregated scores and return the results 
 * 
 * The final results list the location(zip) with businesses similar to target and with "foot traffic" higher than target's.
 * 
 * 
 * @author edith
 *
 */
public class BusinessLocationRecommender {
	private static Logger logger = Logger.getLogger(BusinessLocationRecommender.class.getName());

	
	private static String SOLR_FACADE_PATH = "/getSolr?myQ=";
	private static HttpSolrServer SOLR = new HttpSolrServer("http://localhost:8983/solr/ydc");
	private static int OPERATING_PERIODS = 21;
	
	//max number of zip codes (to process) with total review count higher than tgtBiz's zip code total review count
	private static int MAX_TOP_ZIPS = 50;
	
	// max num of scores to return
	private static int MAX_SCORES_NUM = 15; 


	private SolrDocument tgtBiz;
	private MemoryIDMigrator idToThing = new MemoryIDMigrator();

	private DataModel model;

	public BusinessLocationRecommender(String businessId, String dist) throws SolrServerException {
		// get info about the target business from Solr
		tgtBiz = getTargetBusiness(businessId);       
		
		// Step 2 
		String[] topZips = getHigherReviewCountZipWithinDistance(dist, MAX_TOP_ZIPS);
		
		// Step 3
		SolrDocumentList businessesInTopZips = getBusinessesWithCatsInZip(topZips);
		
		// generate an in memory mahout data model for similarity calculation
		model = generateDataModel(businessesInTopZips);
	}
	

	// Step 4 -6 
    public String recommend(boolean shouldAddLinks)  {
    	String retVal = "";
    	
    	try {
        	LogLikelihoodSimilarity similarity = new LogLikelihoodSimilarity(model);
        	String comboBusinessId = getComboBusinessId(tgtBiz); 
        	long tgtBizMId = idToThing.toLongID(comboBusinessId);
        	long[] similarBusinessMIds = (new ThresholdUserNeighborhood(0.5, similarity, model)).getUserNeighborhood(tgtBizMId);
     		
        	// instead of calling a recommender.  I take all businesses above the Similarity threshold, group them by zip, and return the 
        	// sorted aggregated scores in Json
        	HashMap<String, Double[]>zipsAndScores = new HashMap<String, Double[]>();
        	for (long mId : similarBusinessMIds) {
        		String[] comboId = idToThing.toStringID(mId).split("\\|");
        		String zip = comboId[1];
        		
        		if (zip.equals(tgtBiz.get("zip").toString())) {
        			continue;
        		}
        		
        		double score = similarity.userSimilarity(tgtBizMId, mId);

        		Double[] totBizScoreInZip =  zipsAndScores.get(zip);
        		if (totBizScoreInZip != null) {
        			totBizScoreInZip[0] += score;
        			totBizScoreInZip[1] += 1.0;
        			zipsAndScores.put(zip, totBizScoreInZip);
        		} else {
        			totBizScoreInZip = new Double[2];
        			totBizScoreInZip[0] = score;
        			totBizScoreInZip[1] = 1.0;
        			zipsAndScores.put(zip, totBizScoreInZip);
        		}
        	}	
        	// sort by scores
        	retVal = convertSortedListToJsonArray(sortByScore(zipsAndScores), shouldAddLinks);

    	} catch (Exception e) {
    		logger.error("Exception in recommend(): " + e);
    	}
    	
    	//  return a list of {zip, accumulated score for the zip}
    	return retVal;
    }
	
    
    private List<Map.Entry<String, Double[]>> sortByScore(HashMap<String, Double[]> sortingMap) {        
    	List<Map.Entry<String, Double[]>> sortedList = new ArrayList<Map.Entry<String, Double[]>>();
    	sortedList.addAll(sortingMap.entrySet());
    	
        Comparator<Map.Entry<String, Double[]>> byMapValues = new Comparator<Map.Entry<String, Double[]>>() {
            public int compare(Map.Entry<String, Double[]> left, Map.Entry<String, Double[]> right) {
                return right.getValue()[0].compareTo(left.getValue()[0]);
            }
        };
        
    	Collections.sort(sortedList, byMapValues); 
    	return sortedList;
    }
    
    
    // XXX only return the top 20 zips  refactor
    private String convertSortedListToJsonArray(List<Map.Entry<String, Double[]>>list, boolean shouldAddLinks) {
    	String jsonArray = "[";
    	String key, lineSep;
    	int i = 0;
    	
    	for (Map.Entry<String, Double[]> item : list) {
    		if (shouldAddLinks) {
    			key = addLinkToZip(item.getKey());
    			lineSep = "<br>";
    		} else {
    			key = item.getKey();
    			lineSep = "\n";
    		}
    		jsonArray += "{\"zip\":" + key + ",\"score\":" + item.getValue()[0] + ",\"count\":" + item.getValue()[1] 
    				+ "}," + lineSep;
    		if (++i > MAX_SCORES_NUM) {
    			break;
    		}
    	}
    	jsonArray = jsonArray.substring(0, jsonArray.lastIndexOf(','));
    	jsonArray += "]";
    	
    	return jsonArray;
    }
	

	private String addLinkToZip(String zip) {
		String retVal = "";
		try {
			String qStr = getCatQueryStr(tgtBiz) + "&fq=zip:" + zip;
			String encodedQStr = URLEncoder.encode(qStr, "UTF-8");
			retVal =  "<a href=\"" + SOLR_FACADE_PATH + encodedQStr + "\">"	+ zip + "</a>";
		} catch (Exception e) {
			logger.error("Cannot generate solr query link for zipcode " + zip, e) ;
		}
		return retVal;
	}
	
	
	SolrDocument getTargetBusiness(String id) throws SolrServerException {
		SolrQuery query = new SolrQuery("*:*");
		query.addFilterQuery("business_id:" + id);
		query.setFields("zip","business_id", "categories","lat_lon","op_schedule");

		QueryResponse response = SOLR.query(query);
		return response.getResults().get(0);
	}

	
	String[] getHigherReviewCountZipWithinDistance(String dist, int returnZipCount) throws SolrServerException {
		String catQStr = getCatQueryStr(tgtBiz);
		SolrQuery query = new SolrQuery(catQStr);       
		query.addFilterQuery("{!bbox sfield=lat_lon}");
		query.add("pt",tgtBiz.get("lat_lon").toString() );
		query.add("d", dist);
		query.add("fl", "zip review_count");
		query.setRows(6000);
		
		QueryResponse response = SOLR.query(query);  
		return parseReviewCountInZipResponse(response.getResults(), returnZipCount);
	}
	
	
	// each result is [zip, review_count] for one business
	String[] parseReviewCountInZipResponse(SolrDocumentList results, int maxZipNum) {
		HashMap<Integer, Integer>zipReviewCountMap = new HashMap<Integer, Integer>();
		
		// aggregate total review count per zip
		for (SolrDocument result : results) {
			Integer reviewCount = (Integer)result.get("review_count");
			Integer zip = (Integer)result.get("zip");

			if (zipReviewCountMap.get("zip") == null) {
				zipReviewCountMap.put(zip, reviewCount);
			} else {
				int totCount = zipReviewCountMap.get(zip);
				zipReviewCountMap.put(zip, totCount+reviewCount);
			}
		}
	
		return getHigherReviewCount(sortByReviewCount(zipReviewCountMap), maxZipNum);
	}
	
	
	private String[] getHigherReviewCount(List<Map.Entry<Integer, Integer>> sortedZipReviewList, int maxZipNum) {
		int sortedListSize = (maxZipNum < sortedZipReviewList.size()) ? maxZipNum : sortedZipReviewList.size();
		String[] sortedList = new String[sortedListSize];

		int i;
		for (i=0; i < sortedListSize; i++) {
			sortedList[i] = sortedZipReviewList.get(i).getKey() + "";
		}	
		return sortedList;
	}

	// XXX refactor
    private List<Map.Entry<Integer, Integer>> sortByReviewCount(HashMap<Integer, Integer> sortingMap) {        
    	List<Map.Entry<Integer, Integer>> sortedList = new ArrayList<Map.Entry<Integer, Integer>>();
    	sortedList.addAll(sortingMap.entrySet());
    	
        Comparator<Map.Entry<Integer, Integer>> byMapValues = new Comparator<Map.Entry<Integer, Integer>>() {
            public int compare(Map.Entry<Integer, Integer> left, Map.Entry<Integer, Integer> right) {
                return right.getValue().compareTo(left.getValue());
            }
        };
        
    	Collections.sort(sortedList, byMapValues); 
    	return sortedList;
    }

	SolrDocumentList getBusinessesWithCatsInZip(String[] zips) throws SolrServerException {
		String catQstr = getCatQueryStr(tgtBiz);
		String zipQstr = "";
		boolean foundTgt = false;
		for (String zip : zips) {
			zipQstr += "zip:" + zip + " ";
			if (!foundTgt && zip == tgtBiz.get("zip")) {
				foundTgt = true;
			}
		}
		
		// need to make sure tgtBiz's zip is in the DRM
		if (!foundTgt) {
			zipQstr += "zip:" + tgtBiz.get("zip") + " ";
		}
		
		String q = "(" + zipQstr + ") AND (" + catQstr + ")";
		SolrQuery query = new SolrQuery(q);   
		query.add("fl", "business_id zip categories op_schedule");
		query.setRows(2000);

		QueryResponse response = SOLR.query(query); 

		return response.getResults();
	}


	DataModel generateDataModel(SolrDocumentList businesses) {	
		// create ids for static operating schedule name.  schedule0 .. 21 represent 21 operating periods (ie. Tues evening, Wed morning)
		for (int i=0; i < OPERATING_PERIODS; i++) {
			idToThing.storeMapping(idToThing.toLongID("schedule"+i), "schedule"+i);
		}

		FastByIDMap<PreferenceArray> featureMap = new FastByIDMap<PreferenceArray>();
		List<String>uniqueCats = new ArrayList<String>();
		for (SolrDocument business : businesses) {
			List<Preference> features = new ArrayList<Preference>();
			String mahoutBusinessStr = getComboBusinessId(business);
			long mahoutBusinessId = idToThing.toLongID(mahoutBusinessStr);
			idToThing.storeMapping(mahoutBusinessId, mahoutBusinessStr);


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
				if (!uniqueCats.contains(cat)) {
					uniqueCats.add(cat);
				}
			}

			BooleanUserPreferenceArray businessArray = new BooleanUserPreferenceArray(features);
			featureMap.put(mahoutBusinessId, businessArray);
		}
		logger.debug("Num of unique categories in top zips=" + uniqueCats.size());

		//return new GenericDataModel(featureMap);
		return new GenericBooleanPrefDataModel(GenericBooleanPrefDataModel.toDataMap(featureMap));
	}



	String getCatQueryStr(SolrDocument tgtBiz) {
		ArrayList<String> cats = (ArrayList<String>)tgtBiz.get("categories");
		String q = "";
		for (String cat : cats) {
			q += "categories:\"" + cat + "\" ";
		}
		return q.trim();
	}
	
    String getComboBusinessId(SolrDocument business) {
		String businessId = business.get("business_id").toString();
		String zip = business.get("zip").toString();
		
		return businessId + "|" + zip;
    }


}

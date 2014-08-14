package com.simfolio.ydc;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.IRStatistics;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.eval.GenericRecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.model.BooleanPreference;
import org.apache.mahout.cf.taste.impl.model.BooleanUserPreferenceArray;
import org.apache.mahout.cf.taste.impl.model.GenericBooleanPrefDataModel;
import org.apache.mahout.cf.taste.impl.model.MemoryIDMigrator;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericBooleanPrefUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.apache.mahout.common.RandomUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;


/* 
 * This program evaluates the quality of the recommendation using precision and recall.
 * With the Yelp academic business dataset, on average, 
 * 83% of the top 10 recommendations are good;
 * 83% of all good recommendations are among those recommended.
 * 
 * This program takes about 6 mins to complete.  To run within an exe jar:
 * java -Dlog4j.configuration=resources/log4j.properties -cp biz2.jar com.simfolio.ydc.EvaluateRecommender 
 * 
 */
public class EvaluateRecommender {
	private static HttpSolrServer solr;
	
	// max num of record to retrieve from solr for an evaluation
	private static int MAX_ROWS = 30000;

	// 3 operating time segments (morning, afternoon, evening) x 7 days
	private static int OPERATING_PERIODS = 21;
	
	// threshold for the similarity score
	static float threshold = 0.5f;
	
	// in memory mahout DRM mapping
	private static MemoryIDMigrator idToThing = new MemoryIDMigrator();
	
	public static void main(String[] args) throws IOException, TasteException, SolrServerException {
		//RandomUtils.useTestSeed();
		
		printTimeStamp();
		solr = Util.getSolrServer(); 
		
		// return the categories and operating schedule of all businesses in the dataset
		SolrDocumentList businesses = getBusinesses();
		
		// generate an in memory mahout DRM for evaluaiton
		DataModel model = generateDataModel(businesses);

		GenericRecommenderIRStatsEvaluator evaluator = new GenericRecommenderIRStatsEvaluator();
		RecommenderBuilder builder = new MyRecommenderBuilder();
		IRStatistics stats = evaluator.evaluate(builder, null, model, null, 10, 
				threshold, /*GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD,*/ 0.2);

		System.out.println("Precision: " + stats.getPrecision());
		System.out.println("Recall" + stats.getRecall());
		printTimeStamp();
	}
	
	
	private static SolrDocumentList getBusinesses() throws SolrServerException {
		SolrQuery query = new SolrQuery("*:*");
		query.setFields("business_id", "categories", "op_schedule");
		query.setRows(MAX_ROWS);

		QueryResponse response = solr.query(query);
		return response.getResults();
	}
	
	private static DataModel generateDataModel(SolrDocumentList businesses) {	
		// create ids for static operating schedule name.  schedule0 .. 21 represent 21 operating periods (ie. Tues evening, Wed morning)
		for (int i=0; i < OPERATING_PERIODS; i++) {
			idToThing.storeMapping(idToThing.toLongID("schedule"+i), "schedule"+i);
		}

		FastByIDMap<PreferenceArray> featureMap = new FastByIDMap<PreferenceArray>();
		for (SolrDocument business : businesses) {
			List<Preference> features = new ArrayList<Preference>();
			String mahoutBusinessIdStr = business.get("business_id").toString();
			long mahoutBusinessId = idToThing.toLongID(mahoutBusinessIdStr);
			idToThing.storeMapping(mahoutBusinessId, mahoutBusinessIdStr);


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

		return new GenericBooleanPrefDataModel(GenericBooleanPrefDataModel.toDataMap(featureMap));
	}
	
	private static void printTimeStamp() {
		Date date = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy h:mm:ss a");
		String formattedDate = sdf.format(date);
		System.out.println(formattedDate);
	}

}


class MyRecommenderBuilder implements RecommenderBuilder {
	public Recommender buildRecommender(DataModel dataModel) throws TasteException {
		UserSimilarity similarity =  new LogLikelihoodSimilarity(dataModel);
		UserNeighborhood neighborhood = new ThresholdUserNeighborhood(EvaluateRecommender.threshold, similarity, dataModel);
		return new GenericBooleanPrefUserBasedRecommender(dataModel, neighborhood, similarity);
	}
}

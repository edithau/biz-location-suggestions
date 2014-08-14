package com.simfolio.ydc;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;

import static org.junit.Assert.*;

import java.util.regex.Pattern;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.junit.Test;

public class BusinessLocationRecommenderTest {
	private static Pattern solrDocFields = Pattern.compile("\\{(.*?)=(.*?), (.*?)=(.*?)\\}");

	
	/* construct a list of SolrDocuments for testing */
	private SolrDocumentList getZipAndReviewCount() {
		SolrDocumentList retVal = new SolrDocumentList();
		
		// 243 records contains category "acupuncture"
		String solrDocs = "SolrDocument{zip=85260, review_count=3}, SolrDocument{zip=85257, review_count=3}, SolrDocument{zip=85142, review_count=5}, SolrDocument{zip=85028, review_count=5}, SolrDocument{zip=85258, review_count=3}, SolrDocument{zip=85323, review_count=3}, SolrDocument{zip=85254, review_count=4}, SolrDocument{zip=85018, review_count=3}, SolrDocument{zip=85020, review_count=3}, SolrDocument{zip=85018, review_count=6}, SolrDocument{zip=85286, review_count=4}, SolrDocument{zip=85006, review_count=3}, SolrDocument{zip=85016, review_count=3}, SolrDocument{zip=85032, review_count=3}, SolrDocument{zip=85032, review_count=3}, SolrDocument{zip=85260, review_count=4}, SolrDocument{zip=85286, review_count=3}, SolrDocument{zip=85003, review_count=3}, SolrDocument{zip=85048, review_count=3}, SolrDocument{zip=85260, review_count=6}, SolrDocument{zip=85234, review_count=3}, SolrDocument{zip=85142, review_count=4}, SolrDocument{zip=85224, review_count=3}, SolrDocument{zip=85044, review_count=4}, SolrDocument{zip=85201, review_count=4}, SolrDocument{zip=85017, review_count=5}, SolrDocument{zip=85255, review_count=5}, SolrDocument{zip=85254, review_count=6}, SolrDocument{zip=85201, review_count=3}, SolrDocument{zip=85006, review_count=4}, SolrDocument{zip=85032, review_count=3}, SolrDocument{zip=85224, review_count=3}, SolrDocument{zip=85253, review_count=3}, SolrDocument{zip=85006, review_count=5}, SolrDocument{zip=85306, review_count=3}, SolrDocument{zip=85351, review_count=4}, SolrDocument{zip=85295, review_count=5}, SolrDocument{zip=85251, review_count=3}, SolrDocument{zip=85206, review_count=4}, SolrDocument{zip=85037, review_count=5}, SolrDocument{zip=85296, review_count=3}, SolrDocument{zip=85255, review_count=4}, SolrDocument{zip=85012, review_count=4}, SolrDocument{zip=85306, review_count=3}, SolrDocument{zip=85258, review_count=9}, SolrDocument{zip=85260, review_count=3}, SolrDocument{zip=85283, review_count=4}, SolrDocument{zip=85085, review_count=4}, SolrDocument{zip=85301, review_count=7}, SolrDocument{zip=85139, review_count=3}, SolrDocument{zip=85027, review_count=4}, SolrDocument{zip=85251, review_count=4}, SolrDocument{zip=85254, review_count=5}, SolrDocument{zip=85204, review_count=3}, SolrDocument{zip=85233, review_count=3}, SolrDocument{zip=85018, review_count=7}, SolrDocument{zip=85251, review_count=4}, SolrDocument{zip=85296, review_count=3}, SolrDocument{zip=85308, review_count=9}, SolrDocument{zip=85258, review_count=3}, SolrDocument{zip=85212, review_count=6}, SolrDocument{zip=85226, review_count=3}, SolrDocument{zip=85018, review_count=4}, SolrDocument{zip=85251, review_count=3}, SolrDocument{zip=85224, review_count=4}, SolrDocument{zip=85254, review_count=4}, SolrDocument{zip=85204, review_count=3}, SolrDocument{zip=85351, review_count=6}, SolrDocument{zip=85022, review_count=3}, SolrDocument{zip=85050, review_count=5}, SolrDocument{zip=85013, review_count=7}, SolrDocument{zip=85053, review_count=4}, SolrDocument{zip=85016, review_count=4}, SolrDocument{zip=85048, review_count=12}, SolrDocument{zip=85224, review_count=3}, SolrDocument{zip=85242, review_count=4}, SolrDocument{zip=85225, review_count=5}, SolrDocument{zip=85016, review_count=3}, SolrDocument{zip=85374, review_count=5}, SolrDocument{zip=85251, review_count=3}, SolrDocument{zip=85041, review_count=5}, SolrDocument{zip=85085, review_count=3}, SolrDocument{zip=85019, review_count=6}, SolrDocument{zip=85282, review_count=3}, SolrDocument{zip=85306, review_count=3}, SolrDocument{zip=85203, review_count=4}, SolrDocument{zip=85206, review_count=6}, SolrDocument{zip=85258, review_count=4}, SolrDocument{zip=85382, review_count=4}, SolrDocument{zip=85251, review_count=4}, SolrDocument{zip=85003, review_count=4}, SolrDocument{zip=85282, review_count=3}, SolrDocument{zip=85310, review_count=7}, SolrDocument{zip=85040, review_count=3}, SolrDocument{zip=85392, review_count=4}, SolrDocument{zip=85308, review_count=4}, SolrDocument{zip=85258, review_count=3}, SolrDocument{zip=85014, review_count=7}, SolrDocument{zip=85016, review_count=3}, SolrDocument{zip=85037, review_count=3}, SolrDocument{zip=85236, review_count=5}, SolrDocument{zip=85260, review_count=3}, SolrDocument{zip=85258, review_count=3}, SolrDocument{zip=85224, review_count=3}, SolrDocument{zip=85296, review_count=6}, SolrDocument{zip=85248, review_count=5}, SolrDocument{zip=85212, review_count=4}, SolrDocument{zip=85326, review_count=4}, SolrDocument{zip=85028, review_count=4}, SolrDocument{zip=85004, review_count=5}, SolrDocument{zip=85260, review_count=7}, SolrDocument{zip=85254, review_count=3}, SolrDocument{zip=85224, review_count=6}, SolrDocument{zip=85382, review_count=6}, SolrDocument{zip=85234, review_count=4}, SolrDocument{zip=85255, review_count=3}, SolrDocument{zip=85016, review_count=30}, SolrDocument{zip=85297, review_count=3}, SolrDocument{zip=85305, review_count=9}, SolrDocument{zip=85016, review_count=6}, SolrDocument{zip=85202, review_count=4}, SolrDocument{zip=85028, review_count=4}, SolrDocument{zip=85282, review_count=6}, SolrDocument{zip=85201, review_count=3}, SolrDocument{zip=85044, review_count=6}, SolrDocument{zip=85033, review_count=3}, SolrDocument{zip=85282, review_count=4}, SolrDocument{zip=85284, review_count=5}, SolrDocument{zip=85331, review_count=5}, SolrDocument{zip=85225, review_count=3}, SolrDocument{zip=85222, review_count=3}, SolrDocument{zip=85018, review_count=3}, SolrDocument{zip=85032, review_count=5}, SolrDocument{zip=85338, review_count=3}, SolrDocument{zip=85251, review_count=3}, SolrDocument{zip=85226, review_count=3}, SolrDocument{zip=85224, review_count=3}, SolrDocument{zip=85306, review_count=6}, SolrDocument{zip=85004, review_count=4}, SolrDocument{zip=85016, review_count=4}, SolrDocument{zip=85308, review_count=3}, SolrDocument{zip=85392, review_count=10}, SolrDocument{zip=85260, review_count=17}, SolrDocument{zip=85020, review_count=3}, SolrDocument{zip=85032, review_count=4}, SolrDocument{zip=85122, review_count=3}, SolrDocument{zip=85283, review_count=5}, SolrDocument{zip=85008, review_count=3}, SolrDocument{zip=85206, review_count=3}, SolrDocument{zip=85255, review_count=3}, SolrDocument{zip=85248, review_count=4}, SolrDocument{zip=85254, review_count=10}, SolrDocument{zip=85254, review_count=3}, SolrDocument{zip=85050, review_count=4}, SolrDocument{zip=85260, review_count=7}, SolrDocument{zip=85213, review_count=3}, SolrDocument{zip=85013, review_count=3}, SolrDocument{zip=85226, review_count=4}, SolrDocument{zip=85395, review_count=4}, SolrDocument{zip=85224, review_count=3}, SolrDocument{zip=85020, review_count=4}, SolrDocument{zip=85014, review_count=3}, SolrDocument{zip=85251, review_count=19}, SolrDocument{zip=85251, review_count=6}, SolrDocument{zip=85258, review_count=3}, SolrDocument{zip=85205, review_count=3}, SolrDocument{zip=85296, review_count=3}, SolrDocument{zip=85206, review_count=3}, SolrDocument{zip=85260, review_count=4}, SolrDocument{zip=85050, review_count=3}, SolrDocument{zip=85004, review_count=4}, SolrDocument{zip=85032, review_count=8}, SolrDocument{zip=85202, review_count=4}, SolrDocument{zip=85003, review_count=3}, SolrDocument{zip=85210, review_count=3}, SolrDocument{zip=85260, review_count=6}, SolrDocument{zip=85296, review_count=4}, SolrDocument{zip=85351, review_count=3}, SolrDocument{zip=85018, review_count=7}, SolrDocument{zip=85044, review_count=14}, SolrDocument{zip=85260, review_count=13}, SolrDocument{zip=85020, review_count=3}, SolrDocument{zip=85013, review_count=3}, SolrDocument{zip=85255, review_count=4}, SolrDocument{zip=85248, review_count=4}, SolrDocument{zip=85251, review_count=4}, SolrDocument{zip=85260, review_count=3}, SolrDocument{zip=85295, review_count=5}, SolrDocument{zip=85022, review_count=7}, SolrDocument{zip=85374, review_count=4}, SolrDocument{zip=85017, review_count=4}, SolrDocument{zip=85381, review_count=5}, SolrDocument{zip=85255, review_count=4}, SolrDocument{zip=85254, review_count=3}, SolrDocument{zip=85260, review_count=9}, SolrDocument{zip=85331, review_count=3}, SolrDocument{zip=85210, review_count=7}, SolrDocument{zip=85022, review_count=3}, SolrDocument{zip=85008, review_count=3}, SolrDocument{zip=85258, review_count=4}, SolrDocument{zip=85258, review_count=6}, SolrDocument{zip=85140, review_count=3}, SolrDocument{zip=85044, review_count=4}, SolrDocument{zip=85383, review_count=3}, SolrDocument{zip=85044, review_count=4}, SolrDocument{zip=85251, review_count=4}, SolrDocument{zip=85008, review_count=8}, SolrDocument{zip=85282, review_count=4}, SolrDocument{zip=85004, review_count=3}, SolrDocument{zip=85260, review_count=4}, SolrDocument{zip=85013, review_count=3}, SolrDocument{zip=85374, review_count=11}, SolrDocument{zip=85308, review_count=7}, SolrDocument{zip=85339, review_count=3}, SolrDocument{zip=85251, review_count=4}, SolrDocument{zip=85018, review_count=4}, SolrDocument{zip=85234, review_count=3}, SolrDocument{zip=85143, review_count=4}, SolrDocument{zip=85382, review_count=3}, SolrDocument{zip=85381, review_count=4}, SolrDocument{zip=85015, review_count=3}, SolrDocument{zip=85048, review_count=3}, SolrDocument{zip=85377, review_count=3}, SolrDocument{zip=85050, review_count=13}, SolrDocument{zip=85006, review_count=4}, SolrDocument{zip=85374, review_count=4}, SolrDocument{zip=85234, review_count=8}, SolrDocument{zip=85034, review_count=6}, SolrDocument{zip=85282, review_count=3}, SolrDocument{zip=85224, review_count=5}, SolrDocument{zip=85382, review_count=4}, SolrDocument{zip=85258, review_count=9}, SolrDocument{zip=85260, review_count=3}, SolrDocument{zip=85260, review_count=4}, SolrDocument{zip=85392, review_count=5}, SolrDocument{zip=85205, review_count=4}, SolrDocument{zip=85296, review_count=5}, SolrDocument{zip=85255, review_count=3}, SolrDocument{zip=85018, review_count=7}, SolrDocument{zip=85118, review_count=3}, SolrDocument{zip=85032, review_count=3}, SolrDocument{zip=85283, review_count=4}, SolrDocument{zip=85139, review_count=4}";
		Matcher matchPattern = solrDocFields.matcher(solrDocs);

		int numFound = 0;
		while(matchPattern.find()) {
			SolrDocument doc = new SolrDocument();
			doc.addField(matchPattern.group(1), Integer.parseInt(matchPattern.group(2)));
			doc.addField(matchPattern.group(3), Integer.parseInt(matchPattern.group(4)));
			retVal.add(doc);
			numFound++;
		}
		
		retVal.setNumFound(numFound);
		return retVal;
	}
	
	/* construct a mock business SolrDocument object */
	private SolrDocument getBusiness() {
		SolrDocument doc = new SolrDocument();
		doc.addField("business_id", "6bjKi4wtIJWD24bFtPV4Ow");
		doc.addField("zip", 85302);
		ArrayList<String> cats = new ArrayList<String>();
		cats.add("Korean");
		cats.add("Restaurants");
		doc.addField("categories", cats);
		doc.addField("lat_lon", "33.578533,-112.15235699999999");
		doc.addField("op_schedule", "000000000000000000000");
		return doc;
	}
	
	@Test
	// test parseReviewCountInZipResponse() returns the top 10 zip codes with the highest aggregated review counts (in desc order) 
	public void testParseZipAndReviewCountResponse() {
		BusinessLocationRecommender blr = new BusinessLocationRecommender();
		String[] highReviewCountZips = blr.parseReviewCountInZipResponse(getZipAndReviewCount(), 10);
		
		assertEquals(Arrays.toString(highReviewCountZips), 
				"[85050, 85258, 85305, 85234, 85008, 85210, 85018, 85308, 85310, 85301]");
		
	}
	
	@Test
	public void testGetQueryString() {
		SolrDocument biz = getBusiness();
		BusinessLocationRecommender blr = new BusinessLocationRecommender();
		String catQStr = blr.getCatQueryStr(biz);
		assertEquals("categories:\"Korean\" categories:\"Restaurants\"", catQStr);
	}
	
	@Test
	public void testGetComboBusinessId() {
		SolrDocument biz = getBusiness();
		BusinessLocationRecommender blr = new BusinessLocationRecommender();
		String comboId = blr.getComboBusinessId(biz);
		assertEquals("6bjKi4wtIJWD24bFtPV4Ow|85302", comboId);
	}	

}

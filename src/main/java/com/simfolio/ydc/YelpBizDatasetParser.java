package com.simfolio.ydc;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.net.URL;
import org.apache.commons.io.FileUtils;

import org.apache.log4j.Logger;

import com.eclipsesource.json.JsonObject;



/*
 * Remove Json Objects from input with no zipcode or no business categories.  
 * convert lat/lon data into solr indexing format.
 * convert hours into solr indexing format
 * 
 * input: Yelp academic business data set in Json format
 * output: Json for solr indexing
 */
public class YelpBizDatasetParser {
	private File outputDir;
	private File yelpBizDataFile;
	
	private static Logger logger = Logger.getRootLogger();
	
	private static String SOLR_JSON_PREFIX = "solr_";
	
	// track num of invalid businesss address in the input json file
	private int invalidAddressCount = 0;
	
	// track num of valid businesses parsed in the input json file
	private int bizCount = 0;

	private int noCategoryCount = 0;

	private int noOpHoursCount = 0;

	private File solrJsonFile;

	public static void main(String[] args) throws Exception {
		// arg0: Yelp business dataset json file, 
		// arg2 : output dir for solr ready json file
		YelpBizDatasetParser ybdp = new YelpBizDatasetParser(args[0], args[1]);
		ybdp.parse();
		System.out.println(ybdp.getSummary());
	}
	
	
	
	public YelpBizDatasetParser(String yelpBizJsonFilename, String oDir) throws IOException, URISyntaxException {
		// input files
		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		URL url = cl.getResource(yelpBizJsonFilename); 
		yelpBizDataFile = new File(url.toURI()); 

		
		// all outputs (incl tmp) go to this dir
		outputDir = new File (oDir);
		FileUtils.deleteDirectory(outputDir);
		outputDir.mkdir();
		solrJsonFile = new File(outputDir + "/" + SOLR_JSON_PREFIX + yelpBizDataFile.getName());
	}
	
	
	public void parse() throws Exception {
		parseYelpBizDataset();
	}
	
	
	
	/* parse the original yelp business json file.  clean up invalid data and select fields that are needed for solr indexing */
	public void parseYelpBizDataset() throws IOException {	
		BufferedReader br = null;
		PrintWriter pw = null;
		
		try {
			br = new BufferedReader(new FileReader(yelpBizDataFile));	
			pw = new PrintWriter(new FileWriter(solrJsonFile, false));
			String line;
			boolean isValidRecord = false;
			
			pw.write("[\n");
			if ((line = br.readLine()) != null)  {
				isValidRecord = writeJsonObject(pw, line);
			}
			while ((line = br.readLine()) != null) {
				if (isValidRecord) {
					pw.write(",\n");
				}
				isValidRecord = writeJsonObject(pw, line);
			}
			pw.write("\n]");
		} finally {
			br.close();
			pw.close();
		}
	}
	
	private boolean writeJsonObject(PrintWriter pw, String line) throws IOException {
		boolean retVal;
		
		JsonObject input = JsonObject.readFrom(line);
		JsonObject output = parseOneRecord(input);
		if (output != null) {
			output.writeTo(pw); 
			bizCount++;
			retVal = true;
		} else {
			retVal = false;
		}
		return retVal;
	}
	
	
	// remove business with no zipcode in its address or no category in categories
	private JsonObject parseOneRecord(JsonObject bizInfo) {
		JsonObject retVal = null;
		String addr = bizInfo.get("full_address").toString();
		String zip = addr.substring(addr.length() - 6, addr.length()-1);	// addresses in the data are quoted.  remove the last " char
		try {
			Integer.parseInt(zip);  // test if the zip has only digits
			if (bizInfo.get("categories").toString().equals("[]")) {
				noCategoryCount++;
				throw new Exception(bizInfo.get("name") + " has no category.  This business will not be indexed");
			}
			retVal = new JsonObject();
			retVal.add("business_id", bizInfo.get("business_id"));
			retVal.add("zip", zip);
			retVal.add("name", bizInfo.get("name"));
			retVal.add("city",  bizInfo.get("city"));
			retVal.add("categories", bizInfo.get("categories"));
			retVal.add("review_count", bizInfo.get("review_count"));
			retVal.add("lat_lon", bizInfo.get("latitude").toString() + ','+bizInfo.get("longitude").toString());
			retVal.add("op_schedule",  getBizOpSchedule((JsonObject)bizInfo.get("hours")));
		} catch (NumberFormatException nfe){
			//logger.warn("Address has no zip code. This business will not be indexed. [\"" + bizInfo.get("name") + "\"]" + addr);
			logger.warn("\"" + bizInfo.get("name") + "\"'s Address has no zip code.  This business will not be indexed.  Address:" + addr);
			invalidAddressCount++;
			retVal = null;
		} catch (Exception e) {
			logger.warn(e.getMessage());
			retVal = null;
		}

		return retVal;
	}
	
	
	String getBizOpSchedule(JsonObject jHrs) {
		String opHrsCodedString;
		
		if (jHrs != null && jHrs.size() > 0) {
			opHrsCodedString = BusinessOpHrs.getCodedString(jHrs);
			


		} else {
			noOpHoursCount++;
			// if there is no op hours present
			opHrsCodedString = BusinessOpHrs.getDefaultOpHrsCodedString();
		}
		
		return opHrsCodedString;
	}
	
	public String getSummary() {
		String str = "";

		str += "Total business Record parsed count = " + bizCount + "\n";
		str += "Invalid business Addresse count = " + invalidAddressCount +"\n";
		str += "Number of business with no Category= " + noCategoryCount + "\n";
		str += "Number of business with no Operation Hours= " + noOpHoursCount + "\n";

		return str;
	}
}

	
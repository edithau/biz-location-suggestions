package com.simfolio.ydc;

import java.util.HashMap;
import java.util.Map;

import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonObject.Member;


/*
 * data structure represent a business operating time segment.
 */
public class BusinessOpHrs {
	static String MORNING_OPEN_BEFORE = "10:01";
	static String MORNING_CLOSE_AFTER = "10:59";
	static String AFTERNOON_OPEN_BEFORE = "13:01";
	static String AFTERNOON_CLOSE_AFTER = "13:59";
	static String EVENING_OPEN_BEFORE = "21:01";
	static String EVENING_CLOSE_AFTER = "21:59";
	
	static char[] INIT_CODED_CHARS = new char[21];


    private static final Map<String, Integer> DAY_MAP = new HashMap<String, Integer>();
    private static final Map<String, Integer> TIME_MAP = new HashMap<String, Integer>();
    
    static {

    	DAY_MAP.put("Monday", 0);
    	DAY_MAP.put("Tuesday", 1);
    	DAY_MAP.put("Wednesday", 2);
    	DAY_MAP.put("Thursday", 3);
    	DAY_MAP.put("Friday", 4);
    	DAY_MAP.put("Saturday", 5);
    	DAY_MAP.put("Sunday", 6);
    	
    	TIME_MAP.put("Morning", 0);
    	TIME_MAP.put("Afternoon", 1);
    	TIME_MAP.put("Evening", 2);
    	
    	for (int i=0; i< INIT_CODED_CHARS.length; i++) {
    		INIT_CODED_CHARS[i] = '0';
    	}
    	
    }
    
    
	static String getCodedString(JsonObject jHrs) {
		char[] opHrs = INIT_CODED_CHARS.clone();
		
		for (Member dayOfWeek : jHrs) {
			String open = dayOfWeek.getValue().asObject().get("open").toString().replaceAll("^\"|\"$", "");
			String close = dayOfWeek.getValue().asObject().get("close").toString().replaceAll("^\"|\"$", "");
			
			editOpHrs(dayOfWeek.getName(), open, close, opHrs);
		}
		
		return String.valueOf(opHrs);
	}
	
	
	private static void editOpHrs(String dayOfWeek, String open, String close, char[] opHrs) {
		int dayOffset = DAY_MAP.get(dayOfWeek) * 3;  // 3 = morning, afternoon, evening
		
		if (isOpenMorning(open, close)) {
			opHrs[dayOffset + TIME_MAP.get("Morning")] = '1';
		}
		if (isOpenAfternoon(open, close)) {
			opHrs[dayOffset + TIME_MAP.get("Afternoon")] = '1';
		}
		if (isOpenEvening(open, close)) {
			opHrs[dayOffset + TIME_MAP.get("Evening")] = '1';
		}
	}
	

	// if no operation hours are presented, assume the business opens 24 hours x 7 days
	static String getDefaultOpHrsCodedString() {
		return "000000000000000000000"; //"111111111111111111111";
	}
	
	static boolean isOpenMorning(String open, String close) {
		return 	(open.compareTo(close) >= 0 || open.compareTo(MORNING_OPEN_BEFORE)  < 0 && close.compareTo(MORNING_CLOSE_AFTER) > 0);
	}
	
	static boolean isOpenAfternoon(String open, String close) {
		return 	(open.compareTo(close) >= 0 || open.compareTo(AFTERNOON_OPEN_BEFORE)  < 0 && close.compareTo(AFTERNOON_CLOSE_AFTER) > 0);
	}
	
	static boolean isOpenEvening(String open, String close) {
		return ( open.compareTo(close) >= 0 || open.compareTo(EVENING_OPEN_BEFORE)  < 0 && close.compareTo(EVENING_CLOSE_AFTER) > 0);
	}

}
	

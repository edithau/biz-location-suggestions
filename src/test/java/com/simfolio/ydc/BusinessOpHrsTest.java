package com.simfolio.ydc;

import org.junit.Test;
import static org.junit.Assert.*;

import com.eclipsesource.json.JsonObject;



public class BusinessOpHrsTest {
	
	@Test
	public void testGetCodedString() {
		// mon thru fri, opens 7:30am and closes at 3:30pm; sat opens at 8am and closes at 2:30pm.  close on sun
		String opHrs = 
			"{\"Monday\":{\"close\":\"15:30\",\"open\":\"07:30\"},\"Tuesday\":{\"close\":\"15:30\",\"open\":\"07:30\"}," +
			"\"Friday\":{\"close\":\"15:30\",\"open\":\"07:30\"},\"Wednesday\":{\"close\":\"15:30\",\"open\":\"07:30\"},\"Thursday\":{\"close\":\"15:30\",\"open\":\"07:30\"},\"Saturday\":{\"close\":\"14:30\",\"open\":\"08:00\"}}";
		JsonObject jOpHours = JsonObject.readFrom( opHrs );
		
		String codedOpHrsStr = BusinessOpHrs.getCodedString(jOpHours);

		// encoded string: open mornings and afternoons, mon thru sat.  closes on sun
		assertEquals(codedOpHrsStr, "110110110110110110000");  
	}
	
	@Test
	public void testValues() {
		// make sure there are 21 operating time segments
		assertEquals(BusinessOpHrs.INIT_CODED_CHARS.length, 21);
		
		// if no operating hours provided, assume the business not open
		assertEquals(BusinessOpHrs.getDefaultOpHrsCodedString(), "000000000000000000000");
		
	}


}

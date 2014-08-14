package com.simfolio.ydc;

import org.junit.Test;
import static org.junit.Assert.*;

public class UtilTest {
	
	@Test
	// make sure config.properties is accessible
	public void testAccessConfigFile() {
		assertEquals(8080, Util.getServletPort());
	}

}

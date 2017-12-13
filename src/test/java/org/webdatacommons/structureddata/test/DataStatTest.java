package org.webdatacommons.structureddata.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import org.junit.Test;
import org.webdatacommons.structureddata.util.Statistics;

public class DataStatTest {
	@Test
	public void dataStatTest() throws FileNotFoundException, IOException {
		System.out.println(Statistics.readDataStat(new InputStreamReader(
				new GZIPInputStream(new FileInputStream(new File(
						"/home/hannes/Desktop/ccdata2/stats/data.csv.gz"))))));
	}
	
	
}

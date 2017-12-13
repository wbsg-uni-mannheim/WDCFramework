package org.webdatacommons.structureddata.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import org.junit.Test;
import org.webdatacommons.structureddata.tablegen.TripleStats;
import org.webdatacommons.structureddata.util.Statistics;

import com.amazonaws.util.json.JSONException;

public class TripleStatTest {
	@Test
	public void tripleStatTest() throws JSONException, IOException {
		TripleStats
				.generateStats(
						new InputStreamReader(
								new GZIPInputStream(
										new FileInputStream(
												new File(
														"/home/hannes/Desktop/ccdata/dataf/ccrdf.html-mf-xfn.7.nq.gz")))),
						"foo");
	}

	@Test
	public void domainParserTest() {
		System.out.println(Statistics
				.getDomain("http://gesundheit-und-praevention.blogspot.com/\n\nhttp://hartz4forall.blogspot.com/"));
	}
}

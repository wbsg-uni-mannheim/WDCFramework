package org.webdatacommons.framework.cli;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import com.martiansoftware.jsap.JSAPException;

public class CommonCrawlSegments {

	public static void main (String args[]) throws JSAPException, IOException{
		ArrayList<String> allsegments = getSegments("C:\\Users\\User\\workspace\\ExtractionFramework_WDC\\src\\main\\resources\\segments_2016.txt");
		
		for (String s:allsegments) {
			Master.main(new String[] {"queue", "-pCC-MAIN-2016-44/segments/"+s+"/warc"});
		}
	}
	
	public static ArrayList<String> getSegments(String segmentsFile) throws IOException{
		ArrayList<String> allsegments = new ArrayList<String>();
		
		BufferedReader br = new BufferedReader(new FileReader(segmentsFile));
		 
		String line = null;
		while ((line = br.readLine()) != null) {
			if (!allsegments.contains(line.trim())) allsegments.add(line.trim());
		}
	 
		br.close();
		System.out.println(allsegments.size());
		return allsegments;
	}
}

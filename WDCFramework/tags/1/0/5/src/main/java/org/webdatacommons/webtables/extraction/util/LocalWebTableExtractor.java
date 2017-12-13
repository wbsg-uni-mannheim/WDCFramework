package org.webdatacommons.webtables.extraction.util;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.webdatacommons.webtables.extraction.BasicExtractionAlgorithm;
import org.webdatacommons.webtables.extraction.ExtractionAlgorithm;
import org.webdatacommons.webtables.extraction.model.DocumentMetadata;
import org.webdatacommons.webtables.extraction.stats.HashMapStatsData;
import org.webdatacommons.webtables.tools.data.Dataset;


/**
 * Extracts tables from a local HTML page.
 * 
 * @author Dominique Ritze
 *
 */
public class LocalWebTableExtractor {
	
	public static void main(String args[]) throws IOException, InterruptedException {
		
		ExtractionAlgorithm ea = new BasicExtractionAlgorithm(new HashMapStatsData(), true);
		File inputFile = new File(args[0]);
		File outputFolder = new File(args[1]);
		InputStream in = new BufferedInputStream(new FileInputStream(inputFile));
        Document doc = Jsoup.parse(in, null, "");
        DocumentMetadata dm = new DocumentMetadata(0, 0, "", "", "");
        List<Dataset> result = ea.extract(doc, dm);        
        for (Dataset er : result) {
        	BufferedWriter write = new BufferedWriter(new FileWriter(new File(outputFolder,inputFile.getName() + "_" + er.getTableNum()+".json")));
            write.write(er.toJson());
            write.flush();
            write.close();
        }
        System.out.println("I'm done.");
	}
	
}

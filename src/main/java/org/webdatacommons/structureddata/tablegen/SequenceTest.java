package org.webdatacommons.structureddata.tablegen;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.log4j.Logger;
import org.webdatacommons.structureddata.extractor.RDFExtractor;
import org.webdatacommons.structureddata.tablegen.TripleStats.LoggingFileInputStream;

public class SequenceTest {

	private static Logger log = Logger.getLogger(SequenceTest.class);

	public static void main(String[] args) throws IOException {
		File dataDir = new File("/home/hannes/Desktop/ccdata/dataf/");
		for (String currentExtractor : RDFExtractor.EXTRACTORS) {
			List<InputStream> fileStreams = new ArrayList<InputStream>();
			int i = 0;
			File dataFile;
			while (true) {
				dataFile = new File(dataDir + File.separator
						+ TripleStats.DATA_PREFIX + currentExtractor + "." + i
						+ ".nq.gz");
				if (!dataFile.exists()) {
					break;
				}
				try {
					fileStreams.add(new GZIPInputStream(
							new LoggingFileInputStream(dataFile)));
				} catch (Exception e1) {
					log.warn("I/O failed on " + dataFile, e1);
				}
				i++;
			}
			log.info(currentExtractor + ": " + fileStreams.size() + " files");

			Reader combinedStream = new InputStreamReader(
					new SequenceInputStream(
							Collections.enumeration(fileStreams)));


			StreamingStatsGenerator rc = new StreamingStatsGenerator(
					100);

			try {
				rc.addNquads(combinedStream);
			} catch (Exception e) {
				log.warn(e);
			}
		}
	}
}

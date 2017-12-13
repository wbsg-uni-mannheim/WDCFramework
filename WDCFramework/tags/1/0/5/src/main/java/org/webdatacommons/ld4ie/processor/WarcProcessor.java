package org.webdatacommons.ld4ie.processor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.model.S3Object;
import org.webdatacommons.framework.processor.FileProcessor;
import org.webdatacommons.framework.processor.ProcessingNode;
import org.webdatacommons.structureddata.util.WARCRecordUtils;

/**
 * Uses the {@link org.archive.io.ArchiveReaderFactory} from the UKWA codebase
 * to read entries from WARC file. Reads a lookup file from S3 an extracted all
 * stated URLs from the WARC.
 * 
 * @author Robert Meusel (robert@informatik.uni-mannheim.de)
 */
public class WarcProcessor extends ProcessingNode implements FileProcessor {

	private static Logger log = Logger.getLogger(WarcProcessor.class);

	@Override
	public Map<String, String> process(ReadableByteChannel fileChannel,
			String inputFileKey) throws Exception {

		try {

			// the inputFileKey is something like
			// common-crawl/crawl-data/CC-Main-2014-52/segments/....
			String wdcKey = inputFileKey.replace(
					"common-crawl/crawl-data/CC-MAIN-2014-52/segments/", "");
			wdcKey = wdcKey.replace("/", "_");

			/**
			 * get file from s3 and process with zipped arc.
			 */
			String wdcBucket = getOrCry("wdcLookupBucket");

			S3Object inputObject = null;
			try {
				inputObject = getStorage().getObject(wdcBucket, wdcKey);
			} catch (S3ServiceException e) {
				// if its not there we skip, as we do not need to check for the
				// file
				Map<String, String> dataStats = new HashMap<String, String>();
				// leave empty
				dataStats.put("pagesWritten", String.valueOf(0l));
				return dataStats;
			}

			BufferedReader mappingReader = new BufferedReader(
					new InputStreamReader(new GZIPInputStream(
							inputObject.getDataInputStream())));

			HashMap<String, String> urlLookupMap = new HashMap<String, String>();
			while (mappingReader.ready()) {
				String line = mappingReader.readLine();
				String tok[] = line.split("\t");
				urlLookupMap.put(tok[0], tok[1]);
			}
			mappingReader.close();

			// create a tmp file to write the output for the html pages
			File tempOutputFile = File.createTempFile("dpef-html-extraction",
					".gz");
			tempOutputFile.deleteOnExit();

			// the writer
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
					new GZIPOutputStream(new FileOutputStream(tempOutputFile))));

			// set name for data output
			String outputFileKey = "html/ex_" + inputFileKey.replace("/", "_")
					+ ".htmlfiles.gz";

			// get archive reader
			final ArchiveReader reader = ArchiveReaderFactory.get(inputFileKey,
					Channels.newInputStream(fileChannel), true);

			log.info("Extracting data from " + inputFileKey + " ...");

			// TODO get S3 Lookup file

			Iterator<ArchiveRecord> readerIt = reader.iterator();

			long pagesWritten = 0;

			// read all entries in the ARC file
			while (readerIt.hasNext()) {

				ArchiveRecord record = readerIt.next();
				ArchiveRecordHeader header = record.getHeader();
				ArcFileItem item = new ArcFileItem();

				item.setArcFileName(inputFileKey);

				// WARC contains lots of stuff. We only want HTTP responses
				if (!header.getMimetype().equals(
						"application/http; msgtype=response")) {
					continue;
				}

				// only if its in the map
				String className = urlLookupMap.get(header.getUrl());
				if (className == null) {
					continue;
				}

				String headers[] = WARCRecordUtils.getHeaders(record, true)
						.split("\n");
				if (headers.length < 1) {
					continue;
				}

				// only consider HTML responses
				String contentType = headerKeyValue(headers, "Content-Type",
						"text/html");

				byte[] bytes = IOUtils.toByteArray(WARCRecordUtils
						.getPayload(record));

				if (bytes.length > 0) {

					String htmlString = new String(bytes, "UTF-8");
					// clean it by replacing line breaks
					htmlString = htmlString.replace("\n", " ").replace("\r", " ");

					// write it
					bw.write("URI: " + header.getUrl() + "\n");
					bw.write("Content-Type: " + contentType + "\n");
					bw.write("Content: " + htmlString + "\n");
					bw.write("Class: " + className + "\n");
					bw.write("\n");
					pagesWritten++;
				}

			}

			// we close the stream
			bw.close();

			/**
			 * write extraction results to s3, if at least one included item was
			 * guessed to include triples
			 */

			if (pagesWritten > 0) {
				S3Object dataFileObject = new S3Object(tempOutputFile);
				dataFileObject.setKey(outputFileKey);
				getStorage()
						.putObject(getOrCry("resultBucket"), dataFileObject);
			}

			// create data file statistics and return
			Map<String, String> dataStats = new HashMap<String, String>();
			dataStats.put("pagesWritten", String.valueOf(pagesWritten));

			return dataStats;
		} catch (Exception e) {
			System.out.println(e.getMessage() + " for file: " + inputFileKey);
			e.printStackTrace();
			throw new Exception(e.fillInStackTrace());
		}
	}

	// some Hannes-TM HTTP header parsing kludges, way faster than libs
	public static String headerValue(String[] headers, String key, String dflt) {
		for (String hdrLine : headers) {
			if (hdrLine.toLowerCase().trim().startsWith(key.toLowerCase())) {
				return hdrLine.trim();
			}
		}
		return dflt;
	}

	public static String headerKeyValue(String[] headers, String key,
			String dflt) {
		String line = headerValue(headers, key, null);
		if (line == null)
			return dflt;
		String[] pces = line.split(":");
		if (pces.length != 2)
			return dflt;
		return pces[1].trim();
	}

	/**
	 * Main method to run the ARC extractor from the command line
	 * 
	 * @throws Exception
	 * */
	public static void main(String[] args) throws Exception {
		System.out.println("Usage arcFileName arcFullPathName");
		new WarcProcessor().process(
				Channels.newChannel(new FileInputStream(args[0])), args[1]);

	}

}

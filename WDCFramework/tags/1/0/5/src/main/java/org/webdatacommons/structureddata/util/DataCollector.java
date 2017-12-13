package org.webdatacommons.structureddata.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.webdatacommons.framework.cli.Master;
import org.webdatacommons.framework.cli.Master.Line;
import org.webdatacommons.structureddata.extractor.RDFExtractor;

import de.dwslab.dwslib.framework.Processor;
import de.dwslab.dwslib.util.io.BufferedChunkingWriter;

public class DataCollector extends Processor<String> {

	// the collectin of writers
	private Map<String, BufferedChunkingWriter> outputWriters = new HashMap<String, BufferedChunkingWriter>();
	// the input folder
	private File localDataInputFolder;
	// the output folder
	private File localDataOutputFolder;
	//
	private long parsingErrors = 0;

	private synchronized void increaseErrorCount(long count) {
		parsingErrors += count;
	}

	public DataCollector(String inputFolderName, String outputFolderName,
			int threads) {
		super(threads);
		localDataInputFolder = new File(inputFolderName);
		localDataOutputFolder = new File(outputFolderName);
		if (!localDataInputFolder.isDirectory()
				|| !localDataOutputFolder.isDirectory()) {
			System.out.println("Input or output folder is not a folder.");
			System.exit(0);
		}
		// initialize writer
		for (String extractor : RDFExtractor.EXTRACTORS) {
			outputWriters.put(extractor, new BufferedChunkingWriter(
					localDataOutputFolder, "dpef." + extractor + ".nq", 100));
		}
	}

	@Override
	protected void afterProcess() {
		// closing all writers
		for (String extractor : outputWriters.keySet()) {
			try {
				outputWriters.get(extractor).close();
			} catch (IOException e) {
			}
		}
		System.out.println("Could not parse " + parsingErrors + " lines.");
	}

	@Override
	protected List<String> fillListToProcess() {

		return Arrays.asList(localDataInputFolder.list(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				if (name.endsWith(".nq.gz")) {
					return true;
				} else {
					return false;
				}
			}
		}));
	}

	@Override
	protected void process(String fileName) throws Exception {
		long parsingErrors = 0;
		Map<String, StringBuffer> buffer = new HashMap<String, StringBuffer>();
		// initialize buffer
		for (String extractor : RDFExtractor.EXTRACTORS) {
			buffer.put(extractor, new StringBuffer());
		}
		BufferedReader retrievedDataReader;
		try {
			// retrievedDataReader = InputUtil
			// .getBufferedReader(new File(fileName));
			retrievedDataReader = new BufferedReader(new InputStreamReader(
					new GZIPInputStream(new FileInputStream(new File(
							localDataInputFolder, fileName))), "UTF-8"));
		} catch (Exception e) {
			System.out.println(fileName + " is not a file.");
			throw new Exception(e.getMessage());
		}

		try {
			String line;
			while (retrievedDataReader.ready()) {
				line = retrievedDataReader.readLine();
				Line l = Master.parseLine(line);
				// in case of parsing errors
				if (l == null) {
					parsingErrors++;
					continue;
				}
				if (!RDFExtractor.EXTRACTORS.contains(l.extractor)) {
					System.out.println(l.quad + "/" + l.extractor
							+ " is strange... skipping.");
					continue;
				}
				buffer.get(l.extractor).append(l.quad + "\n");

				// outputWriters.get(l.extractor).write(new String(l.quad +
				// "\n"));
			}
		} catch (IOException io) {

		} finally {
			// write it
			for (String extractor : buffer.keySet()) {
				outputWriters.get(extractor).write(
						buffer.get(extractor).toString());
			}
			increaseErrorCount(parsingErrors);
			retrievedDataReader.close();
		}

	}

}

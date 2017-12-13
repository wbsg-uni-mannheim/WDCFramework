package org.webdatacommons.framework.io;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CSVStatHandler implements StatHandler {
	private File csvFile;

	public CSVStatHandler() throws IOException {

		csvFile = File.createTempFile("stats", "csv.gz");
	}

	@Override
	public void addStats(String key, Map<String, String> data) {
		Map<String, Object> writeMap = new HashMap<String, Object>();
		writeMap.putAll(data);
		CSVExport.writeToFile(writeMap, csvFile);
	}

	@Override
	public void flush() {
		CSVExport.closeWriter(csvFile);
	}

	public File getFile() {
		return csvFile;
	}
}
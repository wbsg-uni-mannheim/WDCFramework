package org.webdatacommons.framework.io;

import java.util.Map;

import org.apache.log4j.Logger;

public class LoggingStatHandler implements StatHandler {
	private static Logger log = Logger.getLogger(LoggingStatHandler.class);

	@Override
	public void addStats(String key, Map<String, String> data) {
		log.debug(key + ": " + data);
	}

	@Override
	public void flush() {
	}

}
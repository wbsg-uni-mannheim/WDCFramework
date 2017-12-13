package org.webdatacommons.framework.io;

import java.util.Map;

/**
 * 
 * @author Hannes Mühleisen
 *
 */
public interface StatHandler {
	public void addStats(String key, Map<String, String> data);

	public void flush();
}

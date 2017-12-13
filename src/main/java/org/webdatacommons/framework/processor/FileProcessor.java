package org.webdatacommons.framework.processor;

import java.nio.channels.ReadableByteChannel;
import java.util.Map;

/**
 * Interface which needs to be implemented to adapt the processor for personal needs.
 * The processor handles one file at the time, and cannot interact with other threads and their processors.
 * 
 * @author Robert Meusel (robert@informatik.uni-mannheim.de)
 *
 */
public interface FileProcessor {

	Map<String, String> process(ReadableByteChannel fileChannel,
			String inputFileKey) throws Exception;

}

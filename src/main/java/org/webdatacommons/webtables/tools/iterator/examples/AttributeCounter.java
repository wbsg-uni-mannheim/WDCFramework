package org.webdatacommons.webtables.tools.iterator.examples;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.webdatacommons.webtables.tools.iterator.FileLinesIterator;

/* counts unique attributes in the corpus from the extracted first lines of all tables as produced 
 * by the class AttributesExtractor */

/**
 * 
 * The code was mainly copied from the DWTC framework 
 * (https://github.com/JulianEberius/dwtc-extractor & https://github.com/JulianEberius/dwtc-tools)
 * 
 * @author Robert Meusel (robert@informatik.uni-mannheim.de) - Translation to DPEF
 *
 */
public class AttributeCounter extends FileLinesIterator {

	static ConcurrentMap<String, AtomicLong> stats = new ConcurrentHashMap<String, AtomicLong>();

	protected void count(String key) {
		AtomicLong value = stats.get(key);
		if (value == null) {
			value = stats.putIfAbsent(key, new AtomicLong(1));
		}
		if (value != null) {
			value.incrementAndGet();
		}
	}

	@Override
	protected void process(String line) throws IOException {
		String[] atts = line.split(",");
		for (String a : atts) {
			count(a);
		}
	}

	public static void main(String[] args) throws IOException,
			InterruptedException {
		if (2 != args.length) {
			System.out
					.println("Usage: java webreduce.tools.AttributeCounter <path-to-corpus> <path-to-output>");
			System.exit(1);
		}
		AttributeCounter cc = new AttributeCounter();
		cc.iterate(args[0]);

		// transform map to list and sort
		PrintWriter writer = new PrintWriter(args[1], "UTF-8");

		System.out.println("Finished iteration, printinge...");
		for (ConcurrentMap.Entry<String, AtomicLong> s : stats.entrySet()) {
			writer.println(s.getKey() + " " + s.getValue());
		}

		writer.close();
	}
}

package org.webdatacommons.webtables.tools.iterator.examples;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.atomic.AtomicLong;

import org.webdatacommons.webtables.tools.iterator.WebreduceIterator;

/**
 * 
 * The code was mainly copied from the DWTC framework 
 * (https://github.com/JulianEberius/dwtc-extractor & https://github.com/JulianEberius/dwtc-tools)
 * 
 * @author Robert Meusel (robert@informatik.uni-mannheim.de) - Translation to DPEF
 *
 */
public class Counter extends WebreduceIterator {

	public static AtomicLong result = new AtomicLong();

	private static final ThreadLocal<AtomicLong> localResult = new ThreadLocal<AtomicLong>() {
		@Override
		protected AtomicLong initialValue() {
			return new AtomicLong();
		}
	};

	@Override
	protected void process(String key, String value) throws IOException {
		localResult.get().incrementAndGet();
	}

	@Override
	protected void finishProcessFile(File f) throws IOException {
		long partialResult = localResult.get().get();
		localResult.set(new AtomicLong());
		result.addAndGet(partialResult);
	}

	public static void main(String[] args) throws IOException,
			InterruptedException {
		if (1 != args.length) {
			System.out
					.println("Usage: java webreduce.tools.Counter <path-to-corpus>");
			System.exit(1);
		}
		Counter cc = new Counter();
		cc.iterate(args[0]);

		// transform map to list and sort
		PrintWriter writer = new PrintWriter("count_result", "UTF-8");

		System.out.println("Finished iteration, printing...");
		writer.println("------------------------");
		writer.println("Count: " + result);
		writer.println("------------------------");
		

		writer.close();
	}
}

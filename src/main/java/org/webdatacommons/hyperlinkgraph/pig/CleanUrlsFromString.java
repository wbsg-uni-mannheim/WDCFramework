package org.webdatacommons.hyperlinkgraph.pig;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class CleanUrlsFromString extends EvalFunc<Tuple> {

	private final static TupleFactory tupleFactory = TupleFactory.getInstance();
	private final static BagFactory bagFactory = BagFactory.getInstance();

	private static Tuple constructEmptyTuple() throws ExecException {
		Tuple tp = tupleFactory.newTuple(2);
		tp.set(0, "NULL");
		tp.set(1, bagFactory.newDefaultBag());
		return tp;
	}

	/**
	 * This functions transform the Input, which is a URL and a Bag of URLs into
	 * a Bag of cleaned Urls. In particular this means all relative URLs are
	 * transformed to absolute URLs as well as internal links (from the same
	 * page to this particular same page) are removed (mostly references by #).
	 */
	@Override
	public Tuple exec(Tuple input) throws IOException {
		try {

			if (input == null || input.size() < 1) {
				return constructEmptyTuple();
			}
			String extractionOutput = (String) input.get(0);
			if (extractionOutput == null) {
				return constructEmptyTuple();
			}
			String[] urls = extractionOutput.split("\t");
			if (urls.length < 1) {
				return constructEmptyTuple();
			}
			String originUrl = urls[0];
			originUrl = originUrl.trim();
			if (originUrl != null && originUrl.startsWith("http")) {
				List<String> refStrings = new ArrayList<String>();
				for (int i = 1; i < urls.length; i++) {
					String ref = urls[i];
					if (ref != null && ref.length() > 0) {
						String urlString = null;
						if (ref.startsWith("http")) {
							urlString = ref;
						} else if (ref.startsWith("#")) {
							// this is an inner page link - we do not need this.
							continue;
						} else {
							// we need to find the real url
							try {
								URL crawledUrl = new URL(originUrl);
								URL url = new URL(crawledUrl, ref);
								urlString = url.toString();
							} catch (MalformedURLException mex) {
								// write something that we have a clash
								// here.
								urlString = null;
							}
						}
						refStrings.add(urlString);
					}
				}

				Tuple ret = tupleFactory.newTuple(2);
				ret.set(0, originUrl);
				DataBag db = new NonSpillableDataBag();
				for (String s : refStrings) {
					Tuple tp = tupleFactory.newTuple(1);
					tp.set(0, s);
					db.add(tp);
				}
				ret.set(1, db);

				return ret;
			} else {
				return constructEmptyTuple();
			}
		} catch (NullPointerException ne) {
			return constructEmptyTuple();
		}
	}
}

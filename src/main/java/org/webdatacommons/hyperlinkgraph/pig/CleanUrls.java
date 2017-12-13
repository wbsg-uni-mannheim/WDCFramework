package org.webdatacommons.hyperlinkgraph.pig;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class CleanUrls extends EvalFunc<DataBag> {

	private final static TupleFactory tupleFactory = TupleFactory.getInstance();
	private final static BagFactory bagFactory = BagFactory.getInstance();

	/**
	 * This functions transform the Input, which is a URL and a Bag of URLs into
	 * a Bag of cleaned Urls. In particular this means all relative URLs are
	 * transformed to absolute URLs as well as internal links (from the same
	 * page to this particular same page) are removed (mostly references by #).
	 */
	@Override
	public DataBag exec(Tuple input) throws IOException {
		if (input == null || input.size() < 2) {
			return bagFactory.newDefaultBag();
		}
		String originUrl = (String) input.get(0);
		DataBag urlBag = (DataBag) input.get(1);
		if (urlBag == null){
			return bagFactory.newDefaultBag();
		}
		Iterator<Tuple> bagIter = urlBag.iterator();
		List<Tuple> refTuples = new ArrayList<Tuple>();
		while (bagIter.hasNext()){
			Tuple refTuple = bagIter.next();
			if (refTuple == null || refTuple.get(0) == null) {
				continue;
			}
			String ref = (String) refTuple.get(0);
			if (ref != null && ref.length() > 0){
				String urlString = null;
				if (ref.startsWith("http")){
					urlString = ref;
				}else if (ref.startsWith("#")){
					// this is an inner page link - we do not need this.
					continue;
				}else{
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
				Tuple tp = tupleFactory.newTuple(1);
				tp.set(0, urlString);
				refTuples.add(tp);
			}
		}	
		return bagFactory.newDefaultBag(refTuples);
	}
	
	@Override
	public Schema outputSchema(Schema arg0) {
		return new Schema(new Schema.FieldSchema(null, DataType.BAG));
	}
	
	
}

package org.webdatacommons.structureddata.test;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.any23.Any23;
import org.apache.any23.extractor.ExtractionException;
import org.apache.any23.extractor.ExtractorFactory;
import org.apache.any23.extractor.ExtractorGroup;
import org.apache.any23.extractor.html.HCardExtractorFactory;
import org.apache.any23.writer.NQuadsWriter;
import org.junit.Test;

public class HCardFailTest {
	@Test
	public void hCardInfiniteLoop() throws FileNotFoundException, IOException,
			ExtractionException {
		List<ExtractorFactory<?>> f = new ArrayList<ExtractorFactory<?>>();
		f.add(new HCardExtractorFactory());

		ExtractorGroup e = new ExtractorGroup(f);
		Any23 a = new Any23(e);

		a.extract(new File("/some/dir/fail.html"), new NQuadsWriter(
				new FileOutputStream("/dev/null")));

		// will never happen
		assertTrue(true);
	}

}

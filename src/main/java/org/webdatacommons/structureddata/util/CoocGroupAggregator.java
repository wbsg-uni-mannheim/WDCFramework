package org.webdatacommons.structureddata.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;
import org.webdatacommons.structureddata.tablegen.SparseMatrix.LongAggregationEntry;
import org.webdatacommons.structureddata.tablegen.SparseMatrix.SparseStringCooccurenceMatrix;

public class CoocGroupAggregator {
	private static Logger log = Logger.getLogger(CoocGroupAggregator.class);

	public static void main(String[] args) throws IOException {
		CoocGroupAggregator c = new CoocGroupAggregator();
		try {
			String file = args[0];
			double minSupport = Double.parseDouble(args[1]);

			c.readFile(new FileInputStream(file));
			c.printGroups(System.out, minSupport);
		} catch (Exception e) {
			log.warn("Usage: "
					+ CoocGroupAggregator.class.getSimpleName()
					+ " [CSV file separated by '"
					+ SEPARATOR
					+ "', e.g. /tmp/file.csv] [Minimum Group Support, e.g. 0.02]");
			log.warn(e);
		}
	}

	private org.webdatacommons.structureddata.tablegen.SparseMatrix.SparseStringCooccurenceMatrix m = new SparseStringCooccurenceMatrix();
	private long transactions = 0;
	public static final String SEPARATOR = "\t";
	public final static int INDEX_A = 0;
	public final static int INDEX_B = 1;
	public final static int INDEX_S = 2;

	public void readFile(InputStream fstream) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
		String strLine;
		while ((strLine = br.readLine()) != null) {
			try {
				String[] fields = strLine.split(SEPARATOR);
				String a = fields[INDEX_A];
				String b = fields[INDEX_B];
				long s = Long.parseLong(fields[INDEX_S]);
				add(a, b, s);
				transactions += s;
			} catch (Exception e) {
				log.debug(e);
				log.debug(strLine);
			}
		}
		br.close();
	}

	private static Collection<String> filterUrls;
	static {
		filterUrls = new ArrayList<String>();
		BufferedReader br = new BufferedReader(new InputStreamReader(
				CoocGroupAggregator.class.getClassLoader().getResourceAsStream(
						"filteredUrls.list")));
		String strLine;
		try {
			while ((strLine = br.readLine()) != null) {
				filterUrls.add(strLine.trim());
			}
		} catch (IOException e) {
			log.warn(e);
		}
	}

	public void add(String a, String b, long s) {
		for (String filterUrl : filterUrls) {
			if (a.equals(filterUrl) || b.equals(filterUrl) || a.equals(b)) {
				return;
			}
		}
		m.put(a, b, s);
	}

	DecimalFormat sixDForm = new DecimalFormat("#.######");

	public void printGroups(OutputStream out, double minSupport)
			throws IOException {
		StringWriter writer = new StringWriter();
		List<Entry<Set<String>, org.webdatacommons.structureddata.tablegen.SparseMatrix.LongAggregationEntry>> res = m
				.getCoocSorted(transactions, minSupport, 2);
		for (Entry<Set<String>, LongAggregationEntry> e : res) {
			writer.write(e.getValue().toString());
			writer.write(SEPARATOR);
			writer.write(sixDForm.format((e.getValue().getSupport() * 1.0)
					/ transactions));
			for (String s : e.getKey()) {
				writer.write(SEPARATOR);
				writer.write(s);
			}
			writer.write("\n");
		}
		out.write(writer.toString().getBytes());
	}
}

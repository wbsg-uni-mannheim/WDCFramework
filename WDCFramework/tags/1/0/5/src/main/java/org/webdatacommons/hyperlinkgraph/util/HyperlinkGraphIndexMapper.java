package org.webdatacommons.hyperlinkgraph.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import de.dwslab.dwslib.framework.Processor;
import de.dwslab.dwslib.util.io.InputUtil;

/**
 * This class replaces a defined set of Strings with their IDs. First the ID
 * file is read into memory. Than a number of files from a folder are parsed using
 * a number of threads, and the Strings are replaced with the corresponding ID.
 * 
 * @author Robert Meusel (robert@informatik.uni-mannheim.de)
 * 
 */
public class HyperlinkGraphIndexMapper extends Processor<File> {

	public HyperlinkGraphIndexMapper(int numberOfProcessors,
			String inputDataFolderName, String indexDataFolderName,
			int expectedCapacity, String suffix, boolean normalize, int offset)
			throws IOException {
		super(numberOfProcessors);
		this.expectedCapacity = expectedCapacity;
		this.suffix = suffix;
		this.norm = normalize;
		this.offset = offset;
		inputDataFolder = new File(inputDataFolderName);
		if (!inputDataFolder.isDirectory()) {
			System.out.println("Input Data Folder is no directory");
			System.exit(0);
		}
		indexDataFolder = new File(indexDataFolderName);
		if (!indexDataFolder.isDirectory()) {
			System.out.println("Index Data Folder is no directory");
			System.exit(0);
		}
		fillIndex();
	}

	private int offset;
	private boolean norm;
	private File inputDataFolder;
	private HashMap<String, Integer> index;
	private int expectedCapacity;
	private File indexDataFolder;
	private String suffix;

	private void fillIndex() throws IOException {
		System.out.println(new Date() + " Starting loading index ...");
		index = new HashMap<String, Integer>(expectedCapacity, 0.99f);
		List<File> indexFiles = new ArrayList<File>(
				Arrays.asList(indexDataFolder.listFiles()));
		Collections.sort(indexFiles);
		int count = offset;
		for (File f : indexFiles) {
			System.out.println(new Date() + " Parsing index file: "
					+ f.getName());
			BufferedReader br = InputUtil.getBufferedReader(f);
			while (br.ready()) {
				index.put(br.readLine().trim(), count++);
				if (count % 1000000 == 0) {
					System.out.println(new Date() + " ... read " + count
							+ " lines.");
				}
			}
		}
		System.out.println(new Date() + " Loaded " + count
				+ " lines into the index.");

	}

	@Override
	protected List<File> fillListToProcess() {
		System.out.println(new Date()
				+ " Filling list of files to be processed.");
		List<File> list = new ArrayList<File>(Arrays.asList(inputDataFolder
				.listFiles()));
		System.out.println(new Date() + " filled list with " + list.size()
				+ " items.");
		return list;
	}

	@Override
	protected void process(File arg0) {

		try {
			BufferedReader br = InputUtil.getBufferedReader(arg0);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
					new GZIPOutputStream(new FileOutputStream(arg0 + suffix
							+ ".gz"))));
			while (br.ready()) {
				String line = br.readLine();
				String[] refs = line.split("\t");
				StringBuffer sb = new StringBuffer();
				// test if its already a number
				Integer id = getId(refs[0].trim());
				if (id != null) {
					sb.append(id);
				} else {
					sb.append(refs[0].trim());
				}

				for (int i = 1; i < refs.length; i++) {
					// if (refs[i] == null || refs[i].length() > 0
					// || refs[i].startsWith("#")) {
					// continue;
					// } else {

					String urlString = refs[i];
					if (norm) {
						try {
							URL crawledUrl = new URL(refs[0]);
							URL url = new URL(crawledUrl, refs[i]);
							urlString = url.toString();
						} catch (MalformedURLException mex) {
							// write something that we have a clash here.
						}
					}
					Integer idd = getId(urlString);
					if (idd != null) {
						urlString = idd.toString();
					}
					if (urlString != null) {
						sb.append("\t");
						sb.append(urlString);
					}
				}
				// }
				sb.append("\n");
				bw.write(sb.toString());
			}
			br.close();
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// gets the id from the
	public Integer getId(String line) {
		// first test ifs its already an id
		Integer id = null;
		try {
			id = Integer.parseInt(line);
		} catch (NumberFormatException ex) {
			// try to get it from the map
			id = index.get(line);
		}
		return id;
	}

	public static void main(String[] args) throws NumberFormatException,
			IOException {
		if (args == null || args.length != 7) {
			System.out
					.println("USAGE: HyperlinkGraphIndexMapper indexDataFolder inputDataFolder capacity threads suffix normalize offset");
			return;
		}
		HyperlinkGraphIndexMapper p = new HyperlinkGraphIndexMapper(
				Integer.parseInt(args[3]), args[1], args[0],
				Integer.parseInt(args[2]), args[4],
				Boolean.parseBoolean(args[5]), Integer.parseInt(args[6]));
		p.process();
	}
}

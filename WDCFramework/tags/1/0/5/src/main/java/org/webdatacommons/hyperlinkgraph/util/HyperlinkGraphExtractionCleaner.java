package org.webdatacommons.hyperlinkgraph.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import de.dwslab.dwslib.framework.Processor;
import de.dwslab.dwslib.util.io.InputUtil;

/**
 * This class cleans the web page links. Internal links (from page to the same
 * page) are removed. Relative links are completed with the corresponding
 * absolute link.
 * 
 * @author Robert Meusel (robert@informatik.uni-mannheim.de)
 * 
 */
public class HyperlinkGraphExtractionCleaner extends Processor<File> {

	public static void main(String[] args) throws FileNotFoundException,
			IOException {
		if (args.length != 3) {
			System.out
					.println("USAGE: HyperlinkGraphExtractionCleaner InputFilesFolder Suffix NumThreads");
			return;
		}
		HyperlinkGraphExtractionCleaner p = new HyperlinkGraphExtractionCleaner(
				Integer.parseInt(args[2]), args[0], args[1]);
		p.process();
	}

	public HyperlinkGraphExtractionCleaner(int threads, String inputFolderName,
			String suffix) {
		super(threads);
		this.suffix = suffix;
		this.inputFolder = new File(inputFolderName);
		if (!inputFolder.isDirectory()) {
			System.out.println("Inputfolder is not a folder.");
			System.exit(0);
		}
	}

	private File inputFolder;
	private String suffix;

	@Override
	protected List<File> fillListToProcess() {
		System.out.println(new Date() + "Filling task list with "
				+ inputFolder.listFiles().length + " files.");
		return new ArrayList<File>(Arrays.asList(inputFolder.listFiles()));
	}

	@Override
	protected void process(File object) {
		try {
			BufferedReader br = InputUtil.getBufferedReader(object);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
					new GZIPOutputStream(new FileOutputStream(object
							.getAbsolutePath() + suffix + ".gz"))));
			long lnCnt = 0;
			try {

				while (br.ready()) {
					String line = br.readLine();
					lnCnt++;
					String[] refs = line.split("\t");
					if (refs != null && refs.length > 0) {
						StringBuffer sb = new StringBuffer();
						// simply append the first url as it is complete
						sb.append(refs[0]);
						for (int i = 1; i < refs.length; i++) {
							// kick a ref out if its simply an internal link to
							// the
							// same page starting wit # or being empty
							if (refs[i].length() < 0 || refs[i].startsWith("#")) {
								continue;
							} else {
								String urlString = null;
								if (refs[i].startsWith("http")) {
									urlString = refs[i];
								} else {
									try {
										URL crawledUrl = new URL(refs[0]);
										URL url = new URL(crawledUrl, refs[i]);
										urlString = url.toString();
									} catch (MalformedURLException mex) {
										// write something that we have a clash
										// here.
									}
								}
								sb.append("\t");
								sb.append(urlString);
							}
						}
						sb.append("\n");
						bw.write(sb.toString());
					}
				}
			} catch (Exception ex) {
				System.out.println(new Date() + "Exception " + ex + " in file "
						+ object.getName() + " at line " + lnCnt);
				ex.printStackTrace(System.out);
			} finally {
				br.close();
				bw.close();
			}
		} catch (IOException iox) {
			System.out.println(iox);
			iox.printStackTrace(System.out);
		}
	}

}

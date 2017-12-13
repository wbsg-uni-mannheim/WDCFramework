package org.webdatacommons.hyperlinkgraph.processor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.webdatacommons.cc.wat.json.WatJsonReader;
import org.webdatacommons.cc.wat.json.model.JsonData;
import org.webdatacommons.cc.wat.json.model.Link;

public class RedirWatProcessor {
	
	public static void main(String[] args) throws IOException {
		File f = new File(args[0]);
		BufferedReader br = new BufferedReader(new FileReader(f));
		BufferedWriter bw = new BufferedWriter(new FileWriter(f.getAbsolutePath() + ".tab"));
		int textTotal = 0;
		int hrefTotal = 0;
		int linksTotal = 0;
		while (br.ready()){
			String line = br.readLine();
			// first part "...", is the origin URL
			int sep = line.indexOf("\",\"");
			String url = line.substring(1, sep);
			String json = line.substring(sep + 3, line.length()-1);
			//remove duplicated "
			json = json.replace("\"\"", "\"");
			JsonData jd = WatJsonReader.read(json);
			// we only want text/html pages
			if (jd.envelope.payLoadMetadata.httpResponseMetadata.headers.contentType == null
					|| !jd.envelope.payLoadMetadata.httpResponseMetadata.headers.contentType
							.startsWith("text/html")) {
				continue;
			}
			textTotal++;
			StringBuilder sb = new StringBuilder();
			sb.append(url);
			// now we go through all links, if there are any
			if (jd.envelope.payLoadMetadata.httpResponseMetadata.htmlMetadata.links != null
					&& jd.envelope.payLoadMetadata.httpResponseMetadata.htmlMetadata.links.length > 0) {
				for (Link link : jd.envelope.payLoadMetadata.httpResponseMetadata.htmlMetadata.links) {
					if (link != null && link.path != null) {
						// only hrefs
						if (link.path.indexOf("href") > -1) {
							sb.append("\t");
							sb.append(link.url);
							hrefTotal++;
						}
						linksTotal++;
					}
				}
				sb.append("\n");
				// write to stream
				bw.write(sb.toString());
			}
		}
		br.close();
		bw.close();
		System.out.println(textTotal + " / " + linksTotal + " / " + hrefTotal);
	}

}

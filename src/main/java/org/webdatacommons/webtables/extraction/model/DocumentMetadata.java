package org.webdatacommons.webtables.extraction.model;

/* Intermediate product of the extraction process */
/**
 * 
 * 
 * The code was mainly copied from the DWT framework 
 * (https://github.com/JulianEberius/dwtc-extractor & https://github.com/JulianEberius/dwtc-tools)
 * 
 * @author Robert Meusel (robert@informatik.uni-mannheim.de) - Translation to DPEF
 *
 */
public class DocumentMetadata {
	private long start;
	private long end;
	private String s3Link;
	private String url;
	private String lastModified;
	
	public DocumentMetadata(long start, long end, String s3Link, String url, String lastModified) {
		super();
		this.start = start;
		this.end = end;
		this.s3Link = s3Link;
		this.url = url;
		this.lastModified = lastModified;
	}
	
	public long getStart() {
		return start;
	}
	public void setStart(long start) {
		this.start = start;
	}
	public long getEnd() {
		return end;
	}
	public void setEnd(long end) {
		this.end = end;
	}
	public String getS3Link() {
		return s3Link;
	}
	public void setS3Link(String s3Link) {
		this.s3Link = s3Link;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getLastModified() {
		return lastModified;
	}
	public void setLastModified(String lastModified) {
		this.lastModified = lastModified;
	}
}

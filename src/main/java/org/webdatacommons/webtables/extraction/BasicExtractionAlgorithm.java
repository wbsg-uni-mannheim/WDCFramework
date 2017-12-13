package org.webdatacommons.webtables.extraction;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringEscapeUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.safety.Whitelist;
import org.jsoup.select.Elements;
import org.webdatacommons.webtables.extraction.detection.HeaderDetection;
import org.webdatacommons.webtables.extraction.detection.KeyColumnDetection;
import org.webdatacommons.webtables.extraction.model.DocumentMetadata;
import org.webdatacommons.webtables.extraction.stats.HashMapStatsData;
import org.webdatacommons.webtables.extraction.stats.StatsKeeper;
import org.webdatacommons.webtables.extraction.stats.TableStats;
import org.webdatacommons.webtables.extraction.util.LuceneNormalizer;
import org.webdatacommons.webtables.extraction.util.TableConvert;
import org.webdatacommons.webtables.tools.data.Dataset;
import org.webdatacommons.webtables.tools.data.HeaderPosition;
import org.webdatacommons.webtables.tools.data.TableOrientation;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;

/*
 * Uses simple heuristics to separate data from layout tables in a given
 * page.
 *
 */
/**
 * 
 * 
 * The code was mainly copied from the DWT framework (https://github.com/JulianEberius/dwtc-tools)
 * 
 * @author Robert Meusel (robert@informatik.uni-mannheim.de) - Translation to DPEF
 *
 */
public class BasicExtractionAlgorithm implements ExtractionAlgorithm {

	protected static final int TABLE_MIN_COLS = 2;
	protected static final double TABLE_MAX_SPARSENESS = 0.49;
	protected static final double TABLE_MAX_LINKS = 0.51;
	protected static final double MIN_ATTRIBUTE_SIZE_AVG = 4.0;
	protected static final double MAX_ATTRIBUTE_SIZE_AVG = 20.0;
	protected static final int TABLE_MIN_ROWS = 3;
	protected static final int CONTEXT_LENGTH_BEFORE_TABLE = 250;
	protected static final int CONTEXT_LENGTH_AFTER_TABLE = 250;

	protected StatsKeeper stats;

	protected static final CharMatcher cleaner = CharMatcher.WHITESPACE;
	protected static final Joiner joiner = Joiner.on(" ").skipNulls();
	protected static final Whitelist whitelist = Whitelist.simpleText();
	protected Pattern tablePattern1 = Pattern
			.compile("<table(.*)(class=\"(.*)\"|id=\"(.*)\")(.*)?>");

	protected boolean th_filter;
	protected boolean th_filter_strong;
	protected boolean th_filter_mark_only;
	protected boolean extract_terms;
	protected boolean extract_content;
	protected boolean extract_part_content;
	protected boolean save_reference;

	protected LuceneNormalizer termExtractor;

	// define the model for classification - phase 1 and phase 2
	TableClassification tc = new TableClassification("/SimpleCart_P1.mdl",
			"/SimpleCart_P2.mdl");

	// object for table class
	TableConvert myTableConvert = new TableConvert(2, 2);

	// table statistic
	TableStats myTableStats;

	public static enum TABLE_COUNTERS {
		TABLE_NOT_PRESENT, TABLES_FOUND, TABLES_INSIDE_FORMS, NON_LEAF_TABLES, SMALL_TABLES, RELATIONS_FOUND, SPARSE_TABLE, LINK_TABLE, CALENDAR_FOUND, NON_REGULAR_TABLES, LANGDETECT_EXCEPTION, ENGLISH, NON_ENGLISH, TO_MANY_BADWORDS, SPANNING_TD, NO_HEADERS, MORE_THAN_ONE_HEADER, SHORT_ATTRIBUTE_NAMES, LONG_ATTRIBUTE_NAMES, NO_HEADERS_CORRECTION,
	}

	public BasicExtractionAlgorithm(StatsKeeper stats,
			boolean th_extract_terms, TableClassification tableClassifier) {
		super();
		if (tableClassifier != null) {
			this.tc = tableClassifier;
		}
		this.stats = stats;
		this.extract_terms = th_extract_terms;
		if (extract_terms)
			this.termExtractor = new LuceneNormalizer();
	}

	public BasicExtractionAlgorithm(StatsKeeper stats, boolean th_extract_terms) {
		this(stats, th_extract_terms, null);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * webreduce.extraction.ExtractionAlgorithm#extract(org.jsoup.nodes.Document
	 * , webreduce.datastructures.DocumentMetadata)
	 */
	@Override
	public List<Dataset> extract(Document doc, DocumentMetadata metadata)
			throws IOException, InterruptedException {
		List<Dataset> result = new ArrayList<Dataset>();
		// Get page content and parse with JSoup
		// String[] tags = null;
		int count = -1;
		// iterate tables tags; find relations
		main_loop: for (Element table : doc.getElementsByTag("table")) {
			// boolean isFiltered = false;
			stats.reportProgress();
			count += 1;
			stats.incCounter(TABLE_COUNTERS.TABLES_FOUND);

			// remove tables inside forms
			for (Element p : table.parents()) {
				if (p.tagName().equals("form")) {
					stats.incCounter(TABLE_COUNTERS.TABLES_INSIDE_FORMS);
					continue main_loop;
				}
			}

			// remove table with sub-tables
			Elements subTables = table.getElementsByTag("table");
			subTables.remove(table);
			if (subTables.size() > 0) {
				stats.incCounter(TABLE_COUNTERS.NON_LEAF_TABLES);
				continue;
			}

			// remove tables with less than N rows
			Elements trs = table.getElementsByTag("tr");
			if (trs.size() < TABLE_MIN_ROWS) {
				stats.incCounter(TABLE_COUNTERS.SMALL_TABLES);
				continue;
			}
			// remove tables with less than M columns
			int maxtdCount = 0;
			int[] tdCounts = new int[trs.size()];
			Multiset<Integer> colCounts = HashMultiset.create();
			for (int tr_idx = 0; tr_idx < trs.size(); tr_idx++) {
				Elements tds = trs.get(tr_idx).select("td, th");
				int td_size = tds.size();
				tdCounts[tr_idx] = td_size;
				colCounts.add(td_size);
				if (td_size > maxtdCount)
					maxtdCount = td_size;
			}
			// find most common number of columns throughout all rows
			colCounts = Multisets.copyHighestCountFirst(colCounts);
			int mostFrequentColCount = colCounts.entrySet().iterator().next()
					.getElement();
			if (mostFrequentColCount < TABLE_MIN_COLS) {
				stats.incCounter(TABLE_COUNTERS.SMALL_TABLES);
				continue;
			}
			// remove non-regular tables (there is a row with more columns
			// than the most common number)
			if (maxtdCount != mostFrequentColCount) {
				stats.incCounter(TABLE_COUNTERS.NON_REGULAR_TABLES);
				continue;
			}

			// eliminate tables with "rowspan" or "colspan" for now
			Elements colSpans = table.select("td[colspan], th[colspan]");
			Elements rowSpans = table.select("td[rowspan], th[rowspan]");
			if (colSpans.size() > 0 || rowSpans.size() > 0) {
				stats.incCounter(TABLE_COUNTERS.SPANNING_TD);
				continue;
			}

			// there should be header cells
			Boolean has_header = true;
			Elements headerCells = table.select("th");
			if (headerCells.size() == 0) {
				stats.incCounter(TABLE_COUNTERS.NO_HEADERS);
				has_header = false;
			}
			// stats.reportProgress();
			Optional<Dataset> r = doExtract(table, trs, mostFrequentColCount);
			if (!r.isPresent()) {
				stats.incCounter(TABLE_COUNTERS.TABLE_NOT_PRESENT);
				continue;
			}
			Dataset er = r.get();

			// average length of first row // only if header is present
			// ROBME We excluded this, as the header detection sucks (before
			// this point)
			// String[] firstRow = er.getAttributes();
			// double firstRowLengthSum = 0.0;
			// for (int i = 0; i < firstRow.length; i++) {
			// firstRowLengthSum += firstRow[i].length();
			// }
			// if ((firstRowLengthSum / firstRow.length) <
			// MIN_ATTRIBUTE_SIZE_AVG) {
			// stats.incCounter(TABLE_COUNTERS.SHORT_ATTRIBUTE_NAMES);
			// continue;
			// }
			// if ((firstRowLengthSum / firstRow.length) >
			// MAX_ATTRIBUTE_SIZE_AVG) {
			// stats.incCounter(TABLE_COUNTERS.LONG_ATTRIBUTE_NAMES);
			// continue;
			// }

			er.tableNum = count;
			er.s3Link = metadata.getS3Link();
			er.recordOffset = metadata.getStart();
			er.recordEndOffset = metadata.getEnd();
			er.url = metadata.getUrl();
			er.lastModified = metadata.getLastModified();

			// if (tags == null && extract_terms) {
			// String bodyContent = doc.select("body").text();
			// Set<String> tagSet = termExtractor.topNTerms(bodyContent, 100);
			// tags = tagSet.toArray(new String[] {});
			// Arrays.sort(tags);
			// }
			// er.termSet = tags;

			er.hasHeader = has_header;
			Elements caption = table.select("caption");
			if (caption.size() == 1)
				er.setTitle(cleanCell(caption.get(0).text()));
			er.setPageTitle(doc.title());
			stats.incCounter(TABLE_COUNTERS.RELATIONS_FOUND);

			// classify the table layout or content
			er.setTableType(tc.classifyTable(table).getTableType());

			// table orientation
			Optional<Element[][]> convertedTable = myTableConvert
					.toTable(table);
			Element[][] twodArrayTable = convertedTable.get();
			String headerPos = er.getHeaderPosition().toString();
			String tableType = er.getTableType().toString();
			er.setTableOrientation(tableOrientation(table, headerPos,
					tableType, twodArrayTable));

			// text before and after table
			// Elements paragraphsFromTBandTAMethod =
			// TextBeforeandTextAfterTable(
			// table, doc);
			String body = doc.select("body").toString();
			// HashMap<Integer, String> textBeforeTable = new HashMap<Integer,
			// String>();
			// HashMap<Integer, String> textAfterTable = new HashMap<Integer,
			// String>();

			// String tablePattern =
			// "<table(.*)(class=\""+table.className().trim()+"\"|id=\""+table.id().trim()+"\")(.*)?>";

			int tableIndex = 0;

			Matcher tableMatcher1 = tablePattern1.matcher(body);
			while (tableMatcher1.find()) {
				if (tableMatcher1.group().length() != 0) {
					if (tableMatcher1.group()
							.contains(table.className().trim())
							|| tableMatcher1.group()
									.contains(table.id().trim())) {
						tableIndex = tableMatcher1.start();
						break;
					}
				}
			}
			int tableLineIndex = 0;
			String[] bodysplit = body.split("\n");

			HERE: for (int i = 0; i < bodysplit.length; i++) {
				if (bodysplit[i] != null) {

					if (bodysplit[i].trim().contains(
							table.toString().split("\n")[0].trim())) {
						tableLineIndex = i;
						break HERE;
					}
				}
			}

			int startIndexofTable = tableLineIndex - 1;
			int lastIndexofTable = table.toString().split("\n").length
					+ tableLineIndex - 1;

			List<String> wordsBeforeTable = new ArrayList<String>();
			List<String> wordsAfterTable = new ArrayList<String>();
			int wordCount = 0;

			ALL_LOOP: for (int i = startIndexofTable; i > 0; i--) {
				// bodysplit[i] = Jsoup.parse(bodysplit[i]).text();
				// bodysplit[i] = bodysplit[i]
				// .trim()
				// .replaceAll(
				// "</?\\w+((\\s+\\w+(\\s*=\\s*(?:\".*?\"|'.*?'|[^'\">\\s]+))?)+\\s*|\\s*)/?>",
				// "");
				String word[] = bodysplit[i].trim().split(" ");
				for (int j = 0; j < word.length; j++) {
					if (word[j] != null && !word[j].isEmpty()
							&& !word[j].equals(" ")) {
						wordsBeforeTable.add(word[j].trim());
						wordCount++;
						if (wordCount >= CONTEXT_LENGTH_BEFORE_TABLE)
							break ALL_LOOP;
					}
				}
			}

			wordCount = 0;
			ALL_LOOP: for (int i = lastIndexofTable; i < bodysplit.length; i++) {
				// bodysplit[i] = bodysplit[i]
				// .trim()
				// .replaceAll(
				// "</?\\w+((\\s+\\w+(\\s*=\\s*(?:\".*?\"|'.*?'|[^'\">\\s]+))?)+\\s*|\\s*)/?>",
				// "");
				// bodysplit[i] = Jsoup.parse(bodysplit[i]).text();
				String word[] = bodysplit[i].trim().split(" ");
				for (int j = 0; j < word.length; j++) {
					if (word[j] != null && !word[j].isEmpty()
							&& !word[j].equals(" ")) {
						wordsAfterTable.add(word[j].trim());
						wordCount++;
						if (wordCount >= CONTEXT_LENGTH_AFTER_TABLE)
							break ALL_LOOP;
					}
				}
			}
			for (String s : wordsBeforeTable) {
				er.textBeforeTable += s + " ";
			}
			er.textBeforeTable = Jsoup.parse(er.textBeforeTable).text();
			for (String s2 : wordsAfterTable) {
				er.textAfterTable += s2 + " ";
			}
			er.textAfterTable = Jsoup.parse(er.textAfterTable).text();

			// er.setTableIndex(tableIndex);

			// table context containing time stamp
			// String bodyContent = doc.select("body").toString();
			Elements paragraphsFromDoc = doc.getElementsByTag("p");
			HashMap<Integer, String> beforeTable = new HashMap<Integer, String>();
			HashMap<Integer, String> afterTable = new HashMap<Integer, String>();

			if (!paragraphsFromDoc.isEmpty()
					&& !TableContextContaningTimeStamp(
							doc.getElementsByTag("p")).isEmpty()) {
				for (Element paragraph : TableContextContaningTimeStamp(doc
						.getElementsByTag("p"))) {
					int paraIndex = body.indexOf(paragraph.toString());
					if (tableIndex - paraIndex < 0) {
						afterTable.put((tableIndex - paraIndex) * (-1),
								paragraph.text());
					} else {
						beforeTable.put(tableIndex - paraIndex,
								paragraph.text());
					}
				}

				// for(String myHashSet :
				// TableContextContaningTimeStamp(doc.getElementsByTag("p"))){
				//
				// myHashMap.put(bodyContent.indexOf(myHashSet), myHashSet);
				//
				// }
				if (afterTable.isEmpty() && !beforeTable.isEmpty()) {
					er.setTableContextTimeStampAfterTable(null);
					er.setTableContextTimeStampBeforeTable(beforeTable
							.toString());
				} else if (beforeTable.isEmpty() && !afterTable.isEmpty()) {
					er.setTableContextTimeStampBeforeTable(null);
					er.setTableContextTimeStampAfterTable(afterTable.toString());
				} else {
					er.setTableContextTimeStampBeforeTable(beforeTable
							.toString());
					er.setTableContextTimeStampAfterTable(afterTable.toString());
				}

				afterTable.clear();
				beforeTable.clear();
			} else {
				if (paragraphsFromDoc.isEmpty()) {
					er.setTableContextTimeStampBeforeTable(null);
					er.setTableContextTimeStampAfterTable(null);
				} else {
					er.setTableContextTimeStampBeforeTable(null);
					er.setTableContextTimeStampAfterTable(null);
				}
			}

			// for key column detection and header detection
			// if table orientation is VERTICAL change it to HORIZONTAL by
			// taking transpose of table array
			if (er.getTableOrientation() == TableOrientation.VERTICAL)
				twodArrayTable = myTableConvert
						.transpose(twodArrayTable, table);

			myTableStats = new TableStats(twodArrayTable[1].length,
					twodArrayTable.length, twodArrayTable);

			// key column detection
			KeyColumnDetection myKeyColumnDetection1 = new KeyColumnDetection(
					myTableStats);
			KeyColumnDetection myKeyColumnDetection2 = myKeyColumnDetection1
					.keyColumnDetection();
			if (myKeyColumnDetection2.isHasKeyColumn()) {
				er.setHasKeyColumn(true);
				// er.setHeaderPosition(HeaderPosition.FIRST_ROW);
				er.setKeyColumnIndex(myKeyColumnDetection2.getKeyColumnIndex());
			} else {
				er.setHasKeyColumn(false);
				er.setKeyColumnIndex(myKeyColumnDetection2.getKeyColumnIndex());
			}

			// header detection
			if (!er.hasHeader) {
				HeaderDetection myHeaderDetection1 = new HeaderDetection(
						myTableStats);
				HeaderDetection myHeaderDetection2 = myHeaderDetection1
						.HeaderDetectionBasedOnCellContentPattern();
				if (myHeaderDetection2.isHasHeader()) {
					er.setHasHeader(true);
					er.setHeaderPosition(HeaderPosition.FIRST_ROW);
					er.setHeaderRowIndex(myHeaderDetection2.getRowIndex());
					stats.incCounter(TABLE_COUNTERS.NO_HEADERS_CORRECTION);
				} else {
					er.setHasHeader(false);
					er.setHeaderPosition(HeaderPosition.NONE);
					er.setHeaderRowIndex(myHeaderDetection2.getRowIndex());
				}
			}

			result.add(er);
		}
		return result;
	}

	// recursive method to find proper parent of table i.e. div tag in which
	// table is present
	protected Element findProperParentTag(Element e) {
		List<String> tagNameList = new ArrayList<String>();
		tagNameList.add("tbody");
		tagNameList.add("tr");
		tagNameList.add("td");
		tagNameList.add("table");

		String tagNameofParent = e.parent().tagName();
		Element parent = e.parent();

		if (tagNameList.contains(tagNameofParent)) {
			parent = this.findProperParentTag(parent);
		}

		return parent;

	}

	// protected Elements TextBeforeandTextAfterTable(Element table, Document
	// doc) {
	//
	// Element elementParent = this.findProperParentTag(table);
	//
	// Elements paragraphs = elementParent.select("p");
	//
	// return paragraphs;
	// }

	protected HashSet<Element> TableContextContaningTimeStamp(
			Elements paragraphs) {

		HashSet<Element> paragraph = new HashSet<Element>();
		String datePattern = "[0-3]?[0-9][\\-|\\|\\.][0-3]?[0-9][\\-|\\|\\.](?:[0-9]{2})?[0-9]{2}(?=(\\s|\\.|\n|$|\\,))";
		String monthPattert = "([J|j]an(?:uary)?|[F|f]eb(?:ruary)?|[M|m]ar(?:ch)?|[A|a]pr(?:il)?|May|[J|j]un(?:e)?|[J|j]ul(?:y)?|[A|a]ug(?:ust)?|[S|s]ep(?:tember)?|[O|o]ct(?:ober)?|[N|n]ov(?:ember)?|[D|d]ec(?:ember)?)(?=(\\s|\\.|\n|$|\\,))";
		String twelveHourClock = "(1[0-2]|0?[1-9]):([0-5]?[0-9])(\\s)?([AP]M)?(?=(\\s|\n|\\.|$|\\,))";
		String twentyfourHourClock = "(2[0-3]|[01]?[0-9]):([0-5]?[0-9])(?=(\\s|\n|\\.|$|\\,))";
		String twentyfourHourClockWithSec = "(2[0-3]|[01]?[0-9]):([0-5]?[0-9]):([0-5]?[0-9])(?=(\\s|\n|\\.|$|\\,))";
		String twelveHourClockWithSec = "(1[0-2]|0	?[1-9]):([0-5]?[0-9]):([0-5]?[0-9])(\\s)?([AP]M)?(?=(\\s|\n|\\.|$|\\,))";
		String onlyYear = "(([1-2][0-9]{3})(\\s?(BC|AC|B.C.|A.C.)?)|([0-9]{3})\\s?(BC|AC|B.C.|A.C.)|([0-9]{2})\\s?(BC|AC|B.C.|A.C.)|([0-9]{1})\\s?(BC|AC|B.C.|A.C.))(?=(\\s|\n|\\.|$|\\,|-|\\/))";
		String hourClock = "(1[0-2]|0?[1-9]):([0-5]?[0-9])(\\s)?(H|h)?(?=(\\s|\n|\\.|$|\\,))";
		String oClock = "(([1]?[0-9]|[2]?[0-4])\\s?([O|o][\'|`][C|c]lock))(?=(\\s|\n|\\.|$|\\,))";

		Pattern myPattern;
		Matcher myMatcher;

		for (Element para : paragraphs) {
			String paraString = para.text();

			myPattern = Pattern.compile(datePattern);
			myMatcher = myPattern.matcher(paraString);

			while (myMatcher.find()) {
				if (myMatcher.group().length() != 0) {
					paragraph.add(para);
				}
			}

			myPattern = Pattern.compile(monthPattert);
			myMatcher = myPattern.matcher(paraString);

			while (myMatcher.find()) {
				if (myMatcher.group().length() != 0) {
					paragraph.add(para);
				}
			}

			myPattern = Pattern.compile(twelveHourClock);
			myMatcher = myPattern.matcher(paraString);

			while (myMatcher.find()) {
				if (myMatcher.group().length() != 0) {
					paragraph.add(para);
				}
			}

			myPattern = Pattern.compile(twelveHourClockWithSec);
			myMatcher = myPattern.matcher(paraString);

			while (myMatcher.find()) {
				if (myMatcher.group().length() != 0) {
					paragraph.add(para);
				}
			}

			myPattern = Pattern.compile(onlyYear);
			myMatcher = myPattern.matcher(paraString);

			while (myMatcher.find()) {
				if (myMatcher.group().length() != 0) {
					paragraph.add(para);
				}
			}

			myPattern = Pattern.compile(twentyfourHourClock);
			myMatcher = myPattern.matcher(paraString);

			while (myMatcher.find()) {
				if (myMatcher.group().length() != 0) {
					paragraph.add(para);
				}
			}

			myPattern = Pattern.compile(twentyfourHourClockWithSec);
			myMatcher = myPattern.matcher(paraString);

			while (myMatcher.find()) {
				if (myMatcher.group().length() != 0) {
					paragraph.add(para);
				}
			}

			myPattern = Pattern.compile(hourClock);
			myMatcher = myPattern.matcher(paraString);

			while (myMatcher.find()) {
				if (myMatcher.group().length() != 0) {
					paragraph.add(para);
				}
			}

			myPattern = Pattern.compile(oClock);
			myMatcher = myPattern.matcher(paraString);

			while (myMatcher.find()) {
				if (myMatcher.group().length() != 0) {
					paragraph.add(para);
				}
			}
		}

		return paragraph;

	}

	protected Optional<Dataset> doExtract(Element table, Elements trs,
			int mostFrequentColCount) {
		// remove sparse tables (more than X% null cells)
		int tableSize = trs.size() * mostFrequentColCount;
		Optional<Dataset> r = asRelation(trs, mostFrequentColCount,
				((int) (TABLE_MAX_SPARSENESS * tableSize)),
				((int) (TABLE_MAX_LINKS * tableSize)));

		return r;
	}

	protected Optional<Dataset> asRelation(Elements input, int numCols,
			int nullLimit, int linkLimit) {
		int nullCounter = 0;
		int linkCounter = 0;
		int numRows = input.size();
		String[][] relation = new String[numCols][numRows];
		for (int r = 0; r < numRows; r++) {
			int c;
			Elements cells = input.get(r).select("td, th");
			int td_size = cells.size();
			for (c = 0; c < td_size; c++) {
				Element cell = cells.get(c);
				Elements links = cell.select("a");
				if (links.size() > 0)
					linkCounter += 1;
				String cellStr = cleanCell(cell.text());
				if (cellStr.length() == 0) {
					nullCounter += 1;
				}
				relation[c][r] = cellStr;
			}
			for (int c_fill = c; c_fill < numCols; c_fill++) {
				relation[c_fill][r] = ""; // just fill with empty string for now
			}
			if (nullCounter > nullLimit) {
				stats.incCounter(TABLE_COUNTERS.SPARSE_TABLE);
				return Optional.absent();
			}
			if (linkCounter > linkLimit) {
				stats.incCounter(TABLE_COUNTERS.LINK_TABLE);
				return Optional.absent();
			}
		}
		Dataset result = new Dataset();
		result.relation = relation;
		result.headerPosition = headerPosition(input);
		// result.tableOrientation = tableOrientation(input);
		return Optional.of(result);
	}

	protected TableOrientation tableOrientation(Element table,
			String headerPositon, String tableType, Element[][] twodArrayTable) {

		// calculate standard deviation for rows
		double[] arrayOfCellLengthByRow;
		double[] standardDeviationofAllRows;
		if (twodArrayTable.length > 10 && twodArrayTable[1].length > 10) {
			arrayOfCellLengthByRow = new double[10];
			standardDeviationofAllRows = new double[10];
		} else if (twodArrayTable.length > 10 && twodArrayTable[1].length <= 10) {
			arrayOfCellLengthByRow = new double[twodArrayTable[1].length];
			standardDeviationofAllRows = new double[10];
		} else if (twodArrayTable.length <= 10 && twodArrayTable[1].length > 10) {
			arrayOfCellLengthByRow = new double[10];
			standardDeviationofAllRows = new double[twodArrayTable.length];
		} else {
			arrayOfCellLengthByRow = new double[twodArrayTable[1].length];
			standardDeviationofAllRows = new double[twodArrayTable.length];
		}

		for (int rowIndex = 0; rowIndex < standardDeviationofAllRows.length; rowIndex++) {
			for (int colIndex = 0; colIndex < arrayOfCellLengthByRow.length; colIndex++) {
				if (twodArrayTable[rowIndex][colIndex] == null) {
					arrayOfCellLengthByRow[colIndex] = 0;
				} else {
					arrayOfCellLengthByRow[colIndex] = twodArrayTable[rowIndex][colIndex]
							.text().length();
				}

			}
			standardDeviationofAllRows[rowIndex] = this
					.StandardDeviation(arrayOfCellLengthByRow);
		}

		double standardDeviationSummationRows = 0;
		for (int count = 0; count < standardDeviationofAllRows.length; count++) {
			standardDeviationSummationRows = standardDeviationSummationRows
					+ standardDeviationofAllRows[count];
		}
		double avgStandardDeviationRows = standardDeviationSummationRows
				/ standardDeviationofAllRows.length;

		// calculate standard deviation for cols
		double[] arrayOfCellLengthByCol;
		double[] standardDeviationofAllCols;
		if (twodArrayTable.length > 10 && twodArrayTable[1].length > 10) {
			arrayOfCellLengthByCol = new double[10];
			standardDeviationofAllCols = new double[10];
		} else if (twodArrayTable.length > 10 && twodArrayTable[1].length <= 10) {
			arrayOfCellLengthByCol = new double[10];
			standardDeviationofAllCols = new double[twodArrayTable[1].length];
		} else if (twodArrayTable.length <= 10 && twodArrayTable[1].length > 10) {
			arrayOfCellLengthByCol = new double[twodArrayTable.length];
			standardDeviationofAllCols = new double[10];
		} else {
			arrayOfCellLengthByCol = new double[twodArrayTable.length];
			standardDeviationofAllCols = new double[twodArrayTable[1].length];
		}

		for (int colIndex = 0; colIndex < standardDeviationofAllCols.length; colIndex++) {
			for (int rowIndex = 0; rowIndex < arrayOfCellLengthByCol.length; rowIndex++) {
				if (twodArrayTable[rowIndex][colIndex] == null) {
					arrayOfCellLengthByCol[rowIndex] = 0;
				} else {
					arrayOfCellLengthByCol[rowIndex] = twodArrayTable[rowIndex][colIndex]
							.text().length();
				}
			}
			standardDeviationofAllCols[colIndex] = this
					.StandardDeviation(arrayOfCellLengthByCol);
		}

		double standardDeviationSummationCols = 0;
		for (int count = 0; count < standardDeviationofAllCols.length; count++) {
			standardDeviationSummationCols = standardDeviationSummationCols
					+ standardDeviationofAllCols[count];
		}
		double avgStandardDeviationCols = standardDeviationSummationCols
				/ standardDeviationofAllCols.length;

		TableOrientation result = null;
		if (headerPositon.equals("FIRST_COLUMN"))
			result = TableOrientation.VERTICAL;
		else if (headerPositon.equals("FIRST_ROW"))
			result = TableOrientation.HORIZONTAL;
		else if (tableType.equals("MATRIX"))
			result = TableOrientation.MIXED;
		else {
			if (avgStandardDeviationCols <= avgStandardDeviationRows)
				result = TableOrientation.HORIZONTAL;
			else
				result = TableOrientation.VERTICAL;
		}

		return result;

	}

	protected double StandardDeviation(double[] array) {
		double sum = 0;

		// Taking the average to numbers
		for (int i = 0; i < array.length; i++) {
			sum = sum + array[i];
		}

		double mean = sum / array.length;

		double[] deviations = new double[array.length];

		// Taking the deviation of mean from each numbers and getting the
		// squares of deviations
		for (int i = 0; i < deviations.length; i++) {
			deviations[i] = Math.pow((array[i] - mean), 2);
		}

		sum = 0;

		// adding all the squares
		for (int i = 0; i < deviations.length; i++) {
			sum = sum + deviations[i];
		}
		double result = sum / (array.length - 1);

		// Taking square root of result gives the standard deviation
		double standardDeviation = Math.sqrt(result);

		return standardDeviation;

	}

	protected HeaderPosition headerPosition(Elements input) {
		// header in firstRow
		boolean fr = true;
		// header in firstCol
		boolean fc = true;

		Elements firstRow = input.get(0).children();
		int rowLength = firstRow.size();
		for (int i = 1; i < rowLength; i++) {
			if (!firstRow.get(i).tag().getName().equals("th")) {
				fr = false;
				break;
			}

		}

		int numRows = input.size();
		for (int i = 1; i < numRows; i++) {
			Elements tds = input.get(i).children();
			// no cells or first cell is not th
			if (tds.size() == 0 || !tds.get(0).tag().getName().equals("th")) {
				fc = false;
				break;
			}

		}

		HeaderPosition result = null;
		if (fr && fc)
			result = HeaderPosition.MIXED;
		else if (fr)
			result = HeaderPosition.FIRST_ROW;
		else if (fc)
			result = HeaderPosition.FIRST_COLUMN;
		else
			result = HeaderPosition.NONE;

		return result;
	}

	protected static String cleanCell(String cell) {
		cell = Jsoup.clean(cell, whitelist);
		cell = StringEscapeUtils.unescapeHtml4(cell);
		cell = cleaner.trimAndCollapseFrom(cell, ' ');
		return cell;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see webreduce.extraction.ExtractionAlgorithm#getStatsKeeper()
	 */
	@Override
	public StatsKeeper getStatsKeeper() {
		return stats;
	}

	private static int NUM_RUNS = 50;

	public static void main(String[] args) throws MalformedURLException,
			IOException, InterruptedException {
		ExtractionAlgorithm ea = new BasicExtractionAlgorithm(
				new HashMapStatsData(), true, null);

		for (String url : new String[] { "https://en.wikipedia.org/wiki/List_of_countries_by_population",
		// "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)",
		// "https://en.wikipedia.org/wiki/BRIC"
		}) {
			InputStream in = new URL(url).openStream();

			long startTime = System.nanoTime();
			for (int i = 0; i < NUM_RUNS; i++) {
				Document doc = Jsoup.parse(in, null, "");
				DocumentMetadata dm = new DocumentMetadata(0, 0, "", "", "");
				List<Dataset> result = ea.extract(doc, dm);

				for (Dataset er : result) {
					System.out.println(Arrays.deepToString(er.relation));
					System.out.println("Has Header: " + er.getHasHeader());
					System.out.println("Header Index: "
							+ er.getHeaderRowIndex());
					System.out.println("Header Position: "
							+ er.getHeaderPosition());
					System.out.println("Table Orientation: "
							+ er.getTableOrientation());
					System.out
							.println("Table Context Before Table(Time Stamp): "
									+ er.getTableContextTimeStampBeforeTable());
					System.out
							.println("Table Context After Table(Time Stamp): "
									+ er.getTableContextTimeStampAfterTable());
					System.out.println("Table Context Before Table: "
							+ er.getTextBeforeTable());
					System.out.println("Table Context After Table: "
							+ er.getTextAfterTable());
					System.out.println("Has Key Column: "
							+ er.getHasKeyColumn());
					System.out.println("Key Column Index: "
							+ er.getKeyColumnIndex());
					System.out.println("Table Type: " + er.getTableType());
					System.out.println("Page Title: " + er.getPageTitle());
					System.out.println("Page URL: " + er.getUrl());
					System.out.println("Table Number: " + er.getTableNum());
					System.out.println("Table Caption: " + er.getTitle());
					System.out.println("");
					System.out
							.println("-----------------------------------------------------------------------------------------");
					System.out.println("");

				}

			}
			long endTime = System.nanoTime();
			System.out.println("Time: "
					+ (((float) (endTime - startTime)) / NUM_RUNS) / 1000000);
			// System.out.println(ea.stats.statsAsMap().toString());

		}

	}

}

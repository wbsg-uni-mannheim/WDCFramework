package org.webdatacommons.webtables.extraction.detection;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.webdatacommons.webtables.extraction.stats.TableStats;

//header detection for horizontal tables only. if there is vertical table convert it to horizontal and then apply this heuristic. otherwise results will be miss leading
/**
 * 
 * 
 * The code was mainly copied from the DWT framework 
 * (https://github.com/JulianEberius/dwtc-extractor & https://github.com/JulianEberius/dwtc-tools)
 * 
 * @author Robert Meusel (robert@informatik.uni-mannheim.de) - Translation to DPEF
 *
 */
public class HeaderDetection {
	
	private TableStats myTableStats;
	public HeaderDetection(TableStats myTableStats){
		this.myTableStats = myTableStats;
	}
	
	private int headerRowIndex;
	public int getRowIndex() {
		return headerRowIndex;
	}

	private boolean hasHeader;
	public boolean isHasHeader() {
		return hasHeader;
	}

	public HeaderDetection(int headerRowIndex, boolean hasHeader) {
		this.headerRowIndex = headerRowIndex;
		this.hasHeader = hasHeader;
	}
	

//	based on first row datatype
	public HeaderDetection HeaderDetectionBasedOnDatatype() {
		Map<Integer, String[]> myRowMap = myTableStats.getRows(5);		
		boolean[] isString = new boolean[myRowMap.size()];
		for (int i = 0;i < myRowMap.size(); i++) {
			isString[i] = myTableStats.isStringOnly(myRowMap.get(i)); 
		}	
		
		for(int i = 1; i < isString.length; i++){
			if(isString[0] && isString[i]){
			return new HeaderDetection(-1,  false);
			}
			else
				continue;
		}
		
		return new HeaderDetection(0, true);
		
	} 
	
//	based on content pattern of cell
	public HeaderDetection HeaderDetectionBasedOnCellContentPattern() {
		Map<Integer, String[]> myRowMap = myTableStats.getRows(5);
		int flag = 0;
		String[] firstRow = myRowMap.get(0);
		String[] secondRow = myRowMap.get(1);
		
		for (int i = 0; i < firstRow.length; i++) {
//			String p1 = myTableStats.extractPatternFromCell(firstRow[i]);
//			String p2 = myTableStats.extractPatternFromCell(secondRow[i]);
			if(firstRow[i].length() <= 10){
				if(myTableStats.extractPatternFromCell(firstRow[i]).equals(myTableStats.extractPatternFromCell(secondRow[i]))) {
					flag ++;
				}
			}
			else
				continue;
		}
		
		if(flag < myRowMap.get(1).length - 1){
			flag = 0;
			Pattern emailpattern = Pattern.compile("^.+@.+\\..+$");
			Matcher emailMatcher;
			for(int i = 1; i < myRowMap.size() - 1; i++){
				String[] currentRow = myRowMap.get(i);
				String[] nextRow = myRowMap.get(i + 1);
				for (int j = 0; j < myRowMap.get(1).length; j++) {
					emailMatcher = emailpattern.matcher(currentRow[j]);
					if(emailMatcher.matches()){
						break;
					}
					else if(!myTableStats.extractPatternFromCell(currentRow[j]).equals(myTableStats.extractPatternFromCell(nextRow[j]))) {
//						return new HeaderDetection(-1, false);
						flag++;
					}
				}	
			}
		
			if(flag > myRowMap.size()*(2))
				return new HeaderDetection(-1, false);
			else
				return new HeaderDetection(0, true);
		}
		else
			return new HeaderDetection(-1, false);
	}
	
}

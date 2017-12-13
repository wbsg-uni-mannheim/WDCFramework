package org.webdatacommons.webtables.extraction.stats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jsoup.nodes.Element;

/**
 * 
 * 
 * The code was mainly copied from the DWT framework 
 * (https://github.com/JulianEberius/dwtc-extractor & https://github.com/JulianEberius/dwtc-tools)
 * 
 * @author Robert Meusel (robert@informatik.uni-mannheim.de) - Translation to DPEF
 *
 */
public class TableStats {
	private int tableWidth;
	public int getTableWidth() {
		return tableWidth;
	}

	public int getTableHeight() {
		return tableHeight;
	}

	private int tableHeight;
	private Element[][] tableArray; 
	public int rowIndex;
	public int colIndex;
	
	public TableStats(int width, int height) {
		this.tableHeight = height;
		this.tableWidth = width;
	}
	
	public TableStats(int width, int height, Element[][] tableArray) {
		this.tableHeight = height;
		this.tableWidth = width;
		this.tableArray = tableArray;
	}
	
	private List<String[]> myColList = new ArrayList<String[]>();
//	below function will return column without first row (because considering first row as a header row)
	public List<String[]> getColumns(){
		String[] colArray; 
		for(int i = 0; i < tableWidth; i++){
			colArray = new String[tableHeight - 1];
			for(int j = 1; j < tableHeight; j++){
				if(!(tableArray[j][i] == null))
					colArray[j - 1] = tableArray[j][i].text();
				else
					colArray[j-1] = "";
			}
			myColList.add(colArray);
		}
		
		return myColList;
		
	}
	
	private Map<Integer, String[]> myRowMap;
	public Map<Integer, String[]> getRows(int rowCounter){
		 myRowMap = new HashMap<Integer, String[]>(rowCounter);
		String[] rowArray; 
		Break_Here:
		for(int i = 0; i < tableHeight; i++){
			rowArray = new String[tableWidth];
			for(int j = 0; j < tableWidth; j++){
				if(!(tableArray[i][j] == null))
					rowArray[j] = tableArray[i][j].text();
				else
					rowArray[j] = "";
			}
			myRowMap.put(i, rowArray);
			rowCounter--;
			if(rowCounter==0)
				break Break_Here;
		}
		
		return myRowMap;
		
	}
	
	public int OverallCellLength(String[] row) {
		int cellLength = 0;
		for (String cell : row) {
				cellLength += cell.length(); 
		}
		
		return cellLength;
	}
	
	public boolean isStringOnly(String[] columnORrow)  
	{  
		int alphaCount = 0, anyCount = 0;
		if (columnORrow.length > 0) {
			
			// count occurrences of alphabetical and numerical
			// characters within the content string
			for(String str : columnORrow){
				for (char c : str.toCharArray()) {
					if(Character.isAlphabetic(c) || Character.isSpaceChar(c)) {
						alphaCount++;
					} 
					else{
					anyCount++;
					}
				}
			}
		}
		
		if(alphaCount > 0 && anyCount == 0)
			return true;
		else
			return false;

	}
	
	public boolean isAlphaNumeric(String[] columnORrow) {
		double stringCount = 0.0, digitCount = 0.0, specialCount = 0.0;
		double[] totalStringCount = new double[columnORrow.length];	
		double[] totalDigitCount = new double[columnORrow.length];	
		double[] totalSpecialCount = new double[columnORrow.length];	
		
		int counter = 0;
		if (columnORrow.length > 0) {
			// count occurrences of alphabetical, special and numerical
			// characters within the content string
			for(String str : columnORrow){
				for (char c : str.trim().toCharArray()) {
					if(Character.isAlphabetic(c) || Character.isSpaceChar(c)) {
						stringCount++;
					} 
					else if(Character.isDigit(c)){
					digitCount++;
						}
					else
						specialCount++;
				}
				totalStringCount[counter] = stringCount;
				totalDigitCount[counter] = digitCount;
				totalSpecialCount[counter] = specialCount;
				counter++;
				stringCount = 0.0;
				digitCount = 0.0;
				specialCount = 0.0;
			}
		}
		
		double avgStringCount = 0.0, avgDigitCount = 0.0, avgSpecialCount = 0.0;
		for(int i=0; i<totalStringCount.length; i++){
			avgStringCount = avgStringCount + totalStringCount[i];
		}
		
		for(int i=0; i<totalStringCount.length; i++){
			avgDigitCount = avgDigitCount + totalDigitCount[i];
		}
		
		for(int i=0; i<totalStringCount.length; i++){
			avgSpecialCount = avgSpecialCount + totalSpecialCount[i];
		}
		
		avgStringCount = avgStringCount/totalStringCount.length;
		avgDigitCount = avgDigitCount/totalDigitCount.length;
		avgSpecialCount = avgSpecialCount/totalSpecialCount.length;
		
		double totalAverageCount = avgStringCount + avgDigitCount + avgSpecialCount; 
		
		if((avgStringCount/totalAverageCount)*100 > 50 && avgSpecialCount < avgStringCount*0.1)
			return true;
		else
			return false;

	}
	
	public double avgColumnCellLength(String[] column) {
		double length = 0;
		if (column.length > 0) {
			for(String str : column){
				length = length + str.length();
			}
		}
		
		return length/column.length;
	}
	
	public String extractPatternFromCell(String cell) {
		String cellPattern = cell //
				.replace("\\s", "")
				.replaceAll("[a-zA-Z]+", "a") // alphabetical
				.replaceAll("[0-9]+", "d")// digits
				// http://www.enchantedlearning.com/grammar/punctuation/
				.replaceAll("[^ad\\s.!;():?,\\-'\"]+", "s")// special (no alphabetical, digits or punctuation)
				.replaceAll("[\\s.!;():?,\\-'\"]+", "p"); // punctuation
		
		return cellPattern;
	}
}

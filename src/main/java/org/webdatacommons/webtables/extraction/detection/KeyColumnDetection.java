package org.webdatacommons.webtables.extraction.detection;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.webdatacommons.webtables.extraction.stats.TableStats;

/**
 * 
 * 
 * The code was mainly copied from the DWT framework 
 * (https://github.com/JulianEberius/dwtc-extractor & https://github.com/JulianEberius/dwtc-tools)
 * 
 * @author Robert Meusel (robert@informatik.uni-mannheim.de) - Translation to DPEF
 *
 */
public class KeyColumnDetection {
	public TableStats myTableStats;
	int numNulls = 0;
	
	int keyColumnIndex =0;
	public int getKeyColumnIndex() {
		return keyColumnIndex;
	}

	boolean hasKeyColumn;	
	public boolean isHasKeyColumn() {
		return hasKeyColumn;
	}
	
	public KeyColumnDetection(TableStats myTableStats) {
		this.myTableStats=myTableStats;
	}

	public KeyColumnDetection(boolean hasKeyColumn, int keyColumnIndex) {
		this.hasKeyColumn = hasKeyColumn;
		this.keyColumnIndex = keyColumnIndex;
	}
	
	//to find out column uniqueness
	public double getColumnUniqnessRank(String[] column) {
	    int uniqueValues = this.getNumberOfUniqueValues(column);
	    
	    double rank1 = (double)uniqueValues / (double)column.length;
	    double rank2 = (double)numNulls / (double)column.length;

	    return rank1 - rank2;
	}
	
	public int getNumberOfUniqueValues(String[] column) {

	    HashSet<String> values = new HashSet<>();

	    for(String columnVal : column) {
	        values.add(columnVal);
	        if(columnVal.isEmpty())
	        	numNulls++;
	    }

	    return values.size();
	}
	
	public KeyColumnDetection keyColumnDetection(){
		List<String[]> colList = myTableStats.getColumns();
		
	List<Double> columnUniqueness	= new ArrayList<>(myTableStats.getTableWidth());
	for (String[] column : colList) {
		if(myTableStats.avgColumnCellLength(column) > 50)
			columnUniqueness.add(0.0);
		else
			columnUniqueness.add(this.getColumnUniqnessRank(column));
    }
	
    double maxCount = -1;
    int maxColumn = -1;

    for (int i = 0; i < columnUniqueness.size(); i++) {
        if (columnUniqueness.get(i) > maxCount && myTableStats.isAlphaNumeric(colList.get(i))
                && myTableStats.avgColumnCellLength(colList.get(i)) > 3.5
                && myTableStats.avgColumnCellLength(colList.get(i)) <= 200) {
            maxCount = (Double) columnUniqueness.get(i);
            maxColumn = i;
        }
    }

    if (maxColumn == -1) {
        return new KeyColumnDetection(false, -1);
    }
    else{
    	String[]key = colList.get(maxColumn);
        
        keyColumnIndex = colList.indexOf(key);

        if (columnUniqueness.get(keyColumnIndex) < 0.5) {
            return new KeyColumnDetection(false, -1);
        }
        else
        	return new KeyColumnDetection(true, keyColumnIndex);
    }
    
    }
}

package util;

import ch.qos.logback.classic.db.names.ColumnName;
import cooxm.devicecontrol.device.FactorDict;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：3 Feb 2015 18:45:25 
 */

public class Column extends FactorDict {
	private String columnName ;
	/** int,varchar,Datetime,double,etc*/
	private String dataType;
	private int columnID;
	
	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	public String getDataType() {
		return dataType;
	}
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	public int getColumnID() {
		return columnID;
	}
	public void setColumnID(int columnID) {
		this.columnID = columnID;
	}
	public Column() {
	}
	
	
	public Column(String columnName, String dataType, int columnID) {
		this.columnName = columnName;
		this.dataType = dataType;
		this.columnID = columnID;
	}
	public static void main(String[] args) {

	}

}

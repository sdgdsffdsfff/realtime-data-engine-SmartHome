package cooxm.util;

import java.util.Iterator;
import java.util.List;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：3 Feb 2015 19:06:06 
 */

public class DataXML {
	private List<Column> columns;	
	private int factorID;

	
	public int getFactorID() {
		return factorID;
	}
	public void setFactorID(int factorID) {
		this.factorID = factorID;
	}
	public List<Column> getColumns() {
		return columns;
	}
	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}
	public DataXML(List<Column> columns) {
		this.columns = columns;
		this.factorID=getFactorIDFromList();
	}
	public DataXML() {
	}
	
	/**获取所有的列名 */
	public String[] getColumnNames(){
		String[] fields=new String[this.columns.size()];
		for (Column column:columns) {
			int index=	column.getColumnID();
			String name=column.getColumnName();
			fields[index]=name;
		}		
		return fields;	
	}
	
	/**获取 对应序号的列名，序号从0开始 */
	public String getColumnNameBySequence(int sequence){
		int size=this.columns.size();
		if(sequence>=size){
			System.err.println("Error: Column sequence No out of boundary of columnList.");
			return null;	
		}
		for (int i=0;i<this.columns.size();i++) {
			if(	columns.get(i).getColumnID()==	sequence){
				return columns.get(i).getColumnName();
			}
		}
		return null;			
	}
	
	
	/**<pre>获取对应列名的序号，序号从0开始
	 * @return  对应列名的序号，如果列不存在返回-1*/
	public int getSequenceByColumnName(String columnName){
		int size=this.columns.size();
		for (int i=0;i<size;i++) {
			if(	columns.get(i).getColumnName()==	columnName){
				return columns.get(i).getColumnID();
			}
		}
		return -1;			
	}
	
	public int getFactorIDFromList(){
		int size=this.columns.size();
		for (int i=0;i<size;i++) {
			if(	columns.get(i).getColumnName()=="id"){
				return columns.get(i).getColumnID();
			}
		}
		
		return -1;			
	}
	
	
	
	public static void main(String[] args) {

	}

}

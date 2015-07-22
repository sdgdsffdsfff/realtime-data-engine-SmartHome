package cooxm.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.parsers.*;  

import org.w3c.dom.*;  
import org.xml.sax.*;  
  
public class PraseXmlUtil  
{  
	
	private static Map<Integer,List<Column>> dataXMLMap;	
	
    public static Map<Integer, List<Column>> getDataXMLMap() {
		return dataXMLMap;
	}

	public static void setDataXMLMap(Map<Integer, List<Column>> dataXMLMap) {
		PraseXmlUtil.dataXMLMap = dataXMLMap;
	}


	public PraseXmlUtil()  
    {  
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();  
        try  
        {  
            DocumentBuilder db = dbf.newDocumentBuilder();  
            Document doc = db.parse("data_format.xml");  
  
            NodeList dogList = doc.getElementsByTagName("struct");  
            //System.out.println("共有" + dogList.getLength() + "个struct节点");  
            dataXMLMap=new HashMap<Integer, List<Column>>();
            for (int i = 0; i < dogList.getLength(); i++)  
            {  
                Node dog = dogList.item(i); 
                Element elem = (Element) dog;                  
                int key= Integer.parseInt(elem.getAttribute("id")); 
                int index=0;
                List<Column> columns=new ArrayList<Column>();
                for (Node node = dog.getFirstChild(); node != null; node = node.getNextSibling())  
                {  
                    if (node.getNodeType() == Node.ELEMENT_NODE)  
                    {  
                    	Column column=new Column();
                    	//String entryname = node.getNodeName();  
                    	Element elemEntry = (Element) node;
                    	String name = elemEntry.getAttribute("name");
                    	String type = elemEntry.getAttribute("type");
                    	String desc = elemEntry.getAttribute("desc");                     	
                    	
                    	column.setColumnID(index);                    	
                    	column.setColumnName(name);
                    	column.setDataType(type);
                    	columns.add(column); 
                    	//System.out.print(index+ "\t"+ name + "\t"+ type + "\t" +desc +"\n");
                    	index++;
                    } 
                    
                }  
                System.out.print("\n");
                dataXMLMap.put(key, columns);
                
            }  
        }  
        catch (Exception e)  
        {  
            e.printStackTrace();  
        }  
    }  
	
	public List<Column> getColumnListByID(int id){
    	for (Entry<Integer, List<Column>> entry:dataXMLMap.entrySet()) {
    		if(entry.getKey()==id){
    			return entry.getValue();
    		}
		}
		return null;		
	}
	
    
	/**获取所有的列名 */
	public List<String> getColumnNames(int id){		
    	for (Entry<Integer, List<Column>> entry:dataXMLMap.entrySet()) {
    		if(entry.getKey()==id){
    			List<String> fields=new ArrayList<String>();
				for (Column column:entry.getValue()) {
					/*int index=	column.getColumnID();
					String name=column.getColumnName();
					fields[index]=name;*/
					fields.add(column.getColumnName());					
				}
				return fields;
    		}			
    	}
		return null;
	}
	
	/**获取 对应序号的列名，序号从0开始 */
	public String getColumnNameBySequence(int id,int columnID){		
    	for (Entry<Integer, List<Column>> entry:dataXMLMap.entrySet()) {
    		if(entry.getKey()==id){
				for (Column column:entry.getValue()) {
					if(column.getColumnID()==columnID){
						return column.getColumnName();
					}
				}
				
    		}			
    	}
		return null;			
	}
	
	
	/**<pre>获取对应列名的序号，序号从0开始
	 * @return  对应列名的序号，如果列不存在返回-1*/
	public int getSequenceByColumnName(int id,String columnName){
    	for (Entry<Integer, List<Column>> entry:dataXMLMap.entrySet()) {
    		if(entry.getKey()==id){
				for (Column column:entry.getValue()) {
					if(column.getColumnName()==columnName){
						return column.getColumnID();
					}
				}				
    		}			
    	}
		return -1;			
	}
	
    
    public static void main(String[] args)  
    { 
    	PraseXmlUtil xml=new PraseXmlUtil();
    	List<String> xx=xml.getColumnNames(40);    	
    	System.out.println(xx.get(5));
//    	for (Entry<Integer, List<Column>> entry:dataXMLMap.entrySet()) {
//    		List<Column> columns=entry.getValue();
//    		System.out.println(entry.getKey() );
//		}    	
    	
    	List<String> yy=new ArrayList<String>();
    	yy.add("2");
    	yy.add("1");
    	System.out.println(yy.get(0)+"_"+yy.get(1));

    }
}
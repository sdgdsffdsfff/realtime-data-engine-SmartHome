package util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.*;  

import org.w3c.dom.*;  
import org.xml.sax.*;  
  
public class PraseXmlUtil  
{  
	
	static Map<Integer,DataXML> dataXMLMap;
	
    public PraseXmlUtil()  
    {  
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();  
        try  
        {  
            DocumentBuilder db = dbf.newDocumentBuilder();  
            Document doc = db.parse("data_format.xml");  
  
            NodeList dogList = doc.getElementsByTagName("struct");  
            System.out.println("共有" + dogList.getLength() + "个struct节点");  
            dataXMLMap=new HashMap<Integer, DataXML>();
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
                    	//String desc = elemEntry.getAttribute("desc");                     	
                    	//System.out.print(entryname + ":" + name + "\t"+ type + "\t" +desc +"\n");
                    	column.setColumnID(index);
                    	i++;
                    	column.setColumnName(name);
                    	column.setDataType(type);
                    	columns.add(column);  
                    	
                    }  
                }  
                //System.out.println();
                DataXML dataXML=new DataXML(columns);
                dataXMLMap.put(key, dataXML);
            }  
        }  
        catch (Exception e)  
        {  
            e.printStackTrace();  
        }  
    }  
    
    public static void main(String[] args)  
    { 
    	
    }
}
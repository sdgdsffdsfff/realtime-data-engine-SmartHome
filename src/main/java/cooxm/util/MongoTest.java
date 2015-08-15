package cooxm.util;

import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import cooxm.devicecontrol.control.Configure;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：Jun 25, 2015 4:37:57 PM 
 */

public class MongoTest {
	
	Mongo mongo = null;
	DB db = null;
	DBCollection table = null;
	
	public MongoTest() {
		Configure config=new Configure();
		String mongo_server_IP=config.getValue("mongo_server_ip");
		int mongo_server_port =Integer.parseInt(config.getValue("mongo_server_port"));
		mongo = new Mongo(mongo_server_IP, mongo_server_port);
		db = mongo.getDB("realTimeEngine");
		table = db.getCollection("sensorData");
	}
	
	
	public void toMongoDB(List<String> fields,String[] columns){
		if(this.mongo==null){
			return;
		}
		DBObject record = new BasicDBObject();
		for (int i = 0; i < columns.length; i++) {
			record.put(fields.get(i), columns[i]);			
		}
		table.insert(record);		
	}

	public void getFromMongoDB(){
		DBCursor cursor = table.find();
		System.out.println("        testFind: ");
		while (cursor.hasNext()) {
			System.out.println(cursor.next());
		}
		// 获取数据总条数
		int sum = cursor.count();
		System.out.println("sum===" + sum);
	}
	
	public static void main(String[] args) {
		String data="2501,20150327144914100,1256789,1101,1,101,2,65535,-65536";
		String[] columns=data.split(",");
		String token=columns[0];
		PraseXmlUtil xml = new PraseXmlUtil();
		List<String> fields=xml.getColumnNames(Integer.parseInt(token));
		MongoTest a = new MongoTest();
	    a.toMongoDB(fields,columns);
	    a.getFromMongoDB();
	    
	}

}

package util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：Feb 5, 2015 4:11:47 PM 
 */

public class GeneralMethod {
	
/* ------------------ 深度复制  ----------------------------*/
	public static Object depthClone(Object srcObj){ 
		Object cloneObj = null; 
		try { 
			ByteArrayOutputStream out = new ByteArrayOutputStream(); 
			ObjectOutputStream oo = new ObjectOutputStream(out); 
			oo.writeObject(srcObj); 
			ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray()); 
			ObjectInputStream oi = new ObjectInputStream(in); 
			cloneObj = oi.readObject(); 
		} catch (IOException e) { 
			e.printStackTrace(); 
		} catch (ClassNotFoundException e) { 
			e.printStackTrace(); 
		} 
		return cloneObj; 
	}
	

	public static void main(String[] args) {

	}

}

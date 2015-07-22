package cooxm.spout;
/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created 14 Jan 2015 11:06:04 
 */

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;

import cooxm.devicecontrol.socket.CtrolSocketServer;
import jline.internal.Log;


public class DataClient implements Runnable  {
	static Logger log =Logger.getLogger(DataClient.class);
	Socket sock=null;
	String data_server_IP;
	int data_server_port ;
	private final static String            auth="ClusterID=1,ServerType=200,ServerID=5";
	private final static String requestDataType="Sharding=false,DataTypeRange=All";
	public static  BlockingQueue<String> dataQueue= new ArrayBlockingQueue<String>(50000);
	FileWriter fwriter;
	BufferedWriter writer;//= new BufferedWriter(this.fwriter);
	InputStreamReader reader; //= new InputStreamReader(this.sock.getInputStream()); 
	BufferedReader input;//=new BufferedReader(reader) ;
	
	public DataClient(String IP,int port) throws IOException {
		this.data_server_IP=IP;
		this.data_server_port=port;
		String authResult=null;
		String SubscribeCode=null;
		this.sock=new  Socket(IP,port);
	
		if(this.sock==null){
			log.info("Connect to "+IP+":"+port+" failed.");
			return;
		}
		Header header=new Header("auth");
		try {
			header.writeByte(this.sock);
			writeMsgToScok(auth);
		} catch (IOException e1) {
			e1.printStackTrace();
		}		
		log.info("Send auth:"+auth);

        try {
        	authResult=header.readHeaderFromScok(sock);
			log.info("Auth result received:"+authResult); 
		} catch (IOException e) {
			e.printStackTrace();
		}
        //log.info(authResult.length());
        if(authResult.equals("AuthenCode=OK")){
    		header=new Header("requestDataType");
    		header.writeByte(this.sock);
        	writeMsgToScok(requestDataType);
        	log.info("Send requestDataType:"+requestDataType);
            try {
            	SubscribeCode=header.readHeaderFromScok(sock);
    			log.info(SubscribeCode); 
    		} catch (IOException e) {
    			e.printStackTrace();
    		}
            if(!SubscribeCode.equals("SubscribeCode=OK")){
            	this.sock.close();
            } 
        	
        }else{
        	log.info("Autherized to "+IP+":"+port+" failed. Socket will be closed.");
        	sock.close();
        }
    	fwriter= new FileWriter("data.txt",true);
    	writer= new BufferedWriter(this.fwriter);
    	reader = new InputStreamReader(this.sock.getInputStream());
    	input=new BufferedReader(reader) ;  	

	}

	

	
	public static class Header{
		byte mainVersion=1;
		byte subVersion=1;
		byte cmdCode;
		short dataLen;
		
		Header(){}
		Header(String request){
			if(request=="auth"){
				this.cmdCode=11;
				this.dataLen=(short) auth.length();
			}else if(request=="requestDataType"){
				this.cmdCode=12;
				this.dataLen=(short) requestDataType.length();
			}
		}
		
		public Header(byte[] b5){
			this.mainVersion=b5[0];
			this.subVersion =b5[1];
			this.cmdCode    =b5[2];
			byte[] len	= {b5[3],b5[4]};			
			this.dataLen=getShort(len);;
		}	
		
	    public String readHeaderFromScok(Socket sock) throws IOException    
	    {
	    	InputStream input = sock.getInputStream();
	    	byte[] b5=new byte[5];
	    	input.read(b5,0,5);
	    	Header header =new Header(b5);
	    	byte[] res=new byte[header.dataLen];
	    	input.read(res,0,header.dataLen);
	    	String x=new String(res);
	    	return x;    	
	    }
		
		public void writeByte(Socket sock) {
			try {
				DataOutputStream dataout=new DataOutputStream(sock.getOutputStream());
				dataout.write(this.mainVersion);
				dataout.write(this.subVersion);
				dataout.write(this.cmdCode);
	            dataout.writeShort((int)this.dataLen);
	            dataout.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}		
	}
	
	

    
    public  void writeMsgToScok(String msg) throws IOException   
    {
    	OutputStreamWriter writer = new OutputStreamWriter(this.sock.getOutputStream());  
        PrintWriter output = new PrintWriter(writer, true);      	
        output.print(msg); 
        output.flush();
    }
    
    public void toQueue(){
    	String data=null;
    	try {
			while(true){
				if((data=this.input.readLine())!=null){
					dataQueue.offer(data, 10, TimeUnit.MILLISECONDS);
					this.writer.write(data+"\n");
					//System.out.print(".");
				}else{
					Log.error("Socket is null,reconnecting...,IP="+this.data_server_IP+",port="+this.data_server_port);
					this.sock=null;
					return;					
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    	
    }

   public static short getShort(byte[] bytes)
    {
        return (short) ((0xff & bytes[1]) | (0xff00 & (bytes[0] << 8)));
    }
	
	/*public static boolean isReachable(InetAddress localInetAddr, InetAddress remoteInetAddr,int port, int timeout) { 		
		boolean isReachable = false; 
		Socket socket = null; 
		try{ 
		 socket = new Socket(); 
		 / **端口号设置为 0 表示在本地挑选一个可用端口进行连接 * /
		 SocketAddress localSocketAddr = new InetSocketAddress(localInetAddr, 0); 
		 socket.bind(localSocketAddr); 
		 InetSocketAddress endpointSocketAddr = new InetSocketAddress(remoteInetAddr, port); 
		 socket.connect(endpointSocketAddr, timeout);        
		 System.out.print("SUCCESS - connected to MsgServer! Local: " + 
				 			localInetAddr.getHostAddress() + " remote: " + 
				 			remoteInetAddr.getHostAddress() + " port:" + port); 
		 isReachable = true; 
		} catch(IOException e) { 
			System.err.print("FAILRE - CAN not connect! Local: " + 
				 	localInetAddr.getHostAddress() + " remote: " + 
				 	remoteInetAddr.getHostAddress() + " port:" + port); 
		} finally{ 
		 if(socket != null) { 
		 try{ 
		 socket.close(); 
		 } catch(IOException e) { 
			 System.err.print("Error occurred while closing socket.."); 
		   } 
		 } 
		} 
		return isReachable; 
	}*/
   

	
	@Override
	public void run() {
        while(true){        	
    		try {
    			if(this.sock==null ||this.sock.isClosed()|| !this.sock.isConnected()){
    				String authResult=null;
    				String SubscribeCode=null;
    				this.sock=new  Socket(this.data_server_IP,data_server_port);    			
    				if(this.sock==null){
    					return;
    				}
    				Header header=new Header("auth");
    				try {
    					header.writeByte(this.sock);
    					writeMsgToScok(auth);
    				} catch (IOException e1) {
    					e1.printStackTrace();
    				}    				
    				log.info("Send auth:"+auth);

    		        try {
    		        	authResult=header.readHeaderFromScok(sock);
    					log.info("Auth result received:"+authResult); 
    				} catch (IOException e) {
    					e.printStackTrace();
    				}
    		        if(authResult.equals("AuthenCode=OK")){
    		    		header=new Header("requestDataType");
    		    		header.writeByte(this.sock);
    		        	writeMsgToScok(requestDataType);
    		        	log.info("Send requestDataType:"+requestDataType);
    		            try {
    		            	SubscribeCode=header.readHeaderFromScok(sock);
    		    			log.info(SubscribeCode); 
    		    		} catch (IOException e) {
    		    			e.printStackTrace();
    		    		}
    		            if(!SubscribeCode.equals("SubscribeCode=OK")){
    		            	this.sock.close();
    		            } 
    		        	
    		        }else{
    		        	log.error("Autherized to "+data_server_IP+":"+data_server_port+" failed. Socket will be closed.");
    		        	sock.close();
    		        }
    		    	fwriter= new FileWriter("data.txt",true);
    		    	writer= new BufferedWriter(this.fwriter);
    		    	reader = new InputStreamReader(this.sock.getInputStream());
    		    	input=new BufferedReader(reader) ;  	   				
    			}else{
    				this.toQueue();
    			}
    		} catch (Exception e) {
    			e.printStackTrace();
				log.info("waiting for 120 senconds,Reconnect to  message server,IP:"+this.data_server_IP+" port: "+this.data_server_port); 
				try {
					Thread.sleep(120*1000);					
				} catch (InterruptedException e2) {
					e2.printStackTrace();
				} 
    		}		 
        }
	}

   	public static void main(String[] args) throws IOException, InterruptedException {
   			DataClient client=new DataClient("172.16.35.16", 10490);
   			new Thread(client).start();
   			/*int count=0;
   			while(client!=null && client.sock.isConnected()  ){
   			    String data=client.input.readLine();
   			    if(data==null){
   			    	//Thread.sleep(10*1000);		    	
   			    	client=new DataClient("172.16.35.16", 10490);
   			    	if(client.input.readLine()==null){ 
   						log.info("Disconnect");

   			    	}
   			    }else {
   					System.out.print(".");
   					count++;
   					if(count>=160){
   						count=0;
   						System.out.print("\n");
   						client.writer.flush();
   					}
   					client.writer.write(data+"\n"); 
   					
   					//log.info(data);
   			    }
   			}
		//}*/
   			
   			Thread.sleep(1000*1000);

	}


}

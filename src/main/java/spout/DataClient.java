package spout;
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
import java.net.Socket;
import java.net.UnknownHostException;



public class DataClient  extends Socket  {
	Socket sock;
	private final static String            auth="ClusterID=1,ServerType=200,ServerID=5";
	private final static String requestDataType="Sharding=false,DataTypeRange=All";
	FileWriter fwriter;
	BufferedWriter writer;//= new BufferedWriter(this.fwriter);
	InputStreamReader reader; //= new InputStreamReader(this.sock.getInputStream()); 
	BufferedReader input;//=new BufferedReader(reader) ;

	

	
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
		
		Header(byte[] b5){
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
		
		public void writeByte(Socket sock) throws IOException{
			DataOutputStream dataout=new DataOutputStream(sock.getOutputStream());
			dataout.write(this.mainVersion);
			dataout.write(this.subVersion);
			dataout.write(this.cmdCode);
            dataout.writeShort((int)this.dataLen);
            dataout.flush();
		}		
	}
	
	
	DataClient(String IP,int port) throws IOException{
		String authResult=null;
		String SubscribeCode=null;
		try {
			this.sock=new  Socket(IP,port);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();  
		}	
		if(this.sock==null){
			System.out.println("Connect to "+IP+":"+port+" failed.");
			return;
		}
		Header header=new Header("auth");
		header.writeByte(this.sock);
		writeMsgToScok(auth);
		System.out.println("Send auth:"+auth);

        try {
        	authResult=header.readHeaderFromScok(sock);
			System.out.println("Auth result received:"+authResult); 
		} catch (IOException e) {
			e.printStackTrace();
		}
        System.out.println(authResult.length());
        if(authResult.equals("AuthenCode=OK")){
    		header=new Header("requestDataType");
    		header.writeByte(this.sock);
        	writeMsgToScok(requestDataType);
        	System.out.println("Send requestDataType:"+requestDataType);
            try {
            	SubscribeCode=header.readHeaderFromScok(sock);
    			System.out.println(SubscribeCode); 
    		} catch (IOException e) {
    			e.printStackTrace();
    		}
            if(!SubscribeCode.equals("SubscribeCode=OK")){
            	this.sock.close();
            } 
        	
        }else{
        	System.out.println("Autherized to "+IP+":"+port+" failed. Socket will be closed.");
        	sock.close();
        }
    	fwriter= new FileWriter("data.txt",true);
    	writer= new BufferedWriter(this.fwriter);
    	reader = new InputStreamReader(this.sock.getInputStream());
    	input=new BufferedReader(reader) ;
	}
	

	

    
    public  void writeMsgToScok(String msg) throws IOException   
    {
    	OutputStreamWriter writer = new OutputStreamWriter(this.sock.getOutputStream());  
        PrintWriter output = new PrintWriter(writer, true);      	
        output.print(msg); 
        output.flush();
    }
    

	

   public static short getShort(byte[] bytes)
    {
        return (short) ((0xff & bytes[1]) | (0xff00 & (bytes[0] << 8)));
    }

   	public static void main(String[] args) throws IOException, InterruptedException {
	DataClient sock=new DataClient("172.16.35.174", 10490);
	int count=0;
		while(sock!=null  ){
		    String data=sock.input.readLine();
		    if(data==null){
		    	Thread.sleep(10*1000);		    	
		    	sock=new DataClient("172.16.35.174", 10490);
		    	if(sock.input.readLine()==null){ 
					System.out.println("Disconnect");
			    	break;		
		    	}
		    }else {
				System.out.print(".");
				count++;
				if(count>=160){
					count=0;
					System.out.print("\n");
				}
				sock.writer.write(data+"\n");
		    }
		}
	}
}

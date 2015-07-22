package cooxm.trigger;

import java.util.concurrent.Callable;

import cooxm.devicecontrol.socket.Message;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：Apr 24, 2015 6:18:05 PM 
 */



import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

public class TriggerTimeOutTask implements Runnable{

	private int ctrolID;
	private int roomID;
	private int triggerID;
	private int timeOut;  //seconds

	public TriggerTimeOutTask(int ctrolID,int roomID,int triggerID,int timeOut) {
		this.ctrolID=ctrolID;
		this.roomID=roomID;
		this.triggerID=triggerID;
		this.timeOut=timeOut;
	}

	public void run() {
		System.out.println(Thread.currentThread().getName() + "\t" + new Date() + ",ctrolID=" + this.ctrolID + ",roomID=" + this.roomID+",triggerID="+this.triggerID);
		try {
			Thread.sleep(timeOut*1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
	
	public boolean equals(TriggerTimeOutTask task){
		if(this.ctrolID==task.ctrolID && this.roomID==task.roomID && this.triggerID==task.triggerID ){
			return true;
		}else{
			return false;
		}
	}
		


	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Timer timer = new Timer();
		TriggerTimeOutTask task = new TriggerTimeOutTask(1,2,3,5);
		TriggerTimeOutTask task1 = new TriggerTimeOutTask(1,2,4,5);
		//timer.schedule(task1, 0, 20000);
		Thread t=new Thread(task1);
		t.start();
		
		Thread.sleep(2000);
		t.interrupt();
		//if(t.interrupted()) {System.out.println("inter");}
		  System.out.println(new Date()+"----------------------------------");
		
	}
}

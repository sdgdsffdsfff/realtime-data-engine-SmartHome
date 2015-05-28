package trigger;

import java.util.Map;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：May 15, 2015 3:18:15 PM 
 */

public class TriggerLogicFactor {
	/** 逻辑计算的先后顺序 */
	int seq_no              ;
	int factorid1           ;     
	int factorid2           ; 
	/**逻辑关系 and  or */
	String logicalrelation  ;     
	int factorid3           ;
	boolean result          ;
	

	

	public int getSeq_no() {
		return seq_no;
	}


	public void setSeq_no(int seq_no) {
		this.seq_no = seq_no;
	}


	public int getFactorid1() {
		return factorid1;
	}


	public void setFactorid1(int factorid1) {
		this.factorid1 = factorid1;
	}


	public int getFactorid2() {
		return factorid2;
	}


	public void setFactorid2(int factorid2) {
		this.factorid2 = factorid2;
	}


	public String getLogicalrelation() {
		return logicalrelation;
	}


	public void setLogicalrelation(String logicalrelation) {
		this.logicalrelation = logicalrelation;
	}


	public int getFactorid3() {
		return factorid3;
	}


	public void setFactorid3(int factorid3) {
		this.factorid3 = factorid3;
	}


	public boolean isResult() {
		return result;
	}


	public void setResult(boolean result) {
		this.result = result;
	}

	TriggerLogicFactor(){
		
	}

	public TriggerLogicFactor(int seq_no, int factorid1,
			int factorid2, String logicalrelation, int factorid3) {
		this.seq_no = seq_no;
		this.factorid1 = factorid1;
		this.factorid2 = factorid2;
		this.logicalrelation = logicalrelation;
		this.factorid3 = factorid3;
	}
	
	public static void main(String[] args) {

	}

}

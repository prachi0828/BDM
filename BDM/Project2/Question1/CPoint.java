import java.io.FileWriter;
import java.util.Random;

public class CPoint {
	private static void CreateDataSet() {
		int count=0;
		int x=0;
		int y=0;
		String lineRecordString;
		 try { 
			 FileWriter fw = new FileWriter("point"); 
			 while(count<6100000){
				 count++;
				 x=new Random().nextInt()*10000;
				 y=new Random().nextInt()*10000;
				
				 lineRecordString=String.valueOf(x)+","+String.valueOf(y)+"\r\n";
				 fw.write(lineRecordString);  			 
			 }
			 fw.close(); 
			 } 
		 catch (Exception e) { 
			 } 
		 }

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CreateDataSet();
	}


}
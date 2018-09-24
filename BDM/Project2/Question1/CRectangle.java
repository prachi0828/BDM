import java.io.FileWriter;
import java.util.Random;

public class CRectangle {
	private static void cDataSet() {
		int index=0;
		int x=0;
		int y=0;		
		int height=0;
		int width=0;
		String recordString;
		 try { 
			 FileWriter fw = new FileWriter("rectangle"); 
			 while(index<6100000){
				 index++;
				 x=new Random().nextInt()*9995;
				 y=new Random().nextInt()*9980+20;
				 height=new Random().nextInt()*19+1;
				 width=new Random().nextInt()*4+1;
				 recordString="r"+String.valueOf(index)+","+String.valueOf(x)+","+String.valueOf(y)+","+String.valueOf(width)+","+String.valueOf(height)+"\r\n";
				 fw.write(recordString);  			 
			 }
			 fw.close(); 
			 } 
		 catch (Exception e) { 
			 } 
		 }


	public static void main(String[] args) 
	{
		cDataSet();
	}


}
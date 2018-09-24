//import files
import java .util.Random;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class generateDataset {
 	public static void main(String[] args) throws IOException {
		Random r= new Random();
		//path to output csv files
		String customercsvFile="//home//hadoop//BDM//customers.csv";
		String transactioncsvFile="//home//hadoop//BDM//transactions.csv";
		
		int id, ageMax=70, ageMin=10, nameMaxRange=20, nameMinRange=10, ccMaxRange=11, ccMinRange=1, salaryMax=10000, salaryMin=100;
		char[] chars = "abcdefghijklmnopqrstuvwxyz".toCharArray();
		
		//Generating Customer Dataset with ID ranging from 1 to 50000
		StringBuilder sbCust = new StringBuilder();
		PrintWriter pwCust = new PrintWriter(new File(customercsvFile));
			
		for(id=1;id<=50000;id++)
		{
			int rangeName=r.nextInt(nameMaxRange-nameMinRange)+nameMinRange;
			//generating name with length between 10 to 20
			for (int j = 0; j < rangeName; j++) 
			{
			    char c = chars[r.nextInt(chars.length)];
			    sbCust.append(c);
			}
			String outputName = sbCust.toString();
			sbCust.setLength(0);
			int resultAge=r.nextInt(ageMax-ageMin)+ageMin;//Age ranging between 10 and 70
			int countryCode=r.nextInt(ccMaxRange-ccMinRange)+ccMinRange;//Country Code ranging between 1 and 10						
			float salary=r.nextInt(salaryMax-salaryMin)+salaryMin;//Salary ranging between 100 and 1000
			String outputCustomer=id+","+outputName+","+resultAge+","+countryCode+","+salary+"\n";
			pwCust.write(outputCustomer); 
		}
		pwCust.close();
		System.out.println("done! cust");
	
		//Generating Transactions Dataset
		int transID, custID,transNumItems,transDescRange;
		int custIDMax=50000,custIDMin=1,transTotalMax=10000,transTotalMin=10,transNumItemsMax=10,transNumItemsMin=1,transDescRangeMax=50,transDescRangeMin=20;
		float transTotal;
		
		StringBuilder sbTrans= new StringBuilder();
		PrintWriter pwTrans = new PrintWriter(new File(transactioncsvFile));
		
		for(transID=1;transID<=5000000;transID++)
		{			
			custID=r.nextInt(custIDMax-custIDMin)+custIDMin;//Customer ID ranging between 1 and 50000
			transTotal=r.nextInt(transTotalMax-transTotalMin)+transTotalMin;//Transaction Total float value ranging between 10 and 10000
			transNumItems=r.nextInt(transNumItemsMax-transNumItemsMin)+transNumItemsMin;//Transaction Number of Items between 1 and 10
			transDescRange=r.nextInt(transDescRangeMax-transDescRangeMin)+transDescRangeMin;
			for (int j = 0; j < transDescRange; j++) {
			    char c = chars[r.nextInt(chars.length)];
			    sbTrans.append(c);
			}
			String outputTransDesc=sbTrans.toString();
			sbTrans.setLength(0);	
			String outputTransactions=transID+","+custID+","+transTotal+","+transNumItems+","+outputTransDesc+"\n";
			pwTrans.write(outputTransactions); 
		}
		pwTrans.close();
		System.out.println("done! trans");
	}
}
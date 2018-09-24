import java.io.FileWriter;
import java.util.Random;
public class Dataset
{
	public static void main(String[] args)
	{
		int count=1;
		float x,y;
		String line;
		try
		{
		// generate points for KmeansCluster
			FileWriter fwInputPoints = new FileWriter("inputpoints");
			while(count<=7500000)
			{
				x=new Random().nextFloat()*10000;
				y=new Random().nextFloat()*10000;
				line=String.valueOf(x)+","+String.valueOf(y)+"\n";
				fwInputPoints.write(line);
				count++;
			}
			fwInputPoints.close();
		}
		catch(Exception e){ }
	}
}
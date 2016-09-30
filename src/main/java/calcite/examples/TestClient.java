package calcite.examples;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Random;

public class TestClient {

	public static void main(String [] args)
	{
		
		try
		{
			System.out.println("Connecting to " + "localhost" +
			" on port " + 9999);
			Socket client = new Socket("localhost", 9999);
		    System.out.println("Just connected to " 
		    + client.getRemoteSocketAddress());
		    DataInputStream din=new DataInputStream(client.getInputStream());  
		    DataOutputStream dout=new DataOutputStream(client.getOutputStream());  
		    //BufferedReader br=new BufferedReader(new InputStreamReader(System.in));  
		    
		    Random rand = new Random();
		    //long rowtime;
		    int productId,orderId=0,units;		   		    
		    while(true){		   
		    	//rowtime = new java.util.Date().getTime();		    	
		    	//dout.writeLong(rowtime);
		    	productId = rand.nextInt(10) + 1;
		    	dout.writeInt(productId);
		    	orderId++;
		    	dout.writeInt(orderId);
		    	units = rand.nextInt(20) + 1;
		    	dout.writeInt(units);
		    	dout.flush();
		    	
			    System.out.println("Server says " + din.readUTF());			    
			    try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		    }
		    //client.close();
		}catch(IOException e)
		{
			e.printStackTrace();
		}
	}
}

package calcite.examples;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;

import com.csvreader.CsvWriter;

public class TestServer extends Thread
{
	   private ServerSocket serverSocket;
	   
	   public TestServer(int port) throws IOException
	   {
	      serverSocket = new ServerSocket(port);
	      //serverSocket.setSoTimeout(10000);
	   }

	   public void run()
	   {

		   System.out.println("Waiting for client on port " +
	       serverSocket.getLocalPort() + "...");
    	   try
    	   {		  
    		   	Socket server = serverSocket.accept();
    		   	System.out.println("Just connected to "
    		   			+ server.getRemoteSocketAddress());
    		   	DataInputStream din=new DataInputStream(server.getInputStream());  
    		   	DataOutputStream dout=new DataOutputStream(server.getOutputStream());  
    		   	//BufferedReader br=new BufferedReader(new InputStreamReader(System.in));
    		   	
    		   	CsvWriter csvOutput;
    		   	//long rowtime;
    		   	int productId,orderId,units;
    		   	while(true)
    		   	{	
    		   		productId=din.readInt();orderId=din.readInt();units=din.readInt();
    		   		System.out.println("productId: " + productId + 
    		   				", orderId: " + orderId + ", units: " + units);
    		   		// /home/hduser/Downloads/calcite-master/example/csv/target/test-classes/sales/SDEPTS.csv
    		   		csvOutput = new CsvWriter(new FileWriter("/home/hduser/Downloads/calcite-master/example/csv/target/test-classes/sales/SORDERS.csv", true), ',');
    		   		csvOutput.endRecord();
    		   		csvOutput.write(String.valueOf(productId));
    		   		csvOutput.write(String.valueOf(orderId));
    		   		csvOutput.write(String.valueOf(units));
    		   		csvOutput.close();
    		   		
    		   		dout.writeUTF("Order received.");
    		   		dout.flush();
    		   	}
    		   	//server.close();
    	   }catch(SocketTimeoutException s)
    	   {
    		   System.out.println("Socket timed out!");    		   
    	   }catch(IOException e)
    	   {
    		   e.printStackTrace();    		   
    	   }
		   	
	   }
	   public static void main(String [] args)
	   {
	      try
	      {
	         Thread t = new TestServer(9999);
	         t.start();
	      }catch(IOException e)
	      {
	         e.printStackTrace();
	      }
	   }
	}
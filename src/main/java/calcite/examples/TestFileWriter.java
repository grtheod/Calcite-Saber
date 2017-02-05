package calcite.examples;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

public class TestFileWriter {
	
	private static final String filename = "/home/hduser/Downloads/sampleText.txt";
	
	public static void main(String[] args) throws IOException, InterruptedException {
		PrintWriter pw = new PrintWriter(new FileOutputStream(filename, false));
			
		Random rand = new Random();

		int  randomNum;
		String input;
		for (int i = 0; i < 100; i++) {
			randomNum = rand.nextInt(1500) + 1;			
			input=""+i+","+randomNum;
			System.out.println(input);
			pw.print(input);
			pw.println();
			pw.flush();
			Thread.sleep(1000);
		}
	 
		pw.close();
	}

}

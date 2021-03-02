package it.unipi.hadoop;

import java.io.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.Arrays;
import java.util.Scanner; // Import the Scanner class to read text files

public class buildCenters {

	public static String[] extractCenters(int numElem, String filePath) throws FileNotFoundException {

		int numero_elementi = numElem;
		int max_row_file = 0;
		int[] row_means_iniziali;
		String[] means_extracted = new String[numero_elementi];

		// counting number of rows in the dataset
		try {
			BufferedReader reader = new BufferedReader(new FileReader(filePath));
			while (reader.readLine() != null)
				max_row_file++;
			reader.close();
		} catch (IOException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}

		// generating random integers
		row_means_iniziali = ThreadLocalRandom.current().ints(0, max_row_file).distinct().limit(numero_elementi)
				.toArray();
		Arrays.sort(row_means_iniziali);

		//for (int i = 0; i < numero_elementi; i++)
		//	System.out.println(row_means_iniziali[i] + " ");

		int current_row = 0;
		int current_element = 0;
		try {

			File myObj = new File(filePath);
			Scanner myReader = new Scanner(myObj);
			while (myReader.hasNextLine() && current_element < numero_elementi) {
				if (current_row == row_means_iniziali[current_element]) {
					means_extracted[current_element] = myReader.nextLine();
					current_element++;
				} else
					myReader.nextLine();
				current_row++;

			}

			myReader.close();
		} catch (FileNotFoundException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}

		return means_extracted;
	}
}

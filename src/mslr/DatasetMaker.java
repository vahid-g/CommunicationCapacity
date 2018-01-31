package mslr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DatasetMaker {

    public static void main(String[] args) {
	String dataFile = "test_data/mslr_sample.txt";
	String outputFile = "test_data/mslr_sample.csv";
	int urlIdCounter = 0;
	// Java 8 version!
	try (BufferedReader br = new BufferedReader(new FileReader(dataFile));
		FileWriter fw = new FileWriter(new File(outputFile))) {
	    String line;
	    while ((line = br.readLine()) != null) {
		String[] fields = line.split(" ");
		if (fields.length < 138) {
		    System.err.println(
			    "Not enough fields for line: " + urlIdCounter);
		    continue;
		}
		urlIdCounter++;
		// int numberOfSlashes = Integer.parseInt(fields[127]);
		// int urlLength = Integer.parseInt(fields[128]);
		// int inLinks = Integer.parseInt(fields[129]);
		// int outLinks = Integer.parseInt(fields[130]);
		// double pageRank = Double.parseDouble(fields[131]);
		// double qualityScore = Double.parseDouble(fields[133]);
		// int urlClickCount = Integer.parseInt(fields[136]);
		List<String> fieldsList = new ArrayList<String>();
		fieldsList.add(fields[127]);
		fieldsList.add(fields[128]);
		fieldsList.add(fields[129]);
		fieldsList.add(fields[130]);
		fieldsList.add(fields[131]);
		fieldsList.add(fields[133]);
		fieldsList.add(fields[135]);
		fieldsList.add(fields[136]);
		fieldsList = fieldsList.stream()
			.map(x -> x.substring(x.indexOf(':') + 1))
			.collect(Collectors.toList());
		String fieldsString = fieldsList.stream()
			.collect(Collectors.joining(" "));
		fw.write(urlIdCounter + " " + fieldsString + "\n");
	    }
	} catch (FileNotFoundException e) {
	    e.printStackTrace();
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

}

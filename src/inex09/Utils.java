package inex09;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Utils {

	public static List<String> addPrefix(List<String> list, String pref) {
		for (int i = 0; i < list.size(); i++) {
			String s = list.get(i);
			list.set(i, pref + s);
		}
		return list;
	}

	public static List<String> listFilesForFolder(final File folder) {
		List<String> list = new ArrayList<String>();
		for (final File fileEntry : folder.listFiles()) {
			if (fileEntry.isDirectory()) {
				list.addAll(listFilesForFolder(fileEntry));
			} else {
				list.add(fileEntry.getPath());
			}
		}
		return list;
	}

	public static List<String> readFileLines(String filename)
			throws IOException {
		FileReader fileReader = new FileReader(filename);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		List<String> lines = new ArrayList<String>();
		String line = null;
		while ((line = bufferedReader.readLine()) != null) {
			lines.add(line);
		}
		bufferedReader.close();
		return lines;
	}

	public static ArrayList<String[]> partitionArray(String[] array,
			int partitionCount) {
		ArrayList<String[]> partitions = new ArrayList<String[]>();
		int partitionSize = array.length / partitionCount;
		int i;
		for (i = 0; i < partitionCount - 1; i++) {
			partitions.add(Arrays.copyOfRange(array, i * partitionSize, (i + 1)
					* partitionSize));
		}
		partitions.add(Arrays.copyOfRange(array, i * partitionSize,
				array.length));
		return partitions;
	}

	public static List<List<String>> partitionList(List<String> list,
			int partitionCount) {
		List<List<String>> partitions = new ArrayList<List<String>>();
		int partitionSize = list.size() / partitionCount;
		int i;
		for (i = 0; i < partitionCount - 1; i++) {
			partitions.add(list.subList(i * partitionSize, (i + 1)
					* partitionSize));
		}
		partitions.add(list.subList(i * partitionSize, list.size()));
		return partitions;
	}

	public static ArrayList<String[]> triPartitionArray(String[] array,
			int partitionCount) {
		ArrayList<String[]> partitions = new ArrayList<String[]>();
		int partitionSize = array.length / partitionCount;
		int i;
		for (i = 0; i < partitionCount - 1; i++) {
			partitions.add(Arrays
					.copyOfRange(array, 0, (i + 1) * partitionSize));
		}
		partitions.add(Arrays.copyOfRange(array, 0, array.length));
		return partitions;
	}

	public static List<String[]> mergePartitions(List<String[]> relsList,
			List<String[]> othersList) {
		System.out.println("merging partitions..");
		List<String[]> mergedList = new ArrayList<String[]>();
		if (relsList.size() != othersList.size()) {
			System.err.println("error in mergePartitions");
		} else {
			for (int i = 0; i < relsList.size(); i++) {
				String[] rel = relsList.get(i);
				String[] other = othersList.get(i);
				System.out.println(" #" + i + " " + "|rels| = " + rel.length + " |others| = " + other.length);
				String[] merged = new String[other.length + rel.length];
				System.arraycopy(rel, 0, merged, 0, rel.length);
				System.arraycopy(other, 0, merged, rel.length, other.length);
				System.out.println(" |merged| = " + merged.length);
				mergedList.add(merged);
			}
		}
		return mergedList;
	}

	public static void shuffleArray(String[] ar) {
		Random rnd = new Random();
		for (int i = ar.length - 1; i > 0; i--) {
			int index = rnd.nextInt(i + 1);
			// Simple swap
			String a = ar[index];
			ar[index] = ar[i];
			ar[i] = a;
		}
	}
	
	public static <T> void shuffleList(List<T> list) {
		Random rnd = new Random();
		for (int i = list.size() - 1; i > 0; i--) {
			int index = rnd.nextInt(i + 1);
			// Simple swap
			T t = list.get(index);
			list.remove(index);
			list.add(index, list.get(i));
			list.remove(i);
			list.add(i, t);
		}
	}

}

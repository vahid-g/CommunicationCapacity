package freebase;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;

public class Utils {

    static void shuffleArray(Object[] ar) {
	// If running on Java 6 or older, use `new Random()` on RHS here
	Random rnd = new Random();
	for (int i = ar.length - 1; i > 0; i--) {
	    int index = rnd.nextInt(i + 1);
	    // Simple swap
	    Object a = ar[index];
	    ar[index] = ar[i];
	    ar[i] = a;
	}
    }

    public static int[][] addMatrix(int[][] a, int[][] b) {
	int[][] c = new int[a.length][a[0].length];
	for (int i = 0; i < a.length; i++) {
	    for (int j = 0; j < a[0].length; j++) {
		c[i][j] = a[i][j] + b[i][j];
	    }
	}
	return c;
    }

    public static List<FreebaseQuery> sampleFreebaseQueries(
	    List<FreebaseQuery> queries, int n) {
	Map<FreebaseQuery, Integer> freqMap = new HashMap<FreebaseQuery, Integer>();
	for (FreebaseQuery query : queries) {
	    if (freqMap.containsKey(query))
		freqMap.put(query, freqMap.get(query) + 1);
	    else
		freqMap.put(query, 1);
	}
	double[] pdf = getPdf(freqMap, queries);
	double[] cdf = getCdf(pdf);
	Random rand = new Random();
	List<FreebaseQuery> sampledQueries = new ArrayList<FreebaseQuery>();
	while (sampledQueries.size() < n) {
	    double r = rand.nextDouble();
	    int index = 0;
	    while (r > cdf[index] && pdf[index] > 0)
		index++;
	    FreebaseQuery query = queries.get(index);
	    sampledQueries.add(query);
	    int freq = Math.max(freqMap.get(query) - 1, 0);
	    freqMap.put(query, freq);
	    pdf = getPdf(freqMap, queries);
	    cdf = getCdf(pdf);
	}
	return sampledQueries;
    }

    private static double[] getCdf(double[] pdf) {
	double[] cdf = new double[pdf.length];
	double sum = 0;
	for (int i = 0; i < pdf.length; i++) {
	    cdf[i] = pdf[i] + sum;
	    sum = cdf[i];
	}
	return cdf;
    }

    private static double[] getPdf(Map<FreebaseQuery, Integer> freqMap,
	    List<FreebaseQuery> queries) {
	double[] pdf = new double[queries.size()];
	double sum = 0;
	for (Integer freq : freqMap.values())
	    sum += freq;
	for (int i = 0; i < queries.size(); i++) {
	    pdf[i] = freqMap.get(queries.get(i)) / sum;
	}
	return pdf;
    }

    public static List<FreebaseQuery> flattenFreebaseQueries(
	    List<FreebaseQuery> queries) {
	List<FreebaseQuery> flatList = new ArrayList<FreebaseQuery>();
	for (FreebaseQuery query : queries){
	    for (int i = 0; i < query.frequency; i++){
		flatList.add(new FreebaseQuery(i, query));
	    }
	}
	return flatList;
    }

	static void sshConnect() {
		String user = "ghadakcv";
		String password = "Hanh@nolde?";
		String host = "flip.engr.oregonstate.edu";
		int port = 22;
		int localPort = 4321;
		String remoteHost = "engr-db.engr.oregonstate.edu";
		int remotePort = 3307;
		try {
			JSch jsch = new JSch();
			FreebaseDataManager.session = jsch.getSession(user, host, port);
			FreebaseDataManager.session.setPassword(password);
			FreebaseDataManager.session
					.setConfig("StrictHostKeyChecking", "no");
			FreebaseDataManager.LOGGER.log(Level.INFO, "Establishing Connection...");
			FreebaseDataManager.session.connect();
			FreebaseDataManager.LOGGER.log(Level.INFO, "SSH Connection established.");
			int assinged_port = FreebaseDataManager.session.setPortForwardingL(
					localPort, remoteHost, remotePort);
			FreebaseDataManager.LOGGER.log(Level.INFO, "localhost:" + assinged_port + " -> "
					+ remoteHost + ":" + remotePort);
			FreebaseDataManager.LOGGER.log(Level.INFO, "Port Forwarded");
		} catch (JSchException e) {
			FreebaseDataManager.LOGGER.log(Level.SEVERE, e.toString());
		}
	}

	static void sshDisconnect() {
		FreebaseDataManager.session.disconnect();
		FreebaseDataManager.LOGGER.log(Level.INFO, "SSH Disconnected");
	}

	public static String sendCommand(String command) {
		StringBuilder outputBuffer = new StringBuilder();
		try {
			Channel channel = FreebaseDataManager.session.openChannel("exec");
			((ChannelExec) channel).setCommand(command);
			InputStream commandOutput = channel.getInputStream();
			channel.connect();
			int readByte = commandOutput.read();
			while (readByte != 0xffffffff) {
				outputBuffer.append((char) readByte);
				readByte = commandOutput.read();
			}
			channel.disconnect();
		} catch (IOException ioX) {
			ioX.printStackTrace();
		} catch (JSchException jschX) {
			jschX.printStackTrace();
		}
		return outputBuffer.toString();
	}
    
}

package irstyle.core;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

import irstyle.api.Params;

public class ExecPrepared {

	private double[] maxScores; // max scores possible so far for each CN
	private ArrayList[] resultSets; // list of ResultSet lists. One for each CN

	public ExecPrepared() {
	}

	private int getNumCombinations(ArrayList[] nodelists) {
		int numComb = 1;
		for (int i = 0; i < nodelists.length; i++)
			numComb *= nodelists[i].size();
		return numComb;
	}

	private int getNumCombinations(ArrayList[] nodelists, int numnfreeTSs) {
		int numComb = 1;
		for (int i = 0; i < numnfreeTSs; i++) // nodelists.length ;i++)
			numComb *= nodelists[i].size();
		return numComb;
	}

	private int[] getACombination(ArrayList[] nodelists, int combNumber) {
		int[] aCombination = new int[nodelists.length];
		for (int i = 0; i < nodelists.length; i++) {
			ArrayList currNodelist = nodelists[i];
			aCombination[i] = combNumber % currNodelist.size();
			combNumber /= currNodelist.size();
		}
		return aCombination;
	}

	private int[] getACombination(ArrayList[] nodelists, int combNumber, int numnfreeTSs) {
		int[] aCombination = new int[nodelists.length];
		for (int i = 0; i < numnfreeTSs; i++) {
			ArrayList currNodelist = nodelists[i];
			aCombination[i] = combNumber % currNodelist.size();
			combNumber /= currNodelist.size();
		}
		return aCombination;
	}

	private int getResults(JDBCaccess jdbcacc, PreparedStatement prepared, ArrayList[] S, int[] indexToBeChecked) {// obsolete
		try {
			int numparams = indexToBeChecked.length;
			int numresults = 0;
			for (int i = 0; i < numparams; i++) {
				int id = ((Integer) ((ArrayList) S[i]).get(indexToBeChecked[i])).intValue();
				prepared.setInt(i + 1, id);
			}
			ResultSet rs = jdbcacc.executePrepared(prepared);
			while (rs.next()) {
				numresults++;
				if (Flags.RESULTS__SHOW_OUTPUT)
					jdbcacc.printResult(rs);
			}
			return numresults;
		} catch (Exception e1) {
			System.out.println("exception class: " + e1.getClass() + "  with message: " + e1.getMessage()
					+ " exception in  ExecPrepared.getResults");
			e1.printStackTrace();
			return -1;
		}

	}

	private int getResultsParallel(JDBCaccess jdbcacc, PreparedStatement prepared, ArrayList[] S,
			int[] indexToBeChecked, int numnfreeTSs) {// differs in taht it inputs numnfreeTSs
														// obsolete
		try {
			int numparams = numnfreeTSs; // indexToBeChecked.length;
			int numresults = 0;
			for (int i = 0; i < numparams; i++) {
				int id = ((Integer) ((ArrayList) S[i]).get(indexToBeChecked[i])).intValue();
				prepared.setInt(i + 1, id);
			}
			ResultSet rs = jdbcacc.executePrepared(prepared);
			while (rs.next()) {
				numresults++;
				if (Flags.RESULTS__SHOW_OUTPUT)
					jdbcacc.printResult(rs);
			}
			return numresults;
		} catch (Exception e1) {
			System.out.println("exception class: " + e1.getClass() + "  with message: " + e1.getMessage()
					+ " exception in  ExecPrepared.getResults");
			e1.printStackTrace();
			return -1;
		}

	}

	private boolean sameTuple(ArrayList nfreeTSs, int[] indexToBeChecked, ArrayList[] S) {
		// ignore combinations with same tuple for the same tuple set
		for (int k = 0; k < nfreeTSs.size(); k++)
			for (int l = 0; l < nfreeTSs.size(); l++)
				if (k != l && ((String) nfreeTSs.get(k)).compareTo((String) nfreeTSs.get(l)) == 0
						&& ((Integer) S[k].get(indexToBeChecked[k]))
								.intValue() == ((Integer) S[l].get(indexToBeChecked[l])).intValue())
					return true;
		return false;
	}

	private double getScore(ArrayList[] scoresS, int[] indexToBeChecked, int CNSize) {
		double score = 0;
		for (int i = 0; i < indexToBeChecked.length; i++) {
			score += ((Integer) scoresS[i].get(indexToBeChecked[i])).doubleValue();
		}
		return score / CNSize;
	}

	private double getScore(double[] scores, int CNSize) {// applies ranking function to array of attributes with scores
															// in scores. We just sum them for now.
		double score = 0;
		for (int i = 0; i < scores.length; i++)
			score += scores[i];
		return score / CNSize;
	}

	private double getScore(ArrayList[] scoresS, int[] indexToBeChecked, int CNSize, int numnfreeTSs) {
		double score = 0;
		for (int i = 0; i < numnfreeTSs; i++) {
			score += ((Integer) scoresS[i].get(indexToBeChecked[i])).doubleValue();
		}
		return score / CNSize;
	}

	private double getScore(double[] scores, int CNSize, int numnfreeTSs) {// applies ranking function to array of
																			// attributes with scores in scores. We just
																			// sum them for now.
		double score = 0;
		for (int i = 0; i < numnfreeTSs; i++)
			score += scores[i];
		return score / CNSize;
	}

	private int addResult(double score, String result, ArrayList R, ArrayList scoresR, int N) {
		// adds result to results array R if result is in top-N results returns new
		// resultsSoFar
		int i = 0;
		for (i = 0; i < R.size(); i++)
			if (score > ((Double) scoresR.get(i)).doubleValue())
				break;
		// if(i>0) i--;
		if (i > N)
			return R.size();
		R.add(i, result);
		scoresR.add(i, new Double(score));
		if (R.size() > N) {
			int size = R.size();
			for (int j = N; j < size; j++) {
				R.remove(R.size() - 1);
				scoresR.remove(scoresR.size() - 1);
			}
		}
		return R.size();
	}

	// check for results and add them to R returns new resultsSoFar
	private int check4Results(JDBCaccess jdbcacc, PreparedStatement prepared, ArrayList[] S, ArrayList[] scoresS,
			int[] indexToBeChecked, ArrayList R, ArrayList scoresR, int N, int CNsize, ArrayList keywords,
			boolean allKeywInResults) {
		try {
			int numparams = indexToBeChecked.length;
			// int numresults=0;
			for (int i = 0; i < numparams; i++) {
				int id = ((Integer) ((ArrayList) S[i]).get(indexToBeChecked[i])).intValue();
				prepared.setInt(i + 1, id);
				if (Flags.DEBUG_INFO2)
					System.out.print(id + " ");
			}
			if (Flags.DEBUG_INFO2)
				System.out.println("");
			ResultSet rs = jdbcacc.executePrepared(prepared);
			if (rs == null)
				System.out.println("rs==null " + prepared.toString());
			else
				while (rs.next()) {
					// numresults++;
					double score = getScore(scoresS, indexToBeChecked, CNsize);
					if (!allKeywInResults)
						addResult(score, jdbcacc.getResult(rs), R, scoresR, N);
					else {
						String str = jdbcacc.getResult(rs);
						if (jdbcacc.containsAll(str, keywords))
							addResult(score, str, R, scoresR, N);
					}
					if (Flags.RESULTS__SHOW_OUTPUT)
						jdbcacc.printResult(rs);
				}
			// return numresults;
			return R.size();
		} catch (Exception e1) {
			System.out.println("exception class: " + e1.getClass() + "  with message: " + e1.getMessage()
					+ " exception in  ExecPrepared.check4Results");
			e1.printStackTrace();
			return -1;
		}
	}

	// check for results and add them to R returns new results SoFar differs in that
	// it inputs numnfreeTSs
	private int check4ResultsParallel(JDBCaccess jdbcacc, PreparedStatement prepared, ArrayList[] S,
			ArrayList[] scoresS, int[] indexToBeChecked, ArrayList R, ArrayList scoresR, int N, int CNsize,
			int numnfreeTSs, ArrayList keywords, boolean allKeywInResults) {
		try {
			int numparams = numnfreeTSs;// indexToBeChecked.length;
			// int numresults=0;
			for (int i = 0; i < numparams; i++) {
				int id = ((Integer) ((ArrayList) S[i]).get(indexToBeChecked[i])).intValue();
				prepared.setInt(i + 1, id);
				if (Flags.DEBUG_INFO2)
					System.out.print(id + " ");
			}
			if (Flags.DEBUG_INFO2)
				System.out.println("");
			ResultSet rs = jdbcacc.executePrepared(prepared);
			while (rs.next()) {
				// numresults++;
				double score = getScore(scoresS, indexToBeChecked, CNsize, numnfreeTSs);
				if (!allKeywInResults)
					addResult(score, jdbcacc.getResult(rs), R, scoresR, N);
				else {
					String str = jdbcacc.getResult(rs);
					if (jdbcacc.containsAll(str, keywords))
						addResult(score, str, R, scoresR, N);
				}
				if (Flags.RESULTS__SHOW_OUTPUT)
					jdbcacc.printResult(rs);
			}
			// return numresults;
			return R.size();
		} catch (Exception e1) {
			System.out.println("exception class: " + e1.getClass() + "  with message: " + e1.getMessage()
					+ " exception in  ExecPrepared.check4Results");
			e1.printStackTrace();
			return -1;
		}
	}

	private double getMax_rTi(int[] B, int[] lookaheadscores, int CNsize) {
		double max = -1;
		for (int i = 0; i < B.length; i++) {
			double[] scores = new double[B.length];
			for (int j = 0; j < B.length; j++) {
				if (i != j)
					scores[j] = B[j];
				else if (lookaheadscores[j] == -1)
					scores[j] = 0;
				else
					scores[j] = lookaheadscores[j];
			}
			double score = getScore(scores, CNsize);
			if (score > max && lookaheadscores[i] >= 0) // 10/20/02
				max = score;
		}
		return max;
	}

	private double getMax_rTiParallel(int[] B, int[] lookaheadscores, int CNsize, int numnfreeTSs) {
		double max = -1;
		for (int i = 0; i < numnfreeTSs; i++) {
			double[] scores = new double[numnfreeTSs];
			for (int j = 0; j < numnfreeTSs; j++)
				if (i != j)
					scores[j] = B[j];
				else if (lookaheadscores[j] == -1)
					scores[j] = 0;
				else
					scores[j] = lookaheadscores[j];
			double score = getScore(scores, CNsize);
			if (score > max && lookaheadscores[i] >= 0)
				max = score;
		}
		return max;
	}

	private int getIndexOfMax_rTi(int[] B, int[] lookaheadscores, int CNsize) {
		double max = -1;
		int index = -1;
		for (int i = 0; i < B.length; i++) {
			if (lookaheadscores[i] != -1) {
				double[] scores = new double[B.length];
				for (int j = 0; j < B.length; j++)
					if (i != j)
						scores[j] = B[j];
					else if (lookaheadscores[j] == -1)
						scores[j] = 0;
					else
						scores[j] = lookaheadscores[j];
				double score = getScore(scores, CNsize);
				if (score > max) {
					max = score;
					index = i;
				}
			}
		}
		return index;
	}

	private int getIndexOfMax_rTiParallel(int[] B, int[] lookaheadscores, int CNsize, int numnfreeTSs) {
		double max = -1;
		int index = -1;
		for (int i = 0; i < numnfreeTSs; i++) {
			if (lookaheadscores[i] != -1) {
				double[] scores = new double[numnfreeTSs];
				for (int j = 0; j < numnfreeTSs; j++)
					if (i != j)
						scores[j] = B[j];
					else if (lookaheadscores[j] == -1)
						scores[j] = 0;
					else
						scores[j] = lookaheadscores[j];
				double score = getScore(scores, CNsize);
				if (score > max) {
					max = score;
					index = i;
				}
			}
		}
		return index;
	}

	boolean foundTopN(ArrayList R, ArrayList scoresR, int[] lookaheadscores, int[] B, int N, int CNsize) {
		// check if R.size<N or R[N]<max r(Ti)
		if (R.size() > N)
			System.out.println("Error: R.size()>N");
		else if (R.size() < N)
			return false;
		else if (((Double) scoresR.get(N - 1)).doubleValue() < getMax_rTi(B, lookaheadscores, CNsize))
			return false;
		return true;
	}

	boolean foundTopNParallel(ArrayList R, ArrayList scoresR, int[][] lookaheadscores, int[][] B, int N, int[] CNsize,
			int[] numnfreeTSs) {
		// check if R.size<N or R[N]<max r(Ti)
		if (R.size() > N)
			System.out.println("Error: R.size()>N");
		else if (R.size() < N)
			return false;
		else {
			int numCNs = B.length;
			double max_rTi = 0;
			for (int c = 0; c < numCNs; c++) {
				double max_rTiofCN = getMax_rTiParallel(B[c], lookaheadscores[c], CNsize[c], numnfreeTSs[c]);
				if (max_rTiofCN > max_rTi)
					max_rTi = max_rTiofCN;
				if (Flags.DEBUG_INFO2)
					System.out.println("CN:" + c + "max_rTiofCN=" + max_rTiofCN);
			}
			if (((Double) scoresR.get(N - 1)).doubleValue() < max_rTi)
				return false;
		}
		return true;
	}

	private void printResults(ArrayList R, ArrayList scoresR) {
		System.out.println("print results");
		for (int i = 0; i < R.size(); i++)
			System.out.println(((String) R.get(i)) + "------" + ((Double) scoresR.get(i)).doubleValue());
	}

	// returns time also adds results and their scores to ResultsAndScores, which is
	// an ArrayList of type Result
	public int ExecuteParameterized(JDBCaccess jdbcacc, String sql, ArrayList nfreeTSs, ArrayList keywords, int N,
			int CNsize, ArrayList ResultsAndScores, boolean allKeywInResults) {
		int numPreparedQueries = 0;
		if (Flags.DEBUG_INFO) {
			String str = "ExecuteParameterized " + sql;
			for (int i = 0; i < nfreeTSs.size(); i++)
				str += " " + (String) nfreeTSs.get(i);
			str += " numresults requested = " + N + " Keywords: ";
			for (int i = 0; i < keywords.size(); i++)
				str += " " + (String) keywords.get(i);
			System.out.println(str);
		}

		long time1 = System.currentTimeMillis();
		int resultsSoFar = 0;
		PreparedStatement prepared = jdbcacc.createPreparedStatement(sql);
		int numkeywords = keywords.size();
		int numnfreeTSs = nfreeTSs.size();
		// keep the ids retrieved so far from nfree TS[i]
		ArrayList[] S = new ArrayList[numnfreeTSs];
		// keep the scores of the ids retrieved so far from nfree TS[i]
		ArrayList[] scoresS = new ArrayList[numnfreeTSs];
		ResultSet[] rs = new ResultSet[numnfreeTSs];
		// score of top tuple for nfree TS[i]. Used instead of the B(TSi)'s in alg.
		int[] B = new int[numnfreeTSs];
		// used to select a particular combination of ids to check for a result
		int[] indexToBeChecked = new int[numnfreeTSs];
		// used to keep ids retrieved but not yet processed. If -1 then no lookahead
		// available
		int[] lookahead = new int[numnfreeTSs];
		// used to keep scores of corresponding ids in lookahead. If -1 then no
		// lookahead available
		int[] lookaheadscores = new int[numnfreeTSs];
		ArrayList R = new ArrayList(N); // to store the Strings of top-N results
		ArrayList scoresR = new ArrayList(N); // to store the scores (double) of the top-N results

		for (int i = 0; i < numnfreeTSs; i++) {
			S[i] = new ArrayList(1);
			scoresS[i] = new ArrayList(1);
			rs[i] = jdbcacc.createCursor("select id,score from " + (String) nfreeTSs.get(i));
			int id = jdbcacc.getNextID(rs[i]);
			if (id < 0)
				System.out.println("not even one tuple in TS!!" + (String) nfreeTSs.get(i));
			int score = jdbcacc.getCurrScore(rs[i]);
			S[i].add(new Integer(id));
			scoresS[i].add(new Integer(score));
			lookahead[i] = id;
			lookaheadscores[i] = B[i] = score;
			indexToBeChecked[i] = 0;
		}
		if (!Flags.ALLOW_DUPLICATE_TUPLES) {
			if (!sameTuple(nfreeTSs, indexToBeChecked, S))
				resultsSoFar = check4Results(jdbcacc, prepared, S, scoresS, indexToBeChecked, R, scoresR, N, CNsize,
						keywords, allKeywInResults);
		} else
			resultsSoFar = check4Results(jdbcacc, prepared, S, scoresS, indexToBeChecked, R, scoresR, N, CNsize,
					keywords, allKeywInResults);
		// resultsSoFar+=getResults(jdbcacc,prepared, S, indexToBeChecked);
		for (int i = 0; i < numnfreeTSs; i++) {
			lookahead[i] = -1;
			lookaheadscores[i] = -1;
		}
		// check if R.size<N or R[N]<max r(Ti)
		boolean foundtopn = false;// foundTopN(R,scoresR,lookaheadscores,B,N,CNsize); //lookaheadscores[i]=-1 at
									// this point
		while (!foundtopn) {
			// get a tuple from all TSs with no lookahead
			// and also get TS with max score
			// int topScore=-1;
			int indexOfTopScore = -1;
			boolean allTSsFinished = true;
			for (int i = 0; i < numnfreeTSs; i++) {
				if (lookaheadscores[i] < 0) // no lookahead
				{
					lookahead[i] = jdbcacc.getNextID(rs[i]); // if finished will return -1
					if (lookahead[i] < 0)
						lookaheadscores[i] = -1;
					if (lookahead[i] > -1) {
						lookaheadscores[i] = jdbcacc.getCurrScore(rs[i]);
						allTSsFinished = false;
					}
				}
				if (lookaheadscores[i] > -1) // lookahead
				{
					allTSsFinished = false;
					// if(indexOfTopScore==-1) indexOfTopScore=i;
				}
				// if(lookaheadscores[i] >topScore)
				// {
				// topScore=lookaheadscores[i];
				// indexOfTopScore=i;
				// }
			}
			if (foundtopn = foundTopN(R, scoresR, lookaheadscores, B, N, CNsize))
				break;
			indexOfTopScore = getIndexOfMax_rTi(B, lookaheadscores, CNsize);
			if (allTSsFinished)
				break;
			// replace S[indexOfTopScore] by lookahead[indexOfTopScore] (=topScore). Replace
			// it back later
			S[indexOfTopScore].add(new Integer(lookahead[indexOfTopScore]));
			scoresS[indexOfTopScore].add(new Integer(lookaheadscores[indexOfTopScore]));
			ArrayList temp = (ArrayList) S[indexOfTopScore].clone();
			ArrayList scorestemp = (ArrayList) scoresS[indexOfTopScore].clone();
			S[indexOfTopScore].clear();
			scoresS[indexOfTopScore].clear();
			S[indexOfTopScore].add(new Integer(lookahead[indexOfTopScore]));
			scoresS[indexOfTopScore].add(new Integer(lookaheadscores[indexOfTopScore]));
			lookahead[indexOfTopScore] = -1;
			lookaheadscores[indexOfTopScore] = -1;
			int numcombinations = getNumCombinations(S);
			for (int j = 0; j < numcombinations; j++) {
				numPreparedQueries++;
				indexToBeChecked = getACombination(S, j);
				// ignore combinations with same tuple for the same tuple set
				if (!Flags.ALLOW_DUPLICATE_TUPLES) {
					if (!sameTuple(nfreeTSs, indexToBeChecked, S)) {
						resultsSoFar = check4Results(jdbcacc, prepared, S, scoresS, indexToBeChecked, R, scoresR, N,
								CNsize, keywords, allKeywInResults);
						if (foundtopn = foundTopN(R, scoresR, lookaheadscores, B, N, CNsize))
							break;
					}
				} else {
					resultsSoFar = check4Results(jdbcacc, prepared, S, scoresS, indexToBeChecked, R, scoresR, N, CNsize,
							keywords, allKeywInResults);
					if (foundtopn = foundTopN(R, scoresR, lookaheadscores, B, N, CNsize))
						break;
				}
			}
			if (foundtopn)
				break;
			S[indexOfTopScore] = temp;
			scoresS[indexOfTopScore] = scorestemp;
			// check if R.size<N or R[N]<max r(Ti)
			// foundtopn=foundTopN(R,scoresR,lookaheadscores,B,N,CNsize);
		}
		long time2 = System.currentTimeMillis();
		// System.out.println("results output = " + resultsSoFar + " numPreparedQueries
		// = " + numPreparedQueries
		// + " in time = " + (time2 - time1));
		if (Flags.RESULTS__SHOW_OUTPUT)
			printResults(R, scoresR);
		for (int i = 0; i < R.size(); i++)
			ResultsAndScores.add(new Result((String) R.get(i), ((Double) scoresR.get(i)).doubleValue()));
		return (int) (time2 - time1);
	}

	private double getMaxCNscore(int lookaheadscores[], int numnfreeTSs) {
		double ret = 0;
		for (int i = 0; i < numnfreeTSs; i++)
			ret += lookaheadscores[i];
		return ret / numnfreeTSs;
	}

	public int ExecuteParallel(JDBCaccess jdbcacc, String[] sqls, ArrayList[] nfreeTSs, ArrayList keywords, int N,
			int[] CNsize, ArrayList ResultsAndScores, boolean allKeywInResults) {// input:
		// sqls[i]: param sql for i-th CN
		// nfreeTSs[i]: list of non free TS names for i-th CN
		int numPreparedQueries = 0;
		if (Flags.DEBUG_INFO) {
			String str = "ExecuteParallel ";
			for (int i = 0; i < sqls.length; i++)
				str += " " + sqls[i];
			str += "\r\n";
			for (int c = 0; c < nfreeTSs.length; c++) {
				str += " CN #" + c;
				for (int i = 0; i < nfreeTSs[c].size(); i++)
					str += " " + (String) nfreeTSs[c].get(i);
				str += "\r\n";
			}
			str += " numresults requested = " + N + " Keywords: ";
			for (int i = 0; i < keywords.size(); i++)
				str += " " + (String) keywords.get(i);
			System.out.println(str);
		}
		long time1 = System.currentTimeMillis();
		int numCNs = sqls.length;
		maxScores = new double[numCNs];
		resultSets = new ArrayList[numCNs];
		for (int c = 0; c < numCNs; c++)
			resultSets[c] = new ArrayList();
		// for(int i=0;i<numCNs;i++)
		// results

		// the first index of all 2 dimensional arrays is the CN #
		int resultsSoFar = 0;
		PreparedStatement[] prepared = new PreparedStatement[numCNs];
		int[] numnfreeTSs = new int[numCNs];
		for (int i = 0; i < numCNs; i++) {
			prepared[i] = jdbcacc.createPreparedStatement(sqls[i]);
			numnfreeTSs[i] = nfreeTSs[i].size();
		}
		int numkeywords = keywords.size();
		int maxnfreeTSsize = 0;
		for (int i = 0; i < numCNs; i++)
			if (nfreeTSs.length > maxnfreeTSsize)
				maxnfreeTSsize = nfreeTSs.length;
		// array (for each CN) of arrays (for each nfreeTS, size of max chosen for
		// width, so some useless i,j combinations in S) of Arraylists (of tuple ids)
		ArrayList[][] S = new ArrayList[numCNs][maxnfreeTSsize];
		// array (for each CN) of arrays (for each nfreeTS, size of max chosen for
		// width, so some useless i,j combinations in S) of Arraylists (of tuple ids)
		ArrayList[][] scoresS = new ArrayList[numCNs][maxnfreeTSsize];
		// ArrayList[] S=new ArrayList[numnfreeTSs]; //keep the ids retrieved so far
		// from nfree TS[i]
		// ResultSet[] rs=new ResultSet[numnfreeTSs];
		// int[] topscores=new int[numnfreeTSs]; //max score so far for nfree TS[i].
		// Score is score(1)+...+score(numkeywords) as defined in MIndexAccess
		int[][] indexToBeChecked = new int[numCNs][maxnfreeTSsize]; // used to select a particular combination of ids to
																	// check for a result
		int[][] lookahead = new int[numCNs][maxnfreeTSsize]; // used to keep ids retrieved but not yet processed. If -1
																// then no lookahead available
		int[][] lookaheadscores = new int[numCNs][maxnfreeTSsize]; // used to keep scores of corresponding ids in
																	// lookahead. If -1 then no lookahead available
		int[][] B = new int[numCNs][maxnfreeTSsize]; // score of top tuple for nfree TS[i]. Used instead of the B(TSi)'s
														// in alg.
		ArrayList R = new ArrayList(N); // to store the Strings of top-N results
		ArrayList scoresR = new ArrayList(N); // to store the scores (double) of the top-N results

		for (int c = 0; c < numCNs; c++)
			for (int i = 0; i < numnfreeTSs[c]; i++) {
				S[c][i] = new ArrayList(1);
				scoresS[c][i] = new ArrayList(1);
				resultSets[c].add(jdbcacc.createCursor("select id,score from " + (String) nfreeTSs[c].get(i)));
				int id = jdbcacc.getNextID((ResultSet) resultSets[c].get(i));
				int score = -1;
				if (id > -1) {
					score = jdbcacc.getCurrScore((ResultSet) resultSets[c].get(i));
				}
				S[c][i].add(new Integer(id));
				scoresS[c][i].add(new Integer(score));
				lookahead[c][i] = id;
				lookaheadscores[c][i] = B[c][i] = score;
				indexToBeChecked[c][i] = 0;
			}
		for (int c = 0; c < numCNs; c++)
			if (!Flags.ALLOW_DUPLICATE_TUPLES) {
				if (!sameTuple(nfreeTSs[c], indexToBeChecked[c], S[c]))
					resultsSoFar = check4ResultsParallel(jdbcacc, prepared[c], S[c], scoresS[c], indexToBeChecked[c], R,
							scoresR, N, CNsize[c], numnfreeTSs[c], keywords, allKeywInResults);
			} else
				resultsSoFar = check4ResultsParallel(jdbcacc, prepared[c], S[c], scoresS[c], indexToBeChecked[c], R,
						scoresR, N, CNsize[c], numnfreeTSs[c], keywords, allKeywInResults);
		// resultsSoFar+=getResultsParallel(jdbcacc,prepared[c], S[c],
		// indexToBeChecked[c],numnfreeTSs[c]);
		for (int c = 0; c < numCNs; c++)
			for (int i = 0; i < numnfreeTSs[c]; i++) {
				lookahead[c][i] = -1;
				lookaheadscores[c][i] = -1;
			}
		// check if R.size<N or R[N]<max u(Ci)
		boolean foundtopn = false;// =foundTopNParallel(R,scoresR,lookaheadscores,B,N,CNsize,numnfreeTSs);
									// //lookaheadscores[i]=-1 at this point
		if (Flags.DEBUG_INFO2)
			printResults(R, scoresR);
		int loop = 0;
		while (!foundtopn) {
			loop++;
			// get CN with max score
			double CNtopScore = -1;
			int CNindexOfTopScore = -1;
			boolean allCNsFinished = true;
			boolean[] CNFinished = new boolean[numCNs];
			for (int c = 0; c < numCNs; c++)
				CNFinished[c] = true;
			for (int c = 0; c < numCNs; c++) {
				for (int i = 0; i < numnfreeTSs[c]; i++) {
					if (lookaheadscores[c][i] < 0) // no lookahead
					{
						// if finished will return -1
						lookahead[c][i] = jdbcacc.getNextID((ResultSet) resultSets[c].get(i));
						if (lookahead[c][i] < 0)
							lookaheadscores[c][i] = -1;
						if (lookahead[c][i] > -1) {
							lookaheadscores[c][i] = jdbcacc.getCurrScore((ResultSet) resultSets[c].get(i));
							allCNsFinished = false;
							CNFinished[c] = false;
						}
					}
					if (lookaheadscores[c][i] > -1) // lookahead
					{
						allCNsFinished = false;
						CNFinished[c] = false;
						// if(CNindexOfTopScore==-1) CNindexOfTopScore=c;
					}
				}
				// double CNscore=getMaxCNscore( lookaheadscores[c],numnfreeTSs[c]);
				// if(CNscore>CNtopScore)
				// {
				// CNtopScore=CNscore;
				// CNindexOfTopScore=c;
				// }
			}
			if (foundtopn = foundTopNParallel(R, scoresR, lookaheadscores, B, N, CNsize, numnfreeTSs))
				break;
			for (int c = 0; c < numCNs; c++) {
				if (!CNFinished[c]) {
					double CNscore = getMax_rTiParallel(B[c], lookaheadscores[c], CNsize[c], numnfreeTSs[c]);
					if (CNscore > CNtopScore) {
						CNtopScore = CNscore;
						CNindexOfTopScore = c;
					}
				}
			}
			// if(Flags.DEBUG_INFO)
			// {
			// for(int i=0;i<lookahead.length;i++)
			// {
			// String str="";
			// for(int j=0;j<numnfreeTSs[i];j++)
			// str+="("+lookahead[i][j]+" "+lookaheadscores[i][j]+") ";
			// System.out.println(str);
			// }
			// }
			if (allCNsFinished)
				break;
			// now get top result from the CNindexOfTopScore-th CN

			// get a tuple from all TSs with no lookahead (anyway, all have due to previous
			// step)
			// and also get TS with max score
			int topScore = -1;
			int indexOfTopScore = 0;
			boolean allTSsFinished = true;
			for (int i = 0; i < numnfreeTSs[CNindexOfTopScore]; i++) {
				if (lookaheadscores[CNindexOfTopScore][i] < 0) // no lookahead
				{
					lookahead[CNindexOfTopScore][i] = jdbcacc
							.getNextID((ResultSet) resultSets[CNindexOfTopScore].get(i)); // if finished will return -1
					if (lookahead[CNindexOfTopScore][i] > -1)
						lookaheadscores[CNindexOfTopScore][i] = jdbcacc
								.getCurrScore((ResultSet) resultSets[CNindexOfTopScore].get(i));
					allTSsFinished = false;
				}
				// if(lookaheadscores[CNindexOfTopScore][i] >topScore)
				// {
				// topScore=lookaheadscores[CNindexOfTopScore][i];
				// indexOfTopScore=i;
				// }
			}
			indexOfTopScore = getIndexOfMax_rTiParallel(B[CNindexOfTopScore], lookaheadscores[CNindexOfTopScore],
					CNsize[CNindexOfTopScore], numnfreeTSs[CNindexOfTopScore]);
			// if(allTSsFinished)//removed since allCNsFinished does same job
			// break;
			// replace S[indexOfTopScore] by lookahead[indexOfTopScore] (=topScore). Replace
			// it back later
			if (Flags.DEBUG_INFO)
				System.out.print(CNindexOfTopScore + "," + indexOfTopScore + " ");
			S[CNindexOfTopScore][indexOfTopScore].add(new Integer(lookahead[CNindexOfTopScore][indexOfTopScore]));
			scoresS[CNindexOfTopScore][indexOfTopScore]
					.add(new Integer(lookaheadscores[CNindexOfTopScore][indexOfTopScore]));
			ArrayList temp = (ArrayList) S[CNindexOfTopScore][indexOfTopScore].clone();
			ArrayList scorestemp = (ArrayList) scoresS[CNindexOfTopScore][indexOfTopScore].clone();
			S[CNindexOfTopScore][indexOfTopScore].clear();
			scoresS[CNindexOfTopScore][indexOfTopScore].clear();
			S[CNindexOfTopScore][indexOfTopScore].add(new Integer(lookahead[CNindexOfTopScore][indexOfTopScore]));
			scoresS[CNindexOfTopScore][indexOfTopScore]
					.add(new Integer(lookaheadscores[CNindexOfTopScore][indexOfTopScore]));
			lookahead[CNindexOfTopScore][indexOfTopScore] = -1;
			lookaheadscores[CNindexOfTopScore][indexOfTopScore] = -1;
			int numcombinations = getNumCombinations(S[CNindexOfTopScore], numnfreeTSs[CNindexOfTopScore]);
			for (int j = 0; j < numcombinations; j++) {
				numPreparedQueries++;
				if (Flags.DEBUG_INFO2) {
					System.out.println("CN: " + CNindexOfTopScore);
				}
				indexToBeChecked[CNindexOfTopScore] = getACombination(S[CNindexOfTopScore], j,
						numnfreeTSs[CNindexOfTopScore]);
				// ignore combinations with same tuple for the same tuple set
				if (!Flags.ALLOW_DUPLICATE_TUPLES) {
					if (!sameTuple(nfreeTSs[CNindexOfTopScore], indexToBeChecked[CNindexOfTopScore],
							S[CNindexOfTopScore])) {
						resultsSoFar = check4ResultsParallel(jdbcacc, prepared[CNindexOfTopScore], S[CNindexOfTopScore],
								scoresS[CNindexOfTopScore], indexToBeChecked[CNindexOfTopScore], R, scoresR, N,
								CNsize[CNindexOfTopScore], numnfreeTSs[CNindexOfTopScore], keywords, allKeywInResults);
						// resultsSoFar+=getResultsParallel(jdbcacc,prepared[CNindexOfTopScore],
						// S[CNindexOfTopScore],
						// indexToBeChecked[CNindexOfTopScore],numnfreeTSs[CNindexOfTopScore]);
						if (foundtopn = foundTopNParallel(R, scoresR, lookaheadscores, B, N, CNsize, numnfreeTSs))
							break;
					}
				} else {
					resultsSoFar = check4ResultsParallel(jdbcacc, prepared[CNindexOfTopScore], S[CNindexOfTopScore],
							scoresS[CNindexOfTopScore], indexToBeChecked[CNindexOfTopScore], R, scoresR, N,
							CNsize[CNindexOfTopScore], numnfreeTSs[CNindexOfTopScore], keywords, allKeywInResults);
					// if(foundtopn=foundTopNParallel(R,scoresR,lookaheadscores,B,N,CNsize,numnfreeTSs))
					// break;
				}
			}
			S[CNindexOfTopScore][indexOfTopScore] = temp;
			scoresS[CNindexOfTopScore][indexOfTopScore] = scorestemp;
			if (foundtopn)
				break;
			if (System.currentTimeMillis() - time1 > (Params.MAX_ALLOWED_TIME)) { // query takes more than 5mins
				break;
			}
		}
		if (Flags.DEBUG_INFO) {
			System.out.println("  #loop" + loop);
			for (int i = 0; i < S.length; i++) {
				String str = "CN #" + i;
				for (int k = 0; k < nfreeTSs[i].size(); k++)
					str += " " + (String) nfreeTSs[i].get(k);
				System.out.println(str);
				for (int j = 0; j < S[0].length; j++) {
					if (S[i][j] != null)
						System.out.print(S[i][j].size() + " ");
					else
						break;
				}
				System.out.println();
			}
		}
		long time2 = System.currentTimeMillis();
		if (Flags.DEBUG_INFO)
			System.out.println("    Parallel algor: results output = " + resultsSoFar + " numPreparedQueries = "
					+ numPreparedQueries + "  in time = " + (time2 - time1));
		if (Flags.RESULTS__SHOW_OUTPUT)
			printResults(R, scoresR);
		for (int i = 0; i < R.size(); i++)
			ResultsAndScores.add(new Result((String) R.get(i), ((Double) scoresR.get(i)).doubleValue()));
		jdbcacc.cleanup();
		return (int) (time2 - time1);
	}

	int getNextIndexSymmetric(int[] lookaheadscores, int nextIndex, int numnfreeTSs) // in symmetric
	{
		for (int l = nextIndex; l < numnfreeTSs; l++)
			if (lookaheadscores[l] > 0)
				return l;
		for (int l = 0; l < nextIndex; l++)
			if (lookaheadscores[l] > 0)
				return l;
		System.out.println("error in getNextIndexSymmetric");
		return 0;
	}

	int ExecuteParameterizedSymmetric(JDBCaccess jdbcacc, String sql, ArrayList nfreeTSs, ArrayList keywords, int N,
			int CNsize, ArrayList ResultsAndScores, boolean allKeywInResults) {// differs from ExecuteParameterized in
																				// that it goes down to TSs in parallel
																				// and does not pick the tuple with
																				// maximum r(Ti)
																				// returns time
																				// also adds results and their scores to
																				// ResultsAndScores, which is an
																				// ArrayList of type Result
		if (Flags.DEBUG_INFO) {
			String str = "ExecuteParameterized " + sql;
			for (int i = 0; i < nfreeTSs.size(); i++)
				str += " " + (String) nfreeTSs.get(i);
			str += " numresults requested = " + N + " Keywords: ";
			for (int i = 0; i < keywords.size(); i++)
				str += " " + (String) keywords.get(i);
			System.out.println(str);
		}

		long time1 = System.currentTimeMillis();
		int resultsSoFar = 0;
		PreparedStatement prepared = jdbcacc.createPreparedStatement(sql);
		int numkeywords = keywords.size();
		int numnfreeTSs = nfreeTSs.size();
		ArrayList[] S = new ArrayList[numnfreeTSs]; // keep the ids retrieved so far from nfree TS[i]
		ArrayList[] scoresS = new ArrayList[numnfreeTSs]; // keep the scores of the ids retrieved so far from nfree
															// TS[i]
		ResultSet[] rs = new ResultSet[numnfreeTSs];
		int[] B = new int[numnfreeTSs]; // score of top tuple for nfree TS[i]. Used instead of the B(TSi)'s in alg.
		int[] indexToBeChecked = new int[numnfreeTSs]; // used to select a particular combination of ids to check for a
														// result
		int[] lookahead = new int[numnfreeTSs]; // used to keep ids retrieved but not yet processed. If -1 then no
												// lookahead available
		int[] lookaheadscores = new int[numnfreeTSs]; // used to keep scores of corresponding ids in lookahead. If -1
														// then no lookahead available
		ArrayList R = new ArrayList(N); // to store the Strings of top-N results
		ArrayList scoresR = new ArrayList(N); // to store the scores (double) of the top-N results

		for (int i = 0; i < numnfreeTSs; i++) {
			S[i] = new ArrayList(1);
			scoresS[i] = new ArrayList(1);
			rs[i] = jdbcacc.createCursor("select id,score from " + (String) nfreeTSs.get(i));
			int id = jdbcacc.getNextID(rs[i]);
			if (id < 0)
				System.out.println("not even one tuple in TS!!" + (String) nfreeTSs.get(i));
			int score = jdbcacc.getCurrScore(rs[i]);
			S[i].add(new Integer(id));
			scoresS[i].add(new Integer(score));
			lookahead[i] = id;
			lookaheadscores[i] = B[i] = score;
			indexToBeChecked[i] = 0;
		}
		if (!Flags.ALLOW_DUPLICATE_TUPLES) {
			if (!sameTuple(nfreeTSs, indexToBeChecked, S))
				resultsSoFar = check4Results(jdbcacc, prepared, S, scoresS, indexToBeChecked, R, scoresR, N, CNsize,
						keywords, allKeywInResults);
		} else
			resultsSoFar = check4Results(jdbcacc, prepared, S, scoresS, indexToBeChecked, R, scoresR, N, CNsize,
					keywords, allKeywInResults);
		// resultsSoFar+=getResults(jdbcacc,prepared, S, indexToBeChecked);
		for (int i = 0; i < numnfreeTSs; i++) {
			lookahead[i] = -1;
			lookaheadscores[i] = -1;
		}
		// check if R.size<N or R[N]<max r(Ti)
		boolean foundtopn = false;// foundTopN(R,scoresR,lookaheadscores,B,N,CNsize); //lookaheadscores[i]=-1 at
									// this point
		int nextIndex = 0;// goes 0,..,numnfreeTSs-1,0,...
		while (!foundtopn) {
			// get a tuple from all TSs with no lookahead
			// and also get TS with max score
			// int topScore=-1;
			int indexOfTopScore = -1;
			boolean allTSsFinished = true;
			for (int i = 0; i < numnfreeTSs; i++) {
				if (lookaheadscores[i] < 0) // no lookahead
				{
					lookahead[i] = jdbcacc.getNextID(rs[i]); // if finished will return -1
					if (lookahead[i] < 0)
						lookaheadscores[i] = -1;
					if (lookahead[i] > -1) {
						lookaheadscores[i] = jdbcacc.getCurrScore(rs[i]);
						allTSsFinished = false;
					}
				}
				if (lookaheadscores[i] > -1) // lookahead
				{
					allTSsFinished = false;
					// if(indexOfTopScore==-1) indexOfTopScore=i;
				}
				// if(lookaheadscores[i] >topScore)
				// {
				// topScore=lookaheadscores[i];
				// indexOfTopScore=i;
				// }
			}
			// indexOfTopScore=getIndexOfMax_rTi(B,lookaheadscores,CNsize); //in non
			// symmetric
			if (allTSsFinished)
				break;
			if (foundtopn = foundTopN(R, scoresR, lookaheadscores, B, N, CNsize))
				break;
			nextIndex = (nextIndex + 1) % numnfreeTSs;
			indexOfTopScore = getNextIndexSymmetric(lookaheadscores, nextIndex, numnfreeTSs); // in symmetric

			// replace S[indexOfTopScore] by lookahead[indexOfTopScore] (=topScore). Replace
			// it back later
			S[indexOfTopScore].add(new Integer(lookahead[indexOfTopScore]));
			scoresS[indexOfTopScore].add(new Integer(lookaheadscores[indexOfTopScore]));
			ArrayList temp = (ArrayList) S[indexOfTopScore].clone();
			ArrayList scorestemp = (ArrayList) scoresS[indexOfTopScore].clone();
			S[indexOfTopScore].clear();
			scoresS[indexOfTopScore].clear();
			S[indexOfTopScore].add(new Integer(lookahead[indexOfTopScore]));
			scoresS[indexOfTopScore].add(new Integer(lookaheadscores[indexOfTopScore]));
			lookahead[indexOfTopScore] = -1;
			lookaheadscores[indexOfTopScore] = -1;
			int numcombinations = getNumCombinations(S);
			for (int j = 0; j < numcombinations; j++) {
				indexToBeChecked = getACombination(S, j);
				// ignore combinations with same tuple for the same tuple set
				if (!Flags.ALLOW_DUPLICATE_TUPLES) {
					if (!sameTuple(nfreeTSs, indexToBeChecked, S)) {
						// resultsSoFar+=getResults(jdbcacc,prepared, S, indexToBeChecked);
						resultsSoFar = check4Results(jdbcacc, prepared, S, scoresS, indexToBeChecked, R, scoresR, N,
								CNsize, keywords, allKeywInResults);
						if (foundtopn = foundTopN(R, scoresR, lookaheadscores, B, N, CNsize))
							break;
					}
				} else {
					resultsSoFar = check4Results(jdbcacc, prepared, S, scoresS, indexToBeChecked, R, scoresR, N, CNsize,
							keywords, allKeywInResults);
					if (foundtopn = foundTopN(R, scoresR, lookaheadscores, B, N, CNsize))
						break;
				}
			}
			if (foundtopn)
				break;
			S[indexOfTopScore] = temp;
			scoresS[indexOfTopScore] = scorestemp;
			// check if R.size<N or R[N]<max r(Ti)
			// foundtopn=foundTopN(R,scoresR,lookaheadscores,B,N,CNsize);
		}
		long time2 = System.currentTimeMillis();
		System.out.println("results output = " + resultsSoFar + " in time = " + (time2 - time1));
		if (Flags.RESULTS__SHOW_OUTPUT)
			printResults(R, scoresR);
		for (int i = 0; i < R.size(); i++)
			ResultsAndScores.add(new Result((String) R.get(i), ((Double) scoresR.get(i)).doubleValue()));
		return (int) (time2 - time1);
	}

	public int ExecuteParallelSymmetric(JDBCaccess jdbcacc, String[] sqls, ArrayList[] nfreeTSs, ArrayList keywords,
			int N, int[] CNsize, boolean allKeywInResults) {// input:
															// sqls[i]: param sql for i-th CN
															// nfreeTSs[i]: list of non free TS names for i-th CN

		if (Flags.DEBUG_INFO) {
			String str = "ExecuteParallel ";
			for (int i = 0; i < sqls.length; i++)
				str += " " + sqls[i];
			str += "\r\n";
			for (int c = 0; c < nfreeTSs.length; c++) {
				str += " CN #" + c;
				for (int i = 0; i < nfreeTSs[c].size(); i++)
					str += " " + (String) nfreeTSs[c].get(i);
				str += "\r\n";
			}
			str += " numresults requested = " + N + " Keywords: ";
			for (int i = 0; i < keywords.size(); i++)
				str += " " + (String) keywords.get(i);
			System.out.println(str);
		}
		long time1 = System.currentTimeMillis();
		int numCNs = sqls.length;
		maxScores = new double[numCNs];
		resultSets = new ArrayList[numCNs];
		for (int c = 0; c < numCNs; c++)
			resultSets[c] = new ArrayList();
		// for(int i=0;i<numCNs;i++)
		// results

		// the first index of all 2 dimensional arrays is the CN #
		int resultsSoFar = 0;
		PreparedStatement[] prepared = new PreparedStatement[numCNs];
		int[] numnfreeTSs = new int[numCNs];
		for (int i = 0; i < numCNs; i++) {
			prepared[i] = jdbcacc.createPreparedStatement(sqls[i]);
			numnfreeTSs[i] = nfreeTSs[i].size();
		}
		int numkeywords = keywords.size();
		int maxnfreeTSsize = 0;
		for (int i = 0; i < numCNs; i++)
			if (nfreeTSs.length > maxnfreeTSsize)
				maxnfreeTSsize = nfreeTSs.length;
		ArrayList[][] S = new ArrayList[numCNs][maxnfreeTSsize]; // array (for each CN) of arrays (for each nfreeTS,
																	// size of max chosen for width, so some useless i,j
																	// combinations in S) of Arraylists (of tuple ids)
		ArrayList[][] scoresS = new ArrayList[numCNs][maxnfreeTSsize]; // array (for each CN) of arrays (for each
																		// nfreeTS, size of max chosen for width, so
																		// some useless i,j combinations in S) of
																		// Arraylists (of tuple ids)
		// ArrayList[] S=new ArrayList[numnfreeTSs]; //keep the ids retrieved so far
		// from nfree TS[i]
		// ResultSet[] rs=new ResultSet[numnfreeTSs];
		// int[] topscores=new int[numnfreeTSs]; //max score so far for nfree TS[i].
		// Score is score(1)+...+score(numkeywords) as defined in MIndexAccess
		int[][] indexToBeChecked = new int[numCNs][maxnfreeTSsize]; // used to select a particular combination of ids to
																	// check for a result
		int[][] lookahead = new int[numCNs][maxnfreeTSsize]; // used to keep ids retrieved but not yet processed. If -1
																// then no lookahead available
		int[][] lookaheadscores = new int[numCNs][maxnfreeTSsize]; // used to keep scores of corresponding ids in
																	// lookahead. If -1 then no lookahead available
		int[][] B = new int[numCNs][maxnfreeTSsize]; // score of top tuple for nfree TS[i]. Used instead of the B(TSi)'s
														// in alg.
		ArrayList R = new ArrayList(N); // to store the Strings of top-N results
		ArrayList scoresR = new ArrayList(N); // to store the scores (double) of the top-N results

		for (int c = 0; c < numCNs; c++)
			for (int i = 0; i < numnfreeTSs[c]; i++) {
				S[c][i] = new ArrayList(1);
				scoresS[c][i] = new ArrayList(1);
				resultSets[c].add(jdbcacc.createCursor("select id,score from " + (String) nfreeTSs[c].get(i)));
				int id = jdbcacc.getNextID((ResultSet) resultSets[c].get(i));
				int score = jdbcacc.getCurrScore((ResultSet) resultSets[c].get(i));
				S[c][i].add(new Integer(id));
				scoresS[c][i].add(new Integer(score));
				lookahead[c][i] = id;
				lookaheadscores[c][i] = B[c][i] = score;
				indexToBeChecked[c][i] = 0;
			}
		for (int c = 0; c < numCNs; c++)
			if (!Flags.ALLOW_DUPLICATE_TUPLES) {
				if (!sameTuple(nfreeTSs[c], indexToBeChecked[c], S[c]))
					resultsSoFar = check4ResultsParallel(jdbcacc, prepared[c], S[c], scoresS[c], indexToBeChecked[c], R,
							scoresR, N, CNsize[c], numnfreeTSs[c], keywords, allKeywInResults);
			} else
				resultsSoFar = check4ResultsParallel(jdbcacc, prepared[c], S[c], scoresS[c], indexToBeChecked[c], R,
						scoresR, N, CNsize[c], numnfreeTSs[c], keywords, allKeywInResults);
		// resultsSoFar+=getResultsParallel(jdbcacc,prepared[c], S[c],
		// indexToBeChecked[c],numnfreeTSs[c]);
		for (int c = 0; c < numCNs; c++)
			for (int i = 0; i < numnfreeTSs[c]; i++) {
				lookahead[c][i] = -1;
				lookaheadscores[c][i] = -1;
			}

		int[] nextIndex = new int[numCNs];
		for (int l = 0; l < numCNs; l++)
			nextIndex[l] = 0;
		// check if R.size<N or R[N]<max u(Ci)
		boolean foundtopn = false;// foundTopNParallel(R,scoresR,lookaheadscores,B,N,CNsize,numnfreeTSs);
									// //lookaheadscores[i]=-1 at this point
		while (!foundtopn) {
			// get CN with max score
			double CNtopScore = -1;
			int CNindexOfTopScore = -1;
			boolean allCNsFinished = true;
			boolean[] CNFinished = new boolean[numCNs];
			for (int c = 0; c < numCNs; c++)
				CNFinished[c] = true;
			for (int c = 0; c < numCNs; c++) {
				for (int i = 0; i < numnfreeTSs[c]; i++) {
					if (lookaheadscores[c][i] < 0) // no lookahead
					{
						lookahead[c][i] = jdbcacc.getNextID((ResultSet) resultSets[c].get(i)); // if finished will
																								// return -1
						if (lookahead[c][i] < 0)
							lookaheadscores[c][i] = -1;
						if (lookahead[c][i] > -1) {
							lookaheadscores[c][i] = jdbcacc.getCurrScore((ResultSet) resultSets[c].get(i));
							allCNsFinished = false;
							CNFinished[c] = false;
						}
					}
					if (lookaheadscores[c][i] > -1) // lookahead
					{
						allCNsFinished = false;
						CNFinished[c] = false;
						// if(CNindexOfTopScore==-1) CNindexOfTopScore=c;
					}
				}
				// double CNscore=getMaxCNscore( lookaheadscores[c],numnfreeTSs[c]);
				// if(CNscore>CNtopScore)
				// {
				// CNtopScore=CNscore;
				// CNindexOfTopScore=c;
				// }
			}
			if (foundtopn = foundTopNParallel(R, scoresR, lookaheadscores, B, N, CNsize, numnfreeTSs))
				break;
			for (int c = 0; c < numCNs; c++) {
				if (!CNFinished[c]) {
					double CNscore = getMax_rTiParallel(B[c], lookaheadscores[c], CNsize[c], numnfreeTSs[c]);
					if (CNscore > CNtopScore) {
						CNtopScore = CNscore;
						CNindexOfTopScore = c;
					}
				}
			}
			// if(Flags.DEBUG_INFO)
			// {
			// for(int i=0;i<lookahead.length;i++)
			// {
			// String str="";
			// for(int j=0;j<numnfreeTSs[i];j++)
			// str+="("+lookahead[i][j]+" "+lookaheadscores[i][j]+") ";
			// System.out.println(str);
			// }
			// }
			if (allCNsFinished)
				break;
			// now get top result from the CNindexOfTopScore-th CN

			// get a tuple from all TSs with no lookahead (anyway, all have due to previous
			// step)
			// and also get TS with max score
			int topScore = -1;
			int indexOfTopScore = 0;
			boolean allTSsFinished = true;
			for (int i = 0; i < numnfreeTSs[CNindexOfTopScore]; i++) {
				if (lookaheadscores[CNindexOfTopScore][i] < 0) // no lookahead
				{
					lookahead[CNindexOfTopScore][i] = jdbcacc
							.getNextID((ResultSet) resultSets[CNindexOfTopScore].get(i)); // if finished will return -1
					if (lookahead[CNindexOfTopScore][i] > -1)
						lookaheadscores[CNindexOfTopScore][i] = jdbcacc
								.getCurrScore((ResultSet) resultSets[CNindexOfTopScore].get(i));
					allTSsFinished = false;
				}
				// if(lookaheadscores[CNindexOfTopScore][i] >topScore)
				// {
				// topScore=lookaheadscores[CNindexOfTopScore][i];
				// indexOfTopScore=i;
				// }
			}
			// indexOfTopScore=getIndexOfMax_rTiParallel(B[CNindexOfTopScore],lookaheadscores[CNindexOfTopScore],CNsize[CNindexOfTopScore],numnfreeTSs[CNindexOfTopScore]);
			// //in non symmetric
			nextIndex[CNindexOfTopScore] = (nextIndex[CNindexOfTopScore] + 1) % numnfreeTSs[CNindexOfTopScore];
			indexOfTopScore = getNextIndexSymmetric(lookaheadscores[CNindexOfTopScore], nextIndex[CNindexOfTopScore],
					numnfreeTSs[CNindexOfTopScore]); // in non symmetric
			// if(allTSsFinished)//removed since allCNsFinished does same job
			// break;
			// replace S[indexOfTopScore] by lookahead[indexOfTopScore] (=topScore). Replace
			// it back later
			S[CNindexOfTopScore][indexOfTopScore].add(new Integer(lookahead[CNindexOfTopScore][indexOfTopScore]));
			scoresS[CNindexOfTopScore][indexOfTopScore]
					.add(new Integer(lookaheadscores[CNindexOfTopScore][indexOfTopScore]));
			ArrayList temp = (ArrayList) S[CNindexOfTopScore][indexOfTopScore].clone();
			ArrayList scorestemp = (ArrayList) scoresS[CNindexOfTopScore][indexOfTopScore].clone();
			S[CNindexOfTopScore][indexOfTopScore].clear();
			scoresS[CNindexOfTopScore][indexOfTopScore].clear();
			S[CNindexOfTopScore][indexOfTopScore].add(new Integer(lookahead[CNindexOfTopScore][indexOfTopScore]));
			scoresS[CNindexOfTopScore][indexOfTopScore]
					.add(new Integer(lookaheadscores[CNindexOfTopScore][indexOfTopScore]));
			lookahead[CNindexOfTopScore][indexOfTopScore] = -1;
			lookaheadscores[CNindexOfTopScore][indexOfTopScore] = -1;
			int numcombinations = getNumCombinations(S[CNindexOfTopScore], numnfreeTSs[CNindexOfTopScore]);
			for (int j = 0; j < numcombinations; j++) {
				indexToBeChecked[CNindexOfTopScore] = getACombination(S[CNindexOfTopScore], j,
						numnfreeTSs[CNindexOfTopScore]);
				// ignore combinations with same tuple for the same tuple set
				if (!Flags.ALLOW_DUPLICATE_TUPLES) {
					if (!sameTuple(nfreeTSs[CNindexOfTopScore], indexToBeChecked[CNindexOfTopScore],
							S[CNindexOfTopScore])) {
						resultsSoFar = check4ResultsParallel(jdbcacc, prepared[CNindexOfTopScore], S[CNindexOfTopScore],
								scoresS[CNindexOfTopScore], indexToBeChecked[CNindexOfTopScore], R, scoresR, N,
								CNsize[CNindexOfTopScore], numnfreeTSs[CNindexOfTopScore], keywords, allKeywInResults);
						// resultsSoFar+=getResultsParallel(jdbcacc,prepared[CNindexOfTopScore],
						// S[CNindexOfTopScore],
						// indexToBeChecked[CNindexOfTopScore],numnfreeTSs[CNindexOfTopScore]);
						if (foundtopn = foundTopNParallel(R, scoresR, lookaheadscores, B, N, CNsize, numnfreeTSs))
							break;
					}
				} else {
					resultsSoFar = check4ResultsParallel(jdbcacc, prepared[CNindexOfTopScore], S[CNindexOfTopScore],
							scoresS[CNindexOfTopScore], indexToBeChecked[CNindexOfTopScore], R, scoresR, N,
							CNsize[CNindexOfTopScore], numnfreeTSs[CNindexOfTopScore], keywords, allKeywInResults);
					// if(foundtopn=foundTopNParallel(R,scoresR,lookaheadscores,B,N,CNsize,numnfreeTSs))
					// break;
				}
			}
			if (foundtopn)
				break;
			S[CNindexOfTopScore][indexOfTopScore] = temp;
			scoresS[CNindexOfTopScore][indexOfTopScore] = scorestemp;
		}

		long time2 = System.currentTimeMillis();
		System.out.println("Parallel algor: results output = " + resultsSoFar + " in time = " + (time2 - time1));
		if (Flags.RESULTS__SHOW_OUTPUT)
			printResults(R, scoresR);
		return (int) (time2 - time1);
	}

}
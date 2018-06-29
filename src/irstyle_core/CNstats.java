package irstyle_core;
public class CNstats
{
	double numActiveTSproduced=0; //#JNTS put is Q
	double numTSwithKeyw=0;	//#JNTS put is Q, that have all keywords
							//and no non-free tuple set can be replaced by a free tuple set
	double numTSwithKeywNoLeaf=0; //#JNTS put is Q, that have all keywords 
							//and no non-free tuple set can be replaced by a free tuple set 
							//and no free tuple set as leaf
	double numCNs=0;
	//numCNs is extracted from numTSwithKeywNoLeaf
	//by pruning out the JNTS�s with two occurences of the same tuple
	
	double numBasicTS=0; // # non-empty basic tuple sets
	void add(CNstats cnst)
	{
		numActiveTSproduced+=cnst.numActiveTSproduced;
		numTSwithKeyw+=cnst.numTSwithKeyw;
		numTSwithKeywNoLeaf+=cnst.numTSwithKeywNoLeaf;
		numCNs+=cnst.numCNs;		
		numBasicTS+=cnst.numBasicTS;
	}
	void divide(double d)
	{
		numActiveTSproduced/=d;
		numTSwithKeyw/=d;
		numTSwithKeywNoLeaf/=d;
		numCNs/=d;	
		numBasicTS/=d;
	}
	String print()
	{
		return 	"numActiveTSproduced="+numActiveTSproduced+" numTSwithKeyw="+numTSwithKeyw+
				" numTSwithKeywNoLeaf="+numTSwithKeywNoLeaf +" numCNs="+numCNs+" numBasicTS="+numBasicTS+"\n";
	}
	
}

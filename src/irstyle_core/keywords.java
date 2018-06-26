package irstyle_core;

import java.util.Random;

public class keywords {
	static Random ran = new Random(1);// );
	static int counter = 0;

	static int getRand(int min, int max) {

		// Random ran=new Random();
		int d = (min + Math.abs(ran.nextInt()) % (max - min + 1));
		return d;
	}

	public static String getRandomKeyword() {
		// return words[getRand(0,words.length-1)];

		return words[(counter++) % words.length];
	}

	public static final String[] words = /*
											 * { "Widom", "Ullman", "Yannis", "Abiteboul", "Hector", "Paepcke",
											 * "Jennifer", "Jeffrey", "System", "database"};
											 * 
											 * 
											 * 
											 * 
											 */
			/*
			 * "Christos", "Mining", "Warehouse", "Clusters ", "Pirahesh", "Hector ",
			 * "Yannis ", "Neighbor", "Hellerstein", "Adaptive", "Jennifer", "Combining",
			 * "Replicated", "Roussopoulos", "Linear ", "Indexing", "Aggregate",
			 * "Concurrency", "Papadias", "Srivastava", "Ordering", "Projected",
			 * "Shanmugasundaram", "Protocols", "Association", "Optimistic", "Surajit ",
			 * "SIGMOD", "VLDB", "ICDE", "Estimation", "Integration", "Heterogeneous",
			 * "Florescu", "Shrinking", "Nick ", "Semistructured", "Mediators", "Record",
			 * "Language", "Faloutsos", "Widom" };
			 */

			{ "1975", "1978", "1979", "1979", "1983", "1985", "1985", "1986", "1987", "1988", "1988", "1990", "1990",
					"1991", "1991", "1992", "1992", "1994", "1994", "1995", "1995", "1995", "1996", "1996", "1996",
					"1997", "1997", "1998", "1999", "1999", "1999", "1999", "1999", "1999", "2000", "2000", "2000",
					"2000", "Advances", "AI-Programme", "Albert", "Anwendung", "Areas", "Artificial", "Artificial",
					"Assumptions", "Aware", "Bamm�", "Bandyopadhyay", "Based", "Beitrag.", "Belief",
					"Blackboard-System", "Boyle", "Capurro", "Cognition", "Cognition.", "Coherent", "Complexity",
					"Conf.", "Conference", "Configuration-Driven", "Connectionist", "Contour", "Cordeschi", "Database",
					"Deffner", "deklarative", "Development", "Diagnosis", "Diagnostic", "Eder", "Effective", "Encoding",
					"Entwerfen", "Entwicklungsmethodik", "Equational", "Erfahrungen", "Erzeugung", "Explanation-Based",
					"Extending", "Feature", "Flohr", "Franz", "Frey", "Friedrich", "Friedrich", "FSE", "G.", "g.", "g.",
					"Geiger", "Generalization", "Generalized", "Generation", "Generierung", "Gerhard", "Gerhard",
					"Guiseppe", "Hamilton", "Hans", "Heideggers", "Helm", "Herz", "HESDE:", "Heuristics", "High-Level",
					"Holger", "Implementing", "Intelligente", "Intelligenz", "Intelligenz", "Intelligenz.",
					"Interaction.", "Internet", "Issues", "John", "Kanaan", "Karl", "Karl", "Karl", "Kirchmeyer",
					"Klaus", "Klaus", "Knowledge-Based", "Knut", "Koh�renz", "K�hle", "Konnektionismus", "K�nnen",
					"Krushanov", "K�hn", "K�nstliche", "K�nstliche", "Kunze", "Kurthen", "Language", "Learning",
					"Learning", "Learning", "Learning", "Lebeda", "Lees", "Leidlmair", "Leo", "Lernen", "Lernmethoden.",
					"Leufke", "lichen", "Limitations", "Limits", "logische", "Look-Ahead", "M.", "Manhart", "Mannes",
					"Marita", "Mark", "Martin", "Martina", "Meunier", "Michael", "mit", "mobile", "Model",
					"Model-Based", "Modelling", "M�ller", "Monika", "Nagele", "nen.", "neue", "neues", "Neumaier",
					"nterface.", "Ny�ri", "orks.", "Otto", "Outline", "Overcoming", "Parser.", "Philosophical",
					"Philosophie", "Problem.", "Process:", "Protocols", "Qualitative", "Radbruch", "Radical", "Rafael",
					"Rainer", "Reasoning", "Recent", "Rechner", "Rechnerunterst�tztes", "RECOMB", "Reflection",
					"Regine", "Reimer", "Relations", "Remarks", "Renate", "Representations", "Requirements", "REX",
					"REX", "RIDE", "RIDE-ADS", "RIDE-DM", "RIDE-DOM", "RIDE-IMS", "RIDE-NDS", "RIDE-TQP", "rks.",
					"Roberto", "RoboCup", "RobVis", "Rolf", "Rough", "RSFDGrC", "RTA", "RTDB", "Rules", "Ruxandra",
					"SAC", "SAC", "SAC", "SAFECOMP", "SAFIR:", "SAIG", "SARA", "SAS", "SBIA", "SC", "SCAI", "Scalable",
					"Scale-Space", "SCCC", "Scheiterer", "Schie�er", "Sch�nbauer", "School", "School/Symposium",
					"Schuette", "SCIE", "SCM", "SEAL", "SEBD", "Secure", "Security", "SEKE", "Selected", "Semantics",
					"Sensor", "Sensory-Motor", "Sequence", "Services", "Sets", "Shape", "SI3D", "SIGAda",
					"SIGBDP-SIGCPR", "SIGCOMM", "SIGDOC", "SIGFIDET", "SIGGRAPH", "SIGIR", "SIGMETRICS", "SIGMOD",
					"SIGMOD", "SIGMOD", "SIGMOD", "SIGMOD", "SIGPLAN", "SIGSOFT", "SIGUCCS", "Situations", "SLP",
					"SMILE", "SODA", "SOFSEM", "Soft", "Software", "Software", "Som", "Some", "Sorts", "SOSP",
					"Soziologische", "SPAA", "Spatial", "Spatial", "Spatio-Temporal", "Spatio-Temporal", "SPDP", "SPIN",
					"SPIRE", "SPIRE/CRIWG", "Sprachen", "SRDS", "SSD", "SSDBM", "SSPR/SPR", "SSR", "STACS", "Stadler",
					"Standfuss", "State", "Stellt", "Stephan", "STOC", "Stochastic", "Storage", "Structural",
					"Structure", "Structures", "Struktur", "Strukturierte", "Sulzer", "Summer", "SWAT", "SYBEN",
					"Syllable-Based", "Symposium", "Symposium", "Symposium", "Symposium", "Symposium", "Symposium",
					"Symposium", "Symposium", "Symposium", "Symposium", "Symposium", "System", "TABLEAUX", "TACAS",
					"TACS", "Tafill", "TAGT", "TAPD", "TAPSOFT", "TAPSOFT", "TAPSOFT", "Targeting", "Temporal",
					"Temporal", "Temporal", "Terminology", "Terminology", "Terminology", "the", "Theorem",
					"Theoretical", "Theorie", "Theory", "Thiopoulos", "Tim", "Tim", "TIME", "TLCA", "Topoi", "Towards",
					"TPHOLs", "Transactions", "Trautteur", "TREC", "TreDS", "Trends", "TRI-Ada", "TRI-Ada", "TRI-Ada",
					"TRI-Ada", "TSD", "TYPES", "Types", "Tzeng", "UIDIS", "UK", "UML", "UML", "und", "und", "und",
					"und", "Und", "unter", "USENIX", "USENIX", "USENIX", "USENIX", "USM", "Vaas", "Variant", "VDB",
					"VDM", "VDM", "VDM", "VISUAL", "VL", "VLDB", "VLDB", "VLSI", "Wallner", "Why", "Wiegand", "Wilkens",
					"Wissensbasiertes", "Wissenserhebung", "Wissenserwerb", "Wolfhard", "Workshop", "Workshop",
					"Workshop", "Workshop", "Workshop", "Workshop", "Zeitbeschr�nkte" };

	public keywords() {
	}
}
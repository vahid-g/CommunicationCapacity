"#CommunicationCapacity" 

CommunicationCapacity is a project about effectiveness of query answering over different representations of the same data. In this project, we are conducting experimental studies on effectiveness of answering MSN queries over freebase relational data. We submit the queries to different representations of the freebase data and report the results. 

We use MySQL az the database engine with Lucene on top of it as the query interface. We have coded the logic of the project in Java 1.7. For further information contact the authors.



Here is the parameters we use for retrieval:
// best params bm3 are 1, 0, 2
// best params bm4 are 0.2 0.1 0.06 0.65
// best params bm4, query4 0.18 0.03 0.03 0.76
// best params for bm5: 
		
 (best p10): [0.24113473, 0.6099291, 0.035460994, 0.10638298, 0.007092199]
 (best mrr): [0.21368337, 0.61097544, 0.028683655, 0.12170705, 0.02495045]
 (best map): [0.22838148, 0.6609311, 0.025199883, 0.070007816, 0.015479729]
 (best all): [0.2277332, 0.62727857, 0.029781511, 0.09936595, 0.015840793]
 


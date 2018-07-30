DSE Document MetaData Search using Tika and DSEFS
=================================================
DSEFS (DataStax Enterprise file system) is a fault-tolerant, general-purpose, distributed file system within DataStax Enterprise.DSEFS is similar to HDFS, but avoids the deployment complexity and single point of failure typical of HDFS. DSEFS is HDFS-compatible and is designed to work in place of HDFS in Spark and other systems. For more on DSEFS, see https://docs.datastax.com/en/dse/6.0/dse-dev/datastax_enterprise/analytics/dsefsTOC.html 

This demo will load all the documents in a directory into DSEFS while extracting the metadata for indexing into DSE Search. It will also look in the links.txt to search for links that it can index. If it finds a github repository, it will index the README.md file.


To run this project you will need to have a DataStax Enterprise instance running in SearchAnalytics mode with DSEFS enabled. You may need to change the 'min_free_space' when you enable DSEFS if running locally - https://docs.datastax.com/en/dse/6.0/dse-dev/datastax_enterprise/analytics/enablingDsefs.html

To create the schema, and the search core, run the following (you may need to change the contact points)

	mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaSetup" -DcontactPoints=localhost
	
To run the document loader, run the following with a file location of where you want to load files from. By default, it is will process all the files in the src/main/resources/files directory.
	
	mvn clean compile exec:java -Dexec.mainClass="com.datastax.tika.Main"  -DcontactPoints=localhost -DfileLocation=/tmp/files 

To query the data in DSE, use cqlsh and run

	select documentid, created_date, type, link from tika.metadata limit 10.
	
To query for a particular word in the document 

	select documentid, created_date, type, link from tika.metadata where solr_query = 'content:mobile' ;
	
You can also use the solr admin ui at 

	http://localhost:8983/solr
	
To access DSEFS, run 

	dse fs
	
Other DSEFS commands can be found at https://docs.datastax.com/en/dse/6.0/dse-dev/datastax_enterprise/analytics/commandsDsefs.html

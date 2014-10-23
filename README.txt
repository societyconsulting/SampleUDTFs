Compile Instructions:


There are two UDTF's here, StringUniqueID and WebMat. To compile one of the 
programs in IntelliJ, open up pom.xml and change the <mainClass> tag to 
whichever java file you want to compile. Then go into View -> Tool Windows ->
Maven Projects. Under Plugins -> compiler run compiler:compile and then under
assembly run assembly:single. This should create the jar file.

To run the UDTF in HIVE you can use the following commands

ADD JAR /<path to>/hive-extension-1.0-SNAPSHOT-jar-with-dependencies.jar;
CREATE TEMPORARY FUNCTION string_id AS 'com.spectralclustering.udtf.StringUniqueID';
SELECT string_id(*) from table_of_strings;

UDTFs:

UDTFS stand for User Defined Table Functions. There different from UDFs (User
Defined Functions) because they take in the entire table before spitting out
a new table. I used them for things like StringUnqiueID which turns a list of
strings into unique IDs since I had to keep an internal map to keep track of
which strings were mapped to which IDs.

Extra Info:

Here's some websites I used to help me.

http://dev.bizo.com/2010/07/extending-hive-with-custom-udtfs.html

https://cwiki.apache.org/confluence/display/Hive/DeveloperGuide+UDTF

This repository has a bunch of examples of UDAF's, UDF's and UDTF's
https://github.com/brndnmtthws/facebook-hive-udfs

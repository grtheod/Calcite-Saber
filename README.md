# Calcite-Saber
1) Add as external jars calcite-core 1.9.0, calcite-linq4j 1.9.0, calcite-core-tests 1.9.0, calcite-example-csv 1.9.0 
from http://search.maven.org/#search%7Cga%7C1%7Ccalcite. At the moment, it can't find the calcite-core 1.9.0 jar from
maven repositories.
2) Add saber-0.0.1-SNAPSHOT.jar from https://github.com/lsds/saber
3) Use the Tester.java and change the sql String in lines 53-54.

Orders Schema
-------------------------
timestamp(long) | orderid(int) | productid(int) | units(int) 



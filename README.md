# Calcite-Saber
1) Add saber-0.0.1-SNAPSHOT.jar from https://github.com/lsds/saber

2) Use the Tester.java and change the sql String in lines 55-61 (https://github.com/giwrgostheod/Calcite-Saber/blob/master/src/main/java/calcite/Tester.java#L55).

Orders Schema
-------------------------
rowtime(long)* | orderid(int) | productid(int) | units(int) | costumerid(int)

Products Schema
-------------------------
rowtime(long)* | productid(int) | description(int) 

Customers Schema
-------------------------
rowtime(long)* | customerid(int) | phone(long) 


*rowtime is declared as timestamp in Calcite and long in Saber

# Calcite-Saber
1) Add saber-0.0.1-SNAPSHOT.jar from https://github.com/lsds/saber

2) Use the Tester.java and change the sql String in lines 55-61 (https://github.com/giwrgostheod/Calcite-Saber/blob/master/src/main/java/calcite/Tester.java#L55).

Orders Schema
-------------------------
timestamp(long) | orderid(int) | productid(int) | units(int) | costumerid(int)

Products Schema
-------------------------
timestamp(long) | productid(int) | description(int) 

Customers Schema
-------------------------
timestamp(long) | customerid(int) | phone(int) 



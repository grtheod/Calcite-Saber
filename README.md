# Calcite-Saber
1) Add saber-0.0.1-SNAPSHOT.jar to External jars from /libs/saber/saber-snapshot/0.0.1

2) Use the Tester.java.

Orders Schema
-------------------------
rowtime(long)* | orderid(int) | productid(int) | units(int) | costumerid(int)

Products Schema
-------------------------
rowtime(long)* | productid(int) | description(int) | price(float)

Customers Schema
-------------------------
rowtime(long)* | customerid(int) | phone(long) 

Orders_Delivery Schema
-------------------------
rowtime(long)* | orderid(int) | date_reported(long) | delivery_status_code(int)

Payments Schema 
-------------------------
rowtime(long)* | costumerid(int) | payment_date(int) | amount(float)



*rowtime is declared as timestamp in Calcite and long in Saber

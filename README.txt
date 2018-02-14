We installed a mySQL server locally on our Windows 8.1 development pc, and connected to jbdc using the defualt url: 'localhost:3306'
We created a database for testing called 'test'
For testing we log into the database as the 'root' user.
Password should be stored in an environment variable named 'MYSQL_ROOT_PASSWORD'.
The data files Person.data & Order.data were copied to folder named 'data' at the projects root directory.
It is assumed that the 'test' database has been created. If Orders or Person table exists, they will be dropped at run-time and recreated.
We create an 'Orders' table instead of an 'Order' table, since 'Order' is a reserved SQL command word.
We used IntelliJ IDEA IDE for developement and testing.


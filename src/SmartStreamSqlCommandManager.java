import java.sql.*;

public class SmartStreamSqlCommandManager {

    SmartStreamDataBaseConnector ssConnector = null;
    ResultSet resultSet = null;
    Statement statement = null;

    final String[] personTableFields = {"person_id", "first_name", "last_name", "street", "city"};
    final String[] ordersTableFields = { "order_id", "order_no", "person_id", "first_name" };
    final String[][] dbTablePrimaryKeys = {{"Person", personTableFields[0]}, {"Orders", ordersTableFields[0]}};

    SmartStreamSqlCommandManager(String dataBaseName){
        ssConnector = new SmartStreamDataBaseConnector();
        ssConnector.connect(dataBaseName);
    }

    protected void dropTableIfExists(String tableName){
        String mySqlCommand = "SET FOREIGN_KEY_CHECKS = 0 ";
        executeSqlCommand( mySqlCommand );

        mySqlCommand = "DROP TABLE IF EXISTS " + tableName;
        executeSqlCommand( mySqlCommand );

        mySqlCommand = "SET FOREIGN_KEY_CHECKS = 1 ";
        executeSqlCommand( mySqlCommand );
    }

    protected void createPersonTable() {

        dropTableIfExists("Person");

        String mySqlCommand =
                "CREATE TABLE Person " +
                "(person_id INTEGER not NULL, " +
                "first_name VARCHAR(50) not NULL, " +
                "last_name VARCHAR(50) not NULL, " +
                "street VARCHAR(50) not NULL, " +
                "city VARCHAR(50) not NULL, " +
                "PRIMARY KEY ( person_id ))";
        executeSqlCommand( mySqlCommand );
    }

    protected void createOrdersTable() {

        dropTableIfExists("Orders");

        String mySqlCommand =
                "CREATE TABLE orders " +
                "(order_id INTEGER not NULL, " +
                "order_no VARCHAR(50) not NULL, " +
                "person_id INTEGER not NULL, " +
                "PRIMARY KEY ( order_id ), " +
                "FOREIGN KEY ( Person_id ) REFERENCES Person( Person_id ))";
        executeSqlCommand( mySqlCommand );
    }

    protected int importPersonDataFile() {
        int tableRowsImportedCount;

        String mySqlCommand =
                "LOAD DATA LOCAL INFILE './data/person.data' " +
                        "INTO TABLE Person " +
                        "FIELDS TERMINATED BY ',' " +
                        "LINES TERMINATED BY '\r\n' " +
                        "IGNORE 1 LINES " +
                        "(person_id,first_name,last_name,street,city)";

        tableRowsImportedCount = executeSqlCommand( mySqlCommand );
        deleteBlankRowsFromTable("Person");

        return tableRowsImportedCount;
    }

    protected void importOrderDataFile() {
        String mySqlCommand =
                "LOAD DATA LOCAL INFILE './data/order.data' " +
                        "INTO TABLE Orders " +
                        "FIELDS TERMINATED BY '|' " +
                        "LINES TERMINATED BY '\r\n' " +
                        "IGNORE 1 LINES " +
                        "(order_id,order_no,person_id)";

        executeSqlCommand( mySqlCommand );
        deleteBlankRowsFromTable("Orders");
    }

    // Remove empty record due to any blank row(s) in raw imported data text file
    protected int deleteBlankRowsFromPersonTable(){
        int tableRowsCount;
        String mySqlCommand = "DELETE FROM Person WHERE person_id = 0";

        tableRowsCount = executeSqlCommand( mySqlCommand );

        return tableRowsCount;
    }

    // Remove empty record due to any blank row(s) in raw imported data text file
    protected int deleteBlankRowsFromTable(String tableName){

        String primaryKey = getPrimaryKeyNameFromTable(tableName);
        String mySqlCommand = "DELETE FROM " + tableName + " WHERE " + primaryKey + " = 0";

        int tableRowsCount = executeSqlCommand( mySqlCommand );

        return tableRowsCount;
    }

    protected ResultSet getAllOrdersWithFirstNameOfCorrespondingPerson() {
        String mySqlQuery =
                "SELECT Orders.*, Person.first_name " +
                        "FROM Orders " +
                        "INNER JOIN Person " +
                        "ON Orders.person_id = Person.person_id";

        resultSet = executeSqlQuery( mySqlQuery );
        return resultSet;
    }

    protected ResultSet getPersonsWithAtLeastOneOrder() {
        String mySqlQuery =
                "SELECT * " +
                        "FROM Person " +
                        "WHERE EXISTS(" +
                        "SELECT NULL FROM Orders " +
                        "WHERE Orders.person_id = Person.person_id) ";

        resultSet = executeSqlQuery(mySqlQuery);
        return resultSet;
    }

    private String getPrimaryKeyNameFromTable(String tableName){
        boolean found = false;
        String primaryKey = "";
        for( int i = 0; !found && i < dbTablePrimaryKeys.length || !found; i++){

            if (dbTablePrimaryKeys[i][0].equals(tableName)){
                primaryKey = dbTablePrimaryKeys[i][1];
                found = true;
            }
        }
        return primaryKey;
    }

    protected int tableRowsCount(String tableName){
        int tableRowsCount = -1;

        String mySqlQuery = "SELECT COUNT(*) FROM " + tableName;
        resultSet = executeSqlQuery( mySqlQuery );

        try{
            resultSet.next();
            tableRowsCount = resultSet.getInt(1);
        } catch (SQLException e){
            e.printStackTrace();
        }

        return tableRowsCount;
    }

    protected boolean isTable(String tableName){
        boolean tableCreated = false;

        try {
            DatabaseMetaData dbm = ssConnector.getConnection().getMetaData();
            resultSet = dbm.getTables(null, null, tableName, null);

            if ( resultSet.next() ) {
                tableCreated = true;
            }
        } catch (SQLException se){
            se.printStackTrace();
        }

        return tableCreated;
    }

    private int executeSqlCommand(String mySqlCommand) {
        int result = -1;
        try {
            statement = ssConnector.getConnection().createStatement();
            result = statement.executeUpdate( mySqlCommand );
        }
        catch (SQLException se) {
            se.printStackTrace();
        }
        return result;
    }

    private ResultSet executeSqlQuery(String query ) {
        try {
            statement = ssConnector.getConnection().createStatement();
            resultSet = statement.executeQuery( query );
        }
        catch ( SQLException err ) {
            System.out.println( "SQLException: " + err.getMessage() ) ;
        }
        return resultSet;
    }

    protected void closeDbResources(){
        try { if (resultSet != null) resultSet.close(); } catch (Exception e) {};
        try { if (statement != null) statement.close(); } catch (Exception e) {};
        try { if (ssConnector.getConnection() != null) ssConnector.disConnect(); } catch (Exception e) {};
    }

    // Not currently used
    private void createDataBase(String dbName){
        String mySqlCommand = "CREATE DATABASE " + dbName;
        executeSqlCommand( mySqlCommand );
    }
}
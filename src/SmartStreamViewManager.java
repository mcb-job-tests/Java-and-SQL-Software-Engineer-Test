import java.sql.ResultSet;
import java.sql.SQLException;

public class SmartStreamViewManager {
    SmartStreamSqlCommandManager sqlCommandManager;
    SmartStreamViewManager(SmartStreamSqlCommandManager sqlCommandManager){
        this.sqlCommandManager = sqlCommandManager;
    }

    protected void printPersonsWithAtLeastOneOrder(){
        System.out.println("*** Persons With At Least One Order ***");
        String[] tableFields = sqlCommandManager.personTableFields;
        ResultSet rs = sqlCommandManager.getPersonsWithAtLeastOneOrder();
        displayDbQueryResult(rs, tableFields);
        System.out.println();
    }

    protected void printAllOrdersWithFirstNameOfCorrespondingPerson(){
        System.out.println( "*** All Orders With First Name Of Corresponding Person ***" );
        String[] tableFields = sqlCommandManager.ordersTableFields;
        ResultSet rs = sqlCommandManager.getAllOrdersWithFirstNameOfCorrespondingPerson();
        displayDbQueryResult( rs, tableFields );
        System.out.println();
    }

    private void printHeader(String[] tableFields){
        String tableHeader = "";

        for ( int i = 0; i < tableFields.length; i++ ) {
            tableHeader += tableFields[i];
            if (i != (tableFields.length - 1)) {
                tableHeader += " , ";
            }
        }
        System.out.println( tableHeader );
    }

    private void displayDbQueryResult( ResultSet rs, String[] tableFields ) {
        String result; // Type can be int, String, … etc
        printHeader( tableFields );
        try {
            while ( rs.next() ) {
                result ="";
                for ( int i = 0; i < tableFields.length; i++ ) {
                    result += rs.getString( tableFields[i] );
                    if (i != (tableFields.length - 1)) {
                        result += " , ";
                    }
                }
                System.out.println( result );
            }
        }
        catch ( SQLException err ) {
            System.out.println( "SQLException: " + err.getMessage() ) ;
        }
    }
}

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SmartStreamDataBaseConnector {

    private Connection connection;

    private String url;
    private String username;
    private String password;
    private String settings;

    SmartStreamDataBaseConnector(){
        url = "jdbc:mysql://localhost:3306/";
        username = "root";
        password = System.getenv("MYSQL_ROOT_PASSWORD");
        settings = "?autoReconnect=true&useSSL=false";
    }

    SmartStreamDataBaseConnector(String url, String settings, String username, String password){
        this.url = url;
        this.settings = settings;
        this.username  = username;
        this.password = password;
    }

    protected boolean isDataBase(String dbName){
        boolean isDb = false;
        ResultSet resultSet = null;
        try{
            resultSet = connection.getMetaData().getCatalogs();

            while (resultSet.next() && isDb == false) {
                String databaseName = resultSet.getString(1);
                if (databaseName.equals(dbName)){
                    isDb = true;
                }
            }
        } catch (Exception err){
                err.printStackTrace();
        } finally {
            try { if (resultSet != null)
                resultSet.close();
            } catch (Exception err) {
                err.printStackTrace();
            }
        }
        return isDb;
    }

    protected void connect(String dbName){
        try {
            connection = DriverManager.getConnection(url + dbName + settings, username, password);
        }
        catch (SQLException err) {
            System.out.println("SQLException: " + dbName + " database > " + err.getMessage()) ;
            connection = null;
        }
    }

    protected void disConnect() {
        try {
            connection.close();
        }
        catch (SQLException se) {
            se.printStackTrace();
        }
    }

    protected boolean isClosed(){
        boolean isClosed = true;
        try{
            isClosed = connection.isClosed();
        }
        catch (SQLException se){
            se.printStackTrace();
        }
        return isClosed;
    }

    protected Connection getConnection(){
        return connection;
    }
}
public class SmartStreamChallenge {

    SmartStreamSqlCommandManager sqlCommandManager;
    SmartStreamViewManager ssViewManager;

    final String dataBaseName = "test";

    public static void main( String[] args ) {
        SmartStreamChallenge sStream = new SmartStreamChallenge();

        sStream.sqlCommandManager.createPersonTable();
        sStream.sqlCommandManager.importPersonDataFile();

        sStream.sqlCommandManager.createOrdersTable();
        sStream.sqlCommandManager.importOrderDataFile();

        sStream.ssViewManager.printPersonsWithAtLeastOneOrder();
        sStream.ssViewManager.printAllOrdersWithFirstNameOfCorrespondingPerson();

        sStream.sqlCommandManager.closeDbResources();
    }

    SmartStreamChallenge() {
        sqlCommandManager = new SmartStreamSqlCommandManager(dataBaseName);
        ssViewManager = new SmartStreamViewManager(sqlCommandManager);
    }
}

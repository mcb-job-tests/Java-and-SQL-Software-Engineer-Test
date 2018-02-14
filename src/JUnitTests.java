import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JUnitTests {

    @Test
    void smartStreamChallengeTester(){
        SmartStreamChallenge smartStream = new SmartStreamChallenge();
        smartStream.sqlCommandManager.ssConnector.connect("wut");
        assertNull(smartStream.sqlCommandManager.ssConnector.getConnection());

        smartStream.sqlCommandManager.ssConnector.connect("test");
        assertNotNull(smartStream.sqlCommandManager.ssConnector.getConnection());
        assertFalse(smartStream.sqlCommandManager.ssConnector.isClosed());
        assertFalse(smartStream.sqlCommandManager.isTable("Person"));

        smartStream.sqlCommandManager.createPersonTable();
        assertTrue(smartStream.sqlCommandManager.isTable("Person"));

        int tableRowsInsertedCount = smartStream.sqlCommandManager.importPersonDataFile();
        assertEquals(4, tableRowsInsertedCount);

        int tableRowsDeletedCount = smartStream.sqlCommandManager.deleteBlankRowsFromPersonTable();
        assertEquals(1, tableRowsDeletedCount);

        int tableRowsCount = smartStream.sqlCommandManager.tableRowsCount("Person");
        assertEquals(3, tableRowsCount);

        smartStream.sqlCommandManager.createOrdersTable();
        assertFalse(smartStream.sqlCommandManager.isTable("Order"));
        assertTrue(smartStream.sqlCommandManager.isTable("Orders"));

        smartStream.sqlCommandManager.closeDbResources();
        assertTrue(smartStream.sqlCommandManager.ssConnector.isClosed());

    }
}
package ru.fizteh.fivt.students.semenenko_denis.JUnit;

import org.junit.*;
import org.junit.rules.TemporaryFolder;
import ru.fizteh.fivt.storage.strings.Table;
import ru.fizteh.fivt.storage.strings.TableProvider;
import ru.fizteh.fivt.students.semenenko_denis.MultiFileHashMap.DatabaseFactory;

import java.io.File;
import java.io.IOException;


public class DatabaseTest {
    @ClassRule
    public static TemporaryFolder folder = new TemporaryFolder();
    private static File dbPath;
    private static TableProvider database;

    @BeforeClass
    public static void setUpClass() throws IOException {
        database = new DatabaseFactory().create(folder.newFolder().getAbsolutePath().toString());
    }

    @Test
    public void testGetAndCreateTable() {
        Assert.assertEquals(null, database.getTable("abc"));
        Table table = database.createTable("abc");
        table.put("a", "a");
        table.commit();
        table = database.getTable("abc");
        Assert.assertEquals(1, table.size());
        Assert.assertEquals(null, database.getTable("abcd"));
        Assert.assertEquals(null, database.getTable("ab"));
        Assert.assertEquals(null, database.getTable("a"));
        Assert.assertEquals(null, database.getTable("qwerty"));
        database.removeTable("abc");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExceptionThrowsThanDropNotExistingTable() {
        database.removeTable("notExist");
    }

    @Test
    public void testCanRemoveTable() {
        Table table = database.createTable("name");
        table.put("a", "a");
        table.commit();
        database.removeTable("name");
        Assert.assertEquals(null, database.getTable("name"));
    }

    @Test
    public void testCanRemoveTableWithNoCommits() {
        Table table = database.createTable("name");
        database.removeTable("name");
        Assert.assertEquals(null, database.getTable("name"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateTableThrowsExceptionThanArgumentIsNull() {
        database.createTable(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRemoveTableThrowsExceptionThanArgumentIsNull() {
        database.removeTable(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetTableThrowsExceptionThanArgumentIsNull() {
        database.getTable(null);
    }

    @Test
    public void testReturnNullWhenTableExist() {
        database.createTable("qwerty");
        Assert.assertEquals(null, database.createTable("qwerty"));
        database.removeTable("qwerty");
    }

    private void assertCanGetTableTNum(int i) {
        Assert.assertEquals("t" + i, database.getTable("t" + i).getName());
    }
}



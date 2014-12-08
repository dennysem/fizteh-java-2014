package ru.fizteh.fivt.students.semenenko_denis.Storeable.Test;

import org.junit.*;
import org.junit.rules.TemporaryFolder;
import ru.fizteh.fivt.storage.structured.ColumnFormatException;
import ru.fizteh.fivt.storage.structured.Storeable;
import ru.fizteh.fivt.storage.structured.Table;
import ru.fizteh.fivt.students.semenenko_denis.MultiFileHashMap.TableHash;
import ru.fizteh.fivt.students.semenenko_denis.Storeable.Database;
import ru.fizteh.fivt.students.semenenko_denis.Storeable.DatabaseFactory;
import ru.fizteh.fivt.students.semenenko_denis.Storeable.StorableClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;


public class TableHashTest {
    private static DatabaseFactory factory;
    private static Database provider;
    private static Table table;
    private static String tableName = "table";
    private static List<Class<?>> signature;
    private Storeable storeable;
    private Storeable storeable2;
    private Storeable storeable3;
    private Storeable storeable4;

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @BeforeClass
    public static void beforeClass() {
        factory = new DatabaseFactory();

        signature = new ArrayList<>();
        signature.add(String.class);
    }

    @Before
    public void before() throws IOException {
        provider = (Database) factory.create(tmpFolder.newFolder().getAbsolutePath().toString());
        table = provider.createTable(tableName, signature);

        storeable = provider.createFor(table);
        storeable2 = provider.createFor(table);
        storeable3 = provider.createFor(table);
        storeable4 = provider.createFor(table);
    }

    @After
    public void after() throws IOException {
        provider.createTable(tableName, signature);
        provider.removeTable(tableName);
    }

    @Test
    public void readWrite() throws IOException {
        storeable.setColumnAt(0, "value");
        table.put("key", storeable);
        storeable2.setColumnAt(0, "значение");
        table.put("ключ", storeable2);
        table.commit();
        Table newTable = provider.getTable("table");
        Assert.assertEquals("value", newTable.get("key").getStringAt(0));
        Assert.assertEquals("значение", newTable.get("ключ").getStringAt(0));
    }

    @Test
    public void getName() {
        Assert.assertEquals("table", table.getName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void getNull() {
        table.get(null);
    }

    @Test
    public void getNotExisting() {
        Assert.assertNull(table.get("key"));
    }

    @Test
    public void getExisting() {
        storeable.setColumnAt(0, "value");
        table.put("key", storeable);
        Assert.assertEquals(table.get("key").getStringAt(0), "value");
    }

    @Test
    public void putExisting() {
        storeable.setColumnAt(0, "old_value");
        table.put("key", storeable);
        storeable2.setColumnAt(0, "new_value");
        Assert.assertEquals("old_value", table.put("key", storeable2).getStringAt(0));
    }

    @Test(expected = ColumnFormatException.class)
    public void putInvalidColumnFormat() {
        List<Class<?>> signature = new ArrayList<>();
        signature.add(Integer.class);
        StorableClass incorrectStoreable = new StorableClass(signature);
        incorrectStoreable.setColumnAt(0, 5);
        table.put("key", incorrectStoreable);
    }

    @Test(expected = IllegalArgumentException.class)
    public void removeNull() {
        table.remove(null);
    }

    @Test
    public void removeNotExisting() {
        Assert.assertNull(table.remove("key"));
    }

    @Test
    public void removeExisting() {
        storeable.setColumnAt(0, "value");
        table.put("key", storeable);
        Assert.assertEquals("value", table.remove("key").getStringAt(0));
    }

    @Test
    public void size() {
        Assert.assertEquals(0, table.size());
        storeable.setColumnAt(0, "1");
        table.put("1", storeable);
        storeable.setColumnAt(0, "2");
        table.put("2", storeable);
        Assert.assertEquals(2, table.size());
        storeable.setColumnAt(0, "3");
        table.put("1", storeable);
        Assert.assertEquals(2, table.size());
        table.remove("1");
        Assert.assertEquals(1, table.size());
    }

    @Test
    public void commitNothing() throws IOException {
        Assert.assertEquals(0, table.commit());
    }

    @Test
    public void commit() throws IOException {
        storeable.setColumnAt(0, "1");
        table.put("1", storeable);
        storeable.setColumnAt(0, "2");
        table.put("2", storeable);
        table.size();
        table.remove("2");
        table.remove("tt");
        Assert.assertEquals(1, table.commit());
    }

    @Test
    public void rollback() throws IOException {
        storeable.setColumnAt(0, "1");
        table.put("1", storeable);
        storeable2.setColumnAt(0, "2");
        table.put("2", storeable2);
        Assert.assertEquals(2, table.commit());
        storeable3.setColumnAt(0, "3");
        table.put("2", storeable3);
        storeable4.setColumnAt(0, "4");
        table.put("4", storeable4);
        Assert.assertEquals(2, table.rollback());
        Assert.assertNull(table.get("4"));
        Assert.assertEquals("2", table.get("2").getStringAt(0));
    }

    @Test
    public void getColumnsCount() {
        Assert.assertEquals(table.getColumnsCount(), 1);
    }

    @Test
    public void getColumnType() throws IOException {
        List<Class<?>> fullSignature = new ArrayList<>();
        fullSignature.add(Integer.class);
        fullSignature.add(Long.class);
        fullSignature.add(Byte.class);
        fullSignature.add(Float.class);
        fullSignature.add(Double.class);
        fullSignature.add(Boolean.class);
        fullSignature.add(String.class);

        Table fullTable =
                provider.createTable("fullTable", fullSignature);
        Assert.assertEquals(Integer.class, fullTable.getColumnType(0));
        Assert.assertEquals(Long.class, fullTable.getColumnType(1));
        Assert.assertEquals(Byte.class, fullTable.getColumnType(2));
        Assert.assertEquals(Float.class, fullTable.getColumnType(3));
        Assert.assertEquals(Double.class, fullTable.getColumnType(4));
        Assert.assertEquals(Boolean.class,  fullTable.getColumnType(5));
        Assert.assertEquals(String.class, fullTable.getColumnType(6));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void getColumnTypeOutOfBounds() {
        table.getColumnType(1);
    }

}

package ru.fizteh.fivt.students.semenenko_denis.Storeable.Test;

import org.junit.*;
import org.junit.rules.TemporaryFolder;
import ru.fizteh.fivt.storage.structured.ColumnFormatException;
import ru.fizteh.fivt.storage.structured.Storeable;
import ru.fizteh.fivt.storage.structured.Table;
import ru.fizteh.fivt.students.semenenko_denis.Storeable.Database;
import ru.fizteh.fivt.students.semenenko_denis.Storeable.DatabaseFactory;
import ru.fizteh.fivt.students.semenenko_denis.Storeable.StorableClass;
import ru.fizteh.fivt.students.semenenko_denis.Storeable.TableHash;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class DatabaseTest {

    private static DatabaseFactory factory;
    private static Database provider;
    private static List<Class<?>> fullSignature;
    private static List<Class<?>> invalidSignature;
    private static List<Object> valuesList;
    private static List<Object> invalidValues;
    private static String correctSerialized;
    private static String nullSerialized;

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @BeforeClass
    public static void beforeClass() {
        factory = new DatabaseFactory();


        fullSignature = new ArrayList<>();
        fullSignature.add(Integer.class);
        fullSignature.add(Long.class);
        fullSignature.add(Byte.class);
        fullSignature.add(Float.class);
        fullSignature.add(Double.class);
        fullSignature.add(Boolean.class);
        fullSignature.add(String.class);

        invalidSignature = new ArrayList<>();
        invalidSignature.add(Character.class);

        valuesList = new ArrayList<>();
        valuesList.add(1);
        valuesList.add((long) 2);
        valuesList.add((byte) 3);
        valuesList.add((float) 4);
        valuesList.add((double) 5);
        valuesList.add(true);
        valuesList.add("six");

        invalidValues = new ArrayList<>();
        invalidValues.add(1);
        invalidValues.add((long) 2);
        invalidValues.add((byte) 3);
        invalidValues.add((float)4.0);
        invalidValues.add((double) 5.0);
        invalidValues.add(true);
        invalidValues.add(7);

        correctSerialized = "[1,2,3,4.0,5.0,true,\"six\"]";
        nullSerialized = "[null,null,null,null,null,null,null]";
    }

    @Before
    public void before() throws IOException {
        provider = (Database) factory.create(tmpFolder.newFolder().getAbsolutePath().toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void createNull() throws IllegalArgumentException, IOException {
        provider.createTable(null, fullSignature);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createInvalidNameDoublePoint() throws IOException {
        provider.createTable("..", fullSignature);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createInvalidNameSlash() throws IOException {
        provider.createTable("gg" + File.separator + "fd", fullSignature);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createNullSignature() throws IOException {
        provider.createTable("table", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createInvalidSignature() throws IOException {
        provider.createTable("table", invalidSignature);
    }

    @Test
    public void createNotExisting() throws IOException {
        Assert.assertNotNull(provider.createTable("tableNotExisting", fullSignature));
    }

    @Test
    public void createExisting() throws IOException {
        provider.createTable("table", fullSignature);
        Assert.assertNull(provider.createTable("table", fullSignature));
    }

    @Test(expected = IllegalArgumentException.class)
    public void removeNull() throws IOException {
        provider.removeTable(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void removeInvalidNameDoublePoint() throws IOException {
        provider.removeTable("..");
    }

    @Test(expected = IllegalArgumentException.class)
    public void removeInvalidNamePoint() throws IOException {
        provider.removeTable(".");
    }

    @Test(expected = IllegalArgumentException.class)
    public void removeInvalidNameSlash() throws IOException {
        provider.removeTable("gg" + File.separator + "fd");
    }

    @Test(expected = IllegalStateException.class)
    public void removeNotExisting() throws IOException {
        provider.removeTable("table");
    }

    @Test
    public void removeExisting() throws IOException {
        provider.createTable("table", fullSignature);
        provider.removeTable("table");
    }

    @Test(expected = IllegalArgumentException.class)
    public void getNull() {
        provider.getTable(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getInvalidNameDoublePoint() {
        provider.getTable("..");
    }

    @Test(expected = IllegalArgumentException.class)
    public void getInvalidNamePoint() {
        provider.getTable(".");
    }

    @Test(expected = IllegalArgumentException.class)
    public void getInvalidNameSlash() {
        provider.getTable("gg" + File.separator + "fd");
    }

    @Test
    public void getNotExisting() {
        Assert.assertNull(provider.getTable("table"));
    }

    @Test
    public void getExisting() throws IOException {
        provider.createTable("table", fullSignature);
        Assert.assertNotNull(provider.getTable("table"));
    }

    @Test
    public void get() throws IOException {
        provider.createTable("table", fullSignature);
        provider.createTable("table2", fullSignature);
        provider.removeTable("table");
        Assert.assertNull(provider.getTable("table"));
        Assert.assertNotNull(provider.getTable("table2"));
    }

    @Test
    public void emptyCreateFor() throws IOException {
        Table table =
                provider.createTable("table", fullSignature);
        Storeable storeable = provider.createFor(table);
        storeable.getIntAt(0);
        storeable.getLongAt(1);
        storeable.getByteAt(2);
        storeable.getFloatAt(3);
        storeable.getDoubleAt(4);
        storeable.getBooleanAt(5);
        storeable.getStringAt(6);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void emptyCreateForOutOfBounds() throws IOException {
        TableHash table =
                (TableHash) provider.createTable("table", fullSignature);
        Storeable storeable = provider.createFor(table);
        storeable.getColumnAt(7);
    }

    @Test
    public void createFor() throws IOException {
        Table table =
                provider.createTable("table", fullSignature);
        Storeable storeable = provider.createFor(table, valuesList);
        Assert.assertEquals(valuesList.get(0), storeable.getIntAt(0));
        Assert.assertEquals(valuesList.get(1), storeable.getLongAt(1));
        Assert.assertEquals(valuesList.get(2), storeable.getByteAt(2));
        Assert.assertEquals(valuesList.get(3), storeable.getFloatAt(3));
        Assert.assertEquals(valuesList.get(4), storeable.getDoubleAt(4));
        Assert.assertEquals(valuesList.get(5), storeable.getBooleanAt(5));
        Assert.assertEquals(valuesList.get(6), storeable.getStringAt(6));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void createForOutOfBounds() throws IOException {
        Table table =
                provider.createTable("table", fullSignature);
        Storeable storeable = provider.createFor(table, valuesList);
        storeable.getColumnAt(7);
    }

    @Test(expected = ColumnFormatException.class)
    public void createForInvalidColumnFormat() throws IOException {
        Table table =
                provider.createTable("table", fullSignature);
        provider.createFor(table, invalidValues);
    }

    @Test
    public void serialize() throws IOException {
        Table table =
                provider.createTable("table", fullSignature);
        Storeable storeable = provider.createFor(table, valuesList);
        String serialized = provider.serialize(table, storeable);
        Assert.assertEquals(serialized, correctSerialized);
    }

    @Test
    public void serializeNullValues() throws IOException {
        Table table =
                provider.createTable("table", fullSignature);
        Storeable storeable = provider.createFor(table);
        String serialized = provider.serialize(table, storeable);
        Assert.assertEquals(serialized, nullSerialized);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void serializeSmallerSize() throws IOException {
        Table table =
                provider.createTable("table", fullSignature);
        List<Class<?>> invalidList = new ArrayList<>();
        for (int columnNumber = 0; columnNumber + 1 < fullSignature.size();
             columnNumber++) {
            invalidList.add(fullSignature.get(columnNumber));
        }
        Table invalidTable =
                provider.createTable("table2", invalidList);
        Storeable storeable = provider.createFor(invalidTable);
        provider.serialize(table, storeable);
    }

    @Test(expected = ColumnFormatException.class)
    public void serializedInvalidColumnFormat() throws IOException {
        Table table =
                provider.createTable("table", fullSignature);

        List<Class<?>> invalidSignature = new ArrayList<>(fullSignature);
        invalidSignature.remove(invalidSignature.size() - 1);
        invalidSignature.add(Integer.class);
        Table invalidTable =
                provider.createTable("table2", invalidSignature);

        Storeable storeable = provider.createFor(invalidTable, invalidValues);
        provider.serialize(table, storeable);
    }

}

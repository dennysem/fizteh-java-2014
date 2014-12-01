package ru.fizteh.fivt.students.semenenko_denis.Storeable;

/**
 * Created by denny_000 on 01.12.2014.
 */

import ru.fizteh.fivt.storage.strings.Table;
import ru.fizteh.fivt.storage.structured.ColumnFormatException;
import ru.fizteh.fivt.storage.structured.Storeable;
import ru.fizteh.fivt.students.semenenko_denis.MultiFileHashMap.*;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.*;

public class Database implements ru.fizteh.fivt.storage.structured.TableProvider {
    private final String signatureFileName = "signature.tsv";

    private Map<Class<?>, String> classNames = new HashMap<>();
    private Map<String, Class<?>> revClassNames;
    private String dbDirPath;
    private Map<String, TableHash> tables;

    private List<Class<?>> readSignature(File signatureFile)
            throws IOException {
        try (Scanner scanner = new Scanner(signatureFile)) {
            List<Class<?>> signature = new ArrayList<>();
            while (scanner.hasNext()) {
                String str = scanner.next();
                signature.add(revClassNames.get(str));
            }
            return signature;
        }
    }

    private void writeSignature(File signatureFile, List<Class<?>> signature)
            throws IOException {
        try (PrintWriter writer = new PrintWriter(signatureFile)) {
            for (Class<?> cl : signature) {
                writer.print(classNames.get(cl) + " ");
            }
        }
    }

    private void checkFormat(Table table, Storeable storeable)
            throws IndexOutOfBoundsException, ColumnFormatException {
        for (int columnNumber = 0; columnNumber < table.getColumnsCount();
             columnNumber++) {
            Object value = storeable.getColumnAt(columnNumber);
            if (value != null
                    && !value.getClass().equals(table.getColumnType(columnNumber))) {
                throw new ColumnFormatException("Invalid storeable format");
            }
        }
    }

    private void initClassNames() {
        classNames.put(Integer.class, "int");
        classNames.put(Long.class, "long");
        classNames.put(Byte.class, "byte");
        classNames.put(Float.class, "float");
        classNames.put(Double.class, "double");
        classNames.put(Float.class, "float");
        classNames.put(Boolean.class, "boolean");
        classNames.put(String.class, "String");

        revClassNames = new HashMap<>();
        for (Class<?> cl : classNames.keySet()) {
            String name = classNames.get(cl);
            revClassNames.put(name, cl);
        }
    }

    private void initProvider() throws IOException {
        File dbDir = new File(dbDirPath);
        for (File curDir : dbDir.listFiles()) {
            if (curDir.isDirectory()) {
                File signatureFile = new File(
                        curDir.getAbsolutePath() + File.separator + signatureFileName);
                List<Class<?>> signature = readSignature(signatureFile);
                Table table = new TableHash(
                        this, curDir.getName(), dbDirPath, signature);
                tables.put(curDir.getName(), table);
            } else {
                throw new IOException("Directory contains incorrect files");
            }
        }
    }

    public boolean contains(String name) {
        return tables.containsKey(name);
    }

    public Map<String, TableHash> getAllTables() {
        return tables;
    }

    @Override
    public ru.fizteh.fivt.storage.structured.Table getTable(String name) {
        if (tables.containsKey(name)) {
            return tables.get(name);
        } else {
            return null;
        }
    }

    @Override
    public ru.fizteh.fivt.storage.structured.Table createTable(String name, List<Class<?>> columnTypes) throws IOException {
        if (columnTypes == null) {
            throw new IllegalArgumentException("Signature can't be null");
        }
        try {
            new StorableClass(columnTypes);
        } catch (ColumnFormatException e) {
            throw new IllegalArgumentException(e);
        }
        if (tables.containsKey(name)) {
            return null;
        } else {
            String tableDirPath = dbDirPath + File.separator + name;
            File tableDir = new File(tableDirPath);
            if (!tableDir.mkdir()) {
                throw new IOException("Can't create this table");
            }

            File signatureFile = new File(tableDirPath
                    + File.separator + signatureFileName);
            writeSignature(signatureFile, columnTypes);
            TableHash table = new TableHash(this, name, dbDirPath, columnTypes);
            tables.put(name, table);
            return table;
        }
    }

    @Override
    public void removeTable(String name) throws IOException {
        if (tables.containsKey(name)) {
            TableHash table = tables.get(name);
            table.removeFromDisk();
            String tablePath = dbDirPath + File.separator + name;
            File tableDir = new File(tablePath);
            String signaturePath = tablePath + File.separator + signatureFileName;
            File signatureFile = new File(signaturePath);
            if (!signatureFile.delete() || !tableDir.delete()) {
                throw new IOException("Can't remove table");
            }
            tables.remove(name);
        } else {
            throw new IllegalStateException("Table doesn't exist");
        }
    }

    @Override
    public Storeable deserialize(ru.fizteh.fivt.storage.structured.Table table, String value) throws ParseException {
        return null;
    }

    @Override
    public String serialize(ru.fizteh.fivt.storage.structured.Table table, Storeable value)
            throws ColumnFormatException {
        if (value == null) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        int size = table.getColumnsCount();
        builder.append('[');
        Object obj;
        for (int i = 0; i < size; i++) {
            obj = value.getColumnAt(i);
            if (obj != null && obj.getClass().equals(String.class)) {
                builder.append('"');
            }
            builder.append(obj);
            if (obj != null && obj.getClass().equals(String.class)) {
                builder.append('"');
            }
            if (i != size - 1) {
                builder.append(", ");
            }
        }
        builder.append(']');
        return builder.toString();
    }

    @Override
    public Storeable createFor(ru.fizteh.fivt.storage.structured.Table table) {
        List<Class<?>> signature = new ArrayList<>();
        for (int columnNumber = 0; columnNumber < table.getColumnsCount();
             columnNumber++) {
            signature.add(table.getColumnType(columnNumber));
        }
        return new StorableClass(signature);
    }

    @Override
    public Storeable createFor(ru.fizteh.fivt.storage.structured.Table table, List<?> values)
            throws ColumnFormatException, IndexOutOfBoundsException {
        if (values.size() != table.getColumnsCount()) {
            throw new IndexOutOfBoundsException("Invalid number of values");
        }
        StorableClass res = (StorableClass) (table);
        for (int columnNumber = 0; columnNumber < values.size(); columnNumber++) {
            if (!table.getColumnType(columnNumber).equals(
                    values.get(columnNumber).getClass())) {
                throw new ColumnFormatException("Invalid values format");
            }
            res.setColumnAt(columnNumber, values.get(columnNumber));
        }
        return res;
    }

    public Path getDirectory() {
        return;
    }
}

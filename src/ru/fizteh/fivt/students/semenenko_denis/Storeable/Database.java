package ru.fizteh.fivt.students.semenenko_denis.Storeable;

/**
 * Created by denny_000 on 01.12.2014.
 */

import javafx.util.Pair;
import ru.fizteh.fivt.storage.structured.ColumnFormatException;
import ru.fizteh.fivt.storage.structured.Storeable;
import ru.fizteh.fivt.storage.structured.Table;
import ru.fizteh.fivt.storage.structured.TableProvider;
import ru.fizteh.fivt.students.semenenko_denis.MultiFileHashMap.*;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.*;

public class Database implements TableProvider {
    private final String signatureFileName = "signature.tsv";

    private Map<Class<?>, String> classNames = new HashMap<>();
    private Map<String, Class<?>> revClassNames;
    private String rootDirectory;
    private Map<String, TableHash> tables = new HashMap<>();

    public Database(String path) throws IOException {
        initClassNames();
        rootDirectory = path;
        load();
    }

    protected File getRootDirectory() throws DatabaseFileStructureException {
        return new File(rootDirectory);
    }

    protected void load() throws IOException {
        File root = getRootDirectory();
        try {
            if (root.exists() && root.isDirectory()) {
                File[] subfolders = getTablesFromRoot(root);
                for (File folder : subfolders) {
                    String name = folder.getName();
                    Path tableSignature = new File(rootDirectory).toPath().resolve(name
                            + File.separator + signatureFileName);
                    List<Class<?>> signature = readSignature(tableSignature.toFile());
                    TableHash table = new TableHash(this, name, signature);
                    tables.put(name, table);
                }
            } else {
                throw new DatabaseFileStructureException("Root directory not found");
            }
        } catch (SecurityException ex) {
            throw new LoadOrSaveException("Error in loading, access denied: " + ex.getMessage(), ex);
        }
    }

    protected File[] getTablesFromRoot(File root) {
        File[] subfolders = root.listFiles();
        for (File folder : subfolders) {
            if (!folder.isDirectory()) {
                return whatToDoWithFiles(folder);
            }
        }
        return subfolders;
    }

    private File[] whatToDoWithFiles(File folder) {
        throw new DatabaseFileStructureException("There is files in root folder. File'"
                + folder.getName() + "");
    }



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

    private void checkFormat(TableHash table, Storeable storeable)
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

    public List<Pair<String, Integer>> listTables() {
        int size = tables.size();
        List<Pair<String, Integer>> result = new ArrayList<>(size);
        for (String table : tables.keySet()) {
            result.add(new Pair<>(table, tables.get(table).size()));
        }
        return result;
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

    protected Path getRootDirectoryPath() throws DatabaseFileStructureException {
        return new File(rootDirectory).toPath();
    }


    @Override
    public Table getTable(String name) {
        checkIsNameInvalid(name);
        if (tables.containsKey(name)) {
            return tables.get(name);
        } else {
            return null;
        }
    }


    @Override
    public Table createTable(String name, List<Class<?>> columnTypes)
            throws IOException {
        checkIsNameInvalid(name);
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
            String tableDirPath = rootDirectory + File.separator + name;
            File tableDir = new File(tableDirPath);
            if (!tableDir.mkdir()) {
                throw new IOException("Can't create this table");
            }
            File signatureFile = new File(tableDirPath
                    + File.separator + signatureFileName);
            writeSignature(signatureFile, columnTypes);
            TableHash table = new TableHash(this, name, columnTypes);
            tables.put(name, table);
            return table;
        }
    }

    @Override
    public void removeTable(String name)
            throws IOException {
        checkIsNameInvalid(name);
        if (name == null) {
            throw new IllegalArgumentException("Name is null");
        }
        TableHash table = (TableHash) getTable(name);

        if (table == null) {
            throw new IllegalStateException("Table not exist");
        }
        table.drop();
        tables.remove(name);
    }

    @Override
    public Storeable deserialize(ru.fizteh.fivt.storage.structured.Table table, String value)
            throws ParseException {
        if (value == null) {
            return null;
        }
        String str = value.trim();
        String stringRegex = "\"([^\"]*)\"";
        String oneColumnTypeRegex = "\\s*(" + stringRegex + "|null|true|false|-?\\d+(\\.\\d+)?)\\s*";
        String jsonRegex = "^\\[" + oneColumnTypeRegex + "(," + oneColumnTypeRegex + ")*\\]$";
        if (!str.matches(jsonRegex)) {
            throw new ParseException("value isn't in JSON format", 0);
        } else {
            try {
                int leftBracket = str.indexOf('[');
                int rightBracket = str.lastIndexOf(']');
                List<Object> values = new LinkedList<>();
                int i = leftBracket + 1;
                while (i < rightBracket) {
                    char currChar = str.charAt(i);
                    if (currChar == '\"') {
                        // String argument. Finding end quote.
                        int endQuoteIndex = i + 1;
                        while (!(str.charAt(endQuoteIndex) == '\"' && str.charAt(endQuoteIndex - 1) != '\\')) {
                            endQuoteIndex++;
                        }
                        String strColumn = str.substring(i + 1, endQuoteIndex);
                        values.add(strColumn);
                        i = endQuoteIndex + 1;
                    } else if (Character.isSpaceChar(currChar) || currChar == ',') {
                        i++;
                    } else if (Character.isDigit(currChar) || currChar == '-') {
                        int nextComma = str.indexOf(',', i);
                        if (nextComma == -1) {
                            // Last column.
                            nextComma = rightBracket;
                        }
                        String number = str.substring(i, nextComma).trim();
                        Class<?> tableColType = table.getColumnType(values.size());
                        if (number.indexOf('.') != -1) {
                            if (tableColType.equals(Double.class)) {
                                values.add(new Double(number));
                            } else if (tableColType.equals(Float.class)) {
                                values.add(new Float(number));
                            }
                        } else {
                            if (tableColType.equals(Integer.class)) {
                                values.add(new Integer(number));
                            } else if (tableColType.equals(Long.class)) {
                                values.add(new Long(number));
                            } else if (tableColType.equals(Double.class)) {
                                values.add(new Double(number));
                            } else if (tableColType.equals(Float.class)) {
                                values.add(new Float(number));
                            }
                        }
                        i = nextComma + 1;
                    } else {
                        // Boolean or null
                        int nextComma = str.indexOf(',', i);
                        if (nextComma == -1) {
                            nextComma = rightBracket;
                        }
                        String boolOrNullValue = str.substring(i, nextComma).trim();
                        if (boolOrNullValue.equals("true")) {
                            values.add(true);
                        } else if (boolOrNullValue.equals("false")) {
                            values.add(false);
                        } else if (boolOrNullValue.equals("null")) {
                            values.add(null);
                        } else {
                            throw new ParseException("it's not possible, but there is a parse error!", 0);
                        }
                        i = nextComma + 1;
                    }
                }
                if (values.size() != table.getColumnsCount()) {
                    throw new ParseException("incompatible sizes of Storeable in the table and json you passed", 0);
                }
                return createFor(table, values);
            } catch (IndexOutOfBoundsException e) {
                throw new ParseException("can't parse your json", 0);
            } catch (NumberFormatException e) {
                throw new ParseException("types incompatibility", 0);
            }
        }
    }

    protected int findClosingQuotes(String string,
                                    int begin,
                                    int end,
                                    char quoteCharacter,
                                    char escapeCharacter) throws ParseException {
        // Indicates that the symbol at current (index) position is escaped by previous symbol.
        boolean escaped = false;

        for (int index = begin; index < end; index++) {
            char c = string.charAt(index);

            if (c == quoteCharacter) {
                if (!escaped) {
                    return index;
                }
                escaped = false;
            } else if (c == escapeCharacter) {
                escaped = !escaped;
            } else {
                if (escaped) {
                    throw new ParseException(
                            "Unexpected escaped symbol at position " + index + ": '" + c + "'", index);
                }
            }

        }

        return -1;
    }

    @Override
    public List<String> getTableNames() {
        List<String> result = new ArrayList<>();
        for (String name: tables.keySet()) {
            result.add(name);
        }
        return result;
    }

    @Override
    public String serialize(ru.fizteh.fivt.storage.structured.Table table, Storeable value)
            throws ColumnFormatException {
        if (value == null) {
            return null;
        }
        List<Class<?>> signature = (((TableHash) table).getSignature());
        StringBuilder builder = new StringBuilder();
        int size = table.getColumnsCount();
        builder.append('[');
        Object obj;
        for (int i = 0; i < size; i++) {
            obj = value.getColumnAt(i);
            if (obj != null && !obj.getClass().equals(signature.get(i))) {
                throw new ColumnFormatException("Wrong column format");
            }
            if (obj != null && obj.getClass().equals(String.class)) {
                builder.append('"');
            }
            builder.append(obj);
            if (obj != null && obj.getClass().equals(String.class)) {
                builder.append('"');
            }
            if (i != size - 1) {
                builder.append(",");
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
        StorableClass res = (StorableClass) createFor(table);
        for (int columnNumber = 0; columnNumber < values.size(); columnNumber++) {
            if (!table.getColumnType(columnNumber).equals(
                    values.get(columnNumber).getClass())) {
                throw new ColumnFormatException("Invalid values format");
            }
            res.setColumnAt(columnNumber, values.get(columnNumber));
        }
        return res;
    }

    protected static void checkIsNameInvalid(String name) {
        if (name == null) {
            throw new IllegalArgumentException("Name can't be null");
        }
        if (name.contains(File.separator) || name.startsWith(".")) {
            throw new IllegalArgumentException("Invalid name of table");
        }
    }
}

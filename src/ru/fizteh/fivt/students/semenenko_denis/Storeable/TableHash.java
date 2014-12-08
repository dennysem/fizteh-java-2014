package ru.fizteh.fivt.students.semenenko_denis.Storeable;

import ru.fizteh.fivt.storage.structured.ColumnFormatException;
import ru.fizteh.fivt.storage.structured.Storeable;
import ru.fizteh.fivt.storage.structured.Table;
import ru.fizteh.fivt.students.semenenko_denis.MultiFileHashMap.*;

import java.io.IOException;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by denny_000 on 01.12.2014.
 */
public class TableHash implements Table{
    protected static final int FILES_COUNT = 16;
    protected static final int SUBDIRECTORIES_COUNT = 16;
    private TableFileDAT[][] structuredParts;
    private Map<String, String> uncommited = new HashMap<>();
    private String tableName;
    private ru.fizteh.fivt.students.semenenko_denis.Storeable.Database database;

    public TableHash(String name, Database databaseParent) {
        initDATFiles();
        tableName = name;
        database = databaseParent;
    }

    protected void initDATFiles() {
        structuredParts = new TableFileDAT[SUBDIRECTORIES_COUNT][];
        for (int i = 0; i < SUBDIRECTORIES_COUNT; ++i) {
            structuredParts[i] = new TableFileDAT[FILES_COUNT];
            for (int j = 0; j < FILES_COUNT; j++) {
                structuredParts[i][j] = new TableFileDAT(this, i, j);
            }
        }
    }

    public String getTableName() {
        return tableName;
    }

    protected TableFileDAT selectPartForKey(String key)
            throws LoadOrSaveException {
        int hashcode = key.hashCode();
        int ndirectory = hashcode % SUBDIRECTORIES_COUNT;
        int nfile = hashcode / SUBDIRECTORIES_COUNT % FILES_COUNT;
        if (ndirectory < 0) {
            ndirectory += SUBDIRECTORIES_COUNT;
        }
        if (nfile < 0) {
            nfile += FILES_COUNT;
        }
        return structuredParts[ndirectory][nfile];
    }

    public TableFileDAT getDATFileForKey(String key) {
        TableFileDAT tablePart = selectPartForKey(key);
        tablePart.load();
        return tablePart;
    }

    public void save() {
        for (String key : uncommited.keySet()) {
            String value = uncommited.get(key);
            if (value == null) {
                getDATFileForKey(key).remove(key);
            } else {
                getDATFileForKey(key).put(key, value);
            }
        }
        uncommited.clear();
        for (TableFileDAT[] dir : structuredParts) {
            for (TableFileDAT part : dir) {
                if (part.isLoaded()) {
                    part.save();
                }
            }
        }
    }

    public int getNumberOfUncommitedChanges() {
        return uncommited.size();
    }

    public Path getDirectory() throws DatabaseFileStructureException {
        return database.getRootDirectoryPath().resolve(tableName);
    }

    @Override
    public Storeable put(String key, Storeable value) throws ColumnFormatException {
        if (key == null) {
            throw new IllegalArgumentException("Key is null.");
        }
        if (value == null) {
            throw new IllegalArgumentException("Key is null.");
        }
        StorableClass oldValue = (StorableClass) get(key);
        if (value.equals(getDATFileForKey(key).get(key))) {
            uncommited.remove(key);
        } else {
            String serializedValue = database.serialize(this, value);
            uncommited.put(key, serializedValue);
        }
        return oldValue;
    }

    @Override
    public Storeable remove(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Key is null.");
        }
        String value = getDATFileForKey(key).get(key);
        StorableClass oldValue = (StorableClass) get(key);
        if (value != null) {
            if (oldValue != null) {
                uncommited.put(key, null);
            }
        } else {
            uncommited.remove(key);
        }
        return oldValue;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public int commit() throws IOException {
        return 0;
    }

    @Override
    public int rollback() {
        return 0;
    }

    @Override
    public int getColumnsCount() {
        return 0;
    }

    @Override
    public Class<?> getColumnType(int columnIndex) throws IndexOutOfBoundsException {
        return null;
    }

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public Storeable get(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Key is null.");
        }
        try {
            if (uncommited.containsKey(key)) {
                return database.deserialize(this, uncommited.get(key));
            } else {
                return database.deserialize(this, getDATFileForKey(key).get(key));
            }
        } catch (ParseException e) {
            return null;
        }
    }

}

package ru.fizteh.fivt.students.semenenko_denis.MultiFileHashMap;

import ru.fizteh.fivt.storage.strings.Table;

import java.io.File;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Consumer;

public class TableHash implements Table {

    protected static final int FILES_COUNT = 16;
    protected static final int SUBDIRECTORIES_COUNT = 16;
    private TableFileDAT[][] structuredParts;
    private Map<String, String> uncommited = new HashMap<>();
    private String tableName;
    private Database database;

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

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public String get(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Key is null.");
        }
        if (uncommited.containsKey(key)) {
            return uncommited.get(key);
        } else {
            return getDATFileForKey(key).get(key);

        }
    }

    @Override
    public String put(String key, String value) {
        if (key == null) {
            throw new IllegalArgumentException("Key is null.");
        }
        if (value == null) {
            throw new IllegalArgumentException("Key is null.");
        }
        String oldValue = get(key);
        if (value.equals(getDATFileForKey(key).get(key))) {
            uncommited.remove(key);
        } else {
            uncommited.put(key, value);
        }
        return oldValue;
    }

    @Override
    public String remove(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Key is null.");
        }
        String value = getDATFileForKey(key).get(key);
        String oldValue = get(key);
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
        int deletedCount = 0;
        int addedCount = 0;
        for (String key : uncommited.keySet()) {
            String value = uncommited.get(key);
            if (value == null) {
                ++deletedCount;
            } else {
                if (getDATFileForKey(key).get(key) == null) {
                    addedCount++;
                }
            }
        }
        int result = 0;
        for (TableFileDAT[] dir : structuredParts) {
            for (TableFileDAT part : dir) {
                part.load();
                result += part.count();
            }
        }
        return result + addedCount - deletedCount;
    }

    @Override
    public int commit() {
        int result = uncommited.size();
        save();
        return result;
    }

    @Override
    public int rollback() {
        int result = getNumberOfUncommitedChanges();
        uncommited.clear();
        initDATFiles();
        return result;
    }

    @Override
    public List<String> list() {
        ArrayList<String> oldList = new ArrayList<>();
        for (TableFileDAT[] dir : structuredParts) {
            for (TableFileDAT part : dir) {
                part.load();
                oldList.addAll(part.list());
            }
        }
        Set<String> items = new TreeSet<>(oldList);
        for (String key : uncommited.keySet()) {
            String value = uncommited.get(key);
            if (value == null) {
                items.remove(key);
            } else {
                items.add(key);
            }
        }
        final ArrayList<String> result = new ArrayList<>(items.size());
        items.forEach(new Consumer<String>() {
            @Override
            public void accept(String s) {
                result.add(s);
            }
        });
        return result;
    }

    public void drop() {
        try {
            File directory = getDirectory().toFile();
            for (TableFileDAT[] dir : structuredParts) {
                for (TableFileDAT part : dir) {
                    part.drop();
                }
            }
            if (!directory.delete()) {
                throw new LoadOrSaveException("Directory can't deleted. Warning: data lost.");
            }
        } catch (SecurityException ex) {
            throw new LoadOrSaveException("Access denied in deleting table.", ex);
        } catch (UnsupportedOperationException ex) {
            throw new LoadOrSaveException("Error in deleting table.", ex);
        }
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

}


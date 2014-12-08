package ru.fizteh.fivt.students.semenenko_denis.MultiFileHashMap;

import ru.fizteh.fivt.storage.strings.TableProvider;
import ru.fizteh.fivt.storage.strings.TableProviderFactory;

public class DatabaseFactory implements TableProviderFactory{
    Database database;

    @Override
    public TableProvider create(String directoryPath) {
        if (directoryPath == null) {
            throw new IllegalArgumentException("Database directory doesn't set.");
        } else {
            database = new Database(directoryPath);
        }
        return database;
    }
}

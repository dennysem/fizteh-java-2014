package ru.fizteh.fivt.students.semenenko_denis.Storeable;

/**
 * Created by denny_000 on 01.12.2014.
 */

import ru.fizteh.fivt.storage.structured.TableProvider;

import java.io.IOException;

public class DatabaseFactory implements ru.fizteh.fivt.storage.structured.TableProviderFactory {
    @Override
    public TableProvider create(String path) throws IOException {
        if (path == null) {
            throw new IllegalArgumentException("Database directory doesn't set.");
        } else {
            Database database = new Database(path);
            return database;
        }
    }
}


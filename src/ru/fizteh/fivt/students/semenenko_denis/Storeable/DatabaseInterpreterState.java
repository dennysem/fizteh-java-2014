package ru.fizteh.fivt.students.semenenko_denis.Storeable;

import ru.fizteh.fivt.storage.structured.Table;
import ru.fizteh.fivt.students.semenenko_denis.MultiFileHashMap.DatabaseFileStructureException;
import ru.fizteh.fivt.students.semenenko_denis.MultiFileHashMap.LoadOrSaveException;
import ru.fizteh.fivt.students.semenenko_denis.MultiFileHashMap.TableNotFoundException;
import ru.fizteh.fivt.students.semenenko_denis.MultiFileHashMap.Interpreter.InterpreterState;
import ru.fizteh.fivt.students.semenenko_denis.MultiFileHashMap.UncommitedChangesException;

/**
 * Created by denny_000 on 02.12.2014.
 */
public class DatabaseInterpreterState extends InterpreterState {
    ru.fizteh.fivt.students.semenenko_denis.Storeable.Database database;
    Table usingTable;

    void setUsingTable(String name) {
        usingTable = database.getTable(name);
    }

    public ru.fizteh.fivt.storage.structured.Table getUsingTable() {
        return usingTable;
    }

    public void useTable(String name) {
        if (name == null) {
            throw new IllegalArgumentException("Name is null");
        }
        if (database.getTable(name) == null) {
            throw new IllegalArgumentException("Table not exist");
        }
        if (usingTable != null
                && ((TableHash) usingTable).getNumberOfUncommitedChanges() != 0) {
            int uncommited = ((TableHash) usingTable).getNumberOfUncommitedChanges();
            throw new UncommitedChangesException(uncommited + " unsaved changes");
        }
        setUsingTable(name);
    }

    public DatabaseInterpreterState(ru.fizteh.fivt.students.semenenko_denis.Storeable.Database database) {
        this.database = database;
    }

    public ru.fizteh.fivt.students.semenenko_denis.Storeable.Database getDatabase() {
        return database;
    }

    public boolean tryToSave() {
        try {
            save();
        } catch (TableNotFoundException ex) {
            //Table not selected.
        } catch (LoadOrSaveException | DatabaseFileStructureException ex) {
            System.err.println(ex.getMessage());
            return false;
        }
        return true;
    }

    public void save() throws LoadOrSaveException, TableNotFoundException, DatabaseFileStructureException {
        TableHash table = (TableHash) usingTable;
        if (table != null) {
            table.save();
        } else {
            throw new TableNotFoundException("Table isn't selected");
        }
    }
}


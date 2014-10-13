package ru.fizteh.fivt.students.sautin1.filemap;

/**
 * Created by sautin1 on 10/12/14.
 */
public class DatabaseState<MappedValue, T extends GeneralTable<MappedValue>> {
    private GeneralTable<MappedValue> activeTable;
    private final GeneralTableProvider<MappedValue, T> tableProvider;

    public DatabaseState() {
        tableProvider = null;
    }

    public DatabaseState(GeneralTableProvider<MappedValue, T> tableProvider) {
        if (tableProvider == null) {
            throw new IllegalArgumentException("Wrong provider");
        }
        this.tableProvider = tableProvider;
    }

    public GeneralTable<MappedValue> getActiveTable() {
        return activeTable;
    }

    public void setActiveTable(GeneralTable<MappedValue> activeTable) {
        if (activeTable == null) {
            throw new IllegalArgumentException("Wrong table");
        }
        this.activeTable = activeTable;
    }

    public GeneralTableProvider<MappedValue, T> getTableProvider() {
        return tableProvider;
    }
}

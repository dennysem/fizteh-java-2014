package ru.fizteh.fivt.students.semenenko_denis.Storeable;

import javafx.util.Pair;
import ru.fizteh.fivt.storage.structured.Table;
import ru.fizteh.fivt.storage.structured.TableProviderFactory;
import ru.fizteh.fivt.students.semenenko_denis.MultiFileHashMap.*;
import ru.fizteh.fivt.students.semenenko_denis.MultiFileHashMap.Interpreter.Utils;
import ru.fizteh.fivt.students.semenenko_denis.MultiFileHashMap.Interpreter.Command;
import ru.fizteh.fivt.students.semenenko_denis.MultiFileHashMap.Interpreter.Interpreter;
import ru.fizteh.fivt.students.semenenko_denis.MultiFileHashMap.Interpreter.InterpreterState;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;
import java.util.function.BiConsumer;

/**
 * Created by denny_000 on 02.12.2014.
 */
public class StorableMain {
    private static final String PARAM_REGEXP = "\\S+";
    private static Map<String, Class<?>> stringClassMap = new HashMap<>();

    static {
        stringClassMap.put("Integer", Integer.class);
        stringClassMap.put("Long", Long.class);
        stringClassMap.put("Byte", Byte.class);
        stringClassMap.put("Double", Double.class);
        stringClassMap.put("Boolean", Boolean.class);
        stringClassMap.put("String", String.class);
    }

    public static void main(String[] args) {
        final TableProviderFactory databaseFactory = new DatabaseFactory();
        Database db = null;
        DatabaseInterpreterState databaseInterpreterState = null;
        try {
            db = (Database) databaseFactory.create(System.getProperty("fizteh.db.dir"));
            databaseInterpreterState = new DatabaseInterpreterState(db);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(-1);
        }
        new Interpreter(databaseInterpreterState, new Command[]{
                new Command("put", 2, new BiConsumer<InterpreterState, String[]>() {
                    @Override
                    public void accept(InterpreterState interpreterState, String[] arguments) {
                        Database database = getDatabase(interpreterState);
                        putCmd(database,(DatabaseInterpreterState) interpreterState, arguments);
                    }
                }),
                new Command("list", 0, new BiConsumer<InterpreterState, String[]>() {
                    @Override
                    public void accept(InterpreterState interpreterState, String[] arguments) {
                        Database database = getDatabase(interpreterState);
                        listCmd(database, (DatabaseInterpreterState) interpreterState);
                    }
                }),
                new Command("get", 1, new BiConsumer<InterpreterState, String[]>() {
                    @Override
                    public void accept(InterpreterState interpreterState, String[] arguments) {
                        Database database = getDatabase(interpreterState);
                        getCmd(database, (DatabaseInterpreterState) interpreterState, arguments);
                    }
                }),
                new Command("remove", 1, new BiConsumer<InterpreterState, String[]>() {
                    @Override
                    public void accept(InterpreterState interpreterState, String[] arguments) {
                        remove((DatabaseInterpreterState) interpreterState, arguments);
                    }
                }),
                new Command("use", 1, new BiConsumer<InterpreterState, String[]>() {
                    @Override
                    public void accept(InterpreterState interpreterState, String[] arguments) {
                        Database database = getDatabase(interpreterState);
                        useCmd(database, (DatabaseInterpreterState) interpreterState, arguments);
                    }

                }),
                new Command("drop", 1, new BiConsumer<InterpreterState, String[]>() {
                    @Override
                    public void accept(InterpreterState interpreterState, String[] arguments) {
                        Database database = getDatabase(interpreterState);
                        dropCmd(database, (DatabaseInterpreterState) interpreterState, arguments);
                    }
                }),
                new Command("create", -1, new BiConsumer<InterpreterState, String[]>() {
                    @Override
                    public void accept(InterpreterState interpreterState, String[] arguments) {
                        try {
                            Database database = getDatabase(interpreterState);
                            String inputedString = arguments[0];
                            String[] tokens = Utils.findAll(PARAM_REGEXP, inputedString);
                            String name = tokens[0];
                            List<Class<?>> signature = parseSignature(inputedString);
                            database.createTable(name, signature);
                            System.out.println("created");
                        } catch (TableAlreadyExistsException e) {
                            System.out.println("Table already exists");
                        } catch (DatabaseFileStructureException | IOException
                                | LoadOrSaveException | IllegalArgumentException e) {
                            System.out.println(e.getMessage());
                        }
                    }
                }),
                new Command("show", 1, new BiConsumer<InterpreterState, String[]>() {
                    @Override
                    public void accept(InterpreterState interpreterState, String[] arguments) {
                        Database database = getDatabase(interpreterState);
                        showCmd(database, (DatabaseInterpreterState) interpreterState, arguments);
                    }

                }),
                new Command("commit", 0, new BiConsumer<InterpreterState, String[]>() {
                    @Override
                    public void accept(InterpreterState interpreterState, String[] arguments) {
                        Database database = getDatabase(interpreterState);
                        commitCmd(database, (DatabaseInterpreterState) interpreterState);
                    }
                }),
                new Command("rollback", 0, new BiConsumer<InterpreterState, String[]>() {
                    @Override
                    public void accept(InterpreterState interpreterState, String[] arguments) {
                        Database database = getDatabase(interpreterState);
                        rollbackCmd(database, (DatabaseInterpreterState) interpreterState);
                    }
                }),
                new Command("size", 0, new BiConsumer<InterpreterState, String[]>() {
                    @Override
                    public void accept(InterpreterState interpreterState, String[] arguments) {
                        Database database = getDatabase(interpreterState);
                        sizeCmd(database, (DatabaseInterpreterState) interpreterState);
                    }
                }),
                new Command("exit", 0, new BiConsumer<InterpreterState, String[]>() {
                    @Override
                    public void accept(InterpreterState interpreterState, String[] arguments) {
                        DatabaseInterpreterState state = (DatabaseInterpreterState) interpreterState;
                        exitCmd(state);
                    }
                })
        }).run(args);
    }

    private static List<Class<?>> parseSignature(String inputedString) {
        int firstBrace = inputedString.indexOf('(');
        int lastBrace = inputedString.lastIndexOf(')');
        if (firstBrace == -1 || lastBrace == -1) {
            throw new IllegalArgumentException("Wrong aruments");
        }
        inputedString = inputedString.replace('(', ' ');
        inputedString = inputedString.replace(')', ' ');
        String[] tokens = Utils.findAll(PARAM_REGEXP, inputedString);
        List<Class<?>> result = new ArrayList<>();
        for (int i = 1; i < tokens.length; ++i) {
            Class<?> cl = stringClassMap.get(tokens[i]);
            if (cl != null) {
                result.add(cl);
            } else {
                throw new IllegalArgumentException("Wrong type(" + tokens[i] + ")");
            }
        }
        return result;
    }

    private static void exitCmd(DatabaseInterpreterState state) {
        Database database = state.getDatabase();
        TableHash usingTable = (TableHash) state.getUsingTable();
        if (usingTable != null
                && usingTable.getNumberOfUncommitedChanges() != 0) {
            int uncommited = usingTable.getNumberOfUncommitedChanges();
            System.out.println(uncommited + " unsaved changes");
        } else {
            state.tryToSave();
            state.exit();
        }
    }

    private static void sizeCmd(Database database,
                                DatabaseInterpreterState interpreterState) {
        if (interpreterState.getUsingTable() != null) {
            int changes = interpreterState.getUsingTable().size();
            System.out.println(changes);
        } else {
            System.out.println("no table");
        }
    }

    private static void rollbackCmd(Database database,
                                    DatabaseInterpreterState interpreterState) {
        if (interpreterState.getUsingTable() != null) {
            int changes = interpreterState.getUsingTable().rollback();
            System.out.println("rollback " + changes + " changes");
        } else {
            System.out.println("no table");
        }
    }

    private static void dropCmd(Database database,
                                DatabaseInterpreterState interpreterState,
                                String[] arguments) {
        String name = arguments[0];
        Table table = database.getTable(name);
        if (table == interpreterState.getUsingTable()) {
            interpreterState.setUsingTable(null);
        }
        try {
            database.removeTable(name);
            System.out.println("dropped");
        } catch (IllegalArgumentException e) {
            System.out.println(name + " not exists");
        } catch (DatabaseFileStructureException | LoadOrSaveException | IOException e) {
            System.err.println(e.getMessage());
        }
    }

    private static void commitCmd(Database database,
                                  DatabaseInterpreterState interpreterState) {
        if (interpreterState.getUsingTable() != null) {
            int changes = 0;
            try {
                changes = interpreterState.getUsingTable().commit();
            } catch (IOException e) {
                System.err.println(e.getMessage());
            }
            System.out.println("commited " + changes + " changes");
        } else {
            System.out.println("no table");
        }
    }

    private static void showCmd(Database database,
                                DatabaseInterpreterState interpreterState,
                                String[] arguments) {
        if (arguments[0].equals("tables")) {
            try {
                List<Pair<String, Integer>> tables = database.listTables();
                for (Pair<String, Integer> table : tables) {
                    System.out.println(table.getKey() + " " + table.getValue());
                }
            } catch (DatabaseFileStructureException | LoadOrSaveException e) {
                System.err.println(e.getMessage());
            }
        } else {
            System.err.println("Unknown command.");
        }
    }

    private static void listCmd(Database database,
                                DatabaseInterpreterState interpreterState) {
        if (interpreterState.getUsingTable() == null) {
            System.out.println("no table");
        } else {
            List<String> result = ((TableHash) interpreterState.getUsingTable()).list();
            String joined = String.join(", ", result);
            System.out.println(joined);
        }
    }

    private static void useCmd(Database database,
                               DatabaseInterpreterState interpreterState,
                               String[] arguments) {
        String name = arguments[0];
        try {
            interpreterState.useTable(name);
            System.out.println("using " + name);
        } catch (IllegalArgumentException | TableNotFoundException e) {
            System.out.println(e.getMessage());
        } catch (UncommitedChangesException e) {
            System.out.println(e.getMessage());
        } catch (DatabaseFileStructureException | LoadOrSaveException e) {
            System.err.println(e.getMessage());
        }
    }

    private static void putCmd(Database database,
                               DatabaseInterpreterState interpreterState,
                               String[] arguments) {
        String key = arguments[0];
        String value = arguments[1];
        if (interpreterState.getUsingTable() == null) {
            System.out.println("no table");
        } else {
            try {
                Table usingTable = interpreterState.getUsingTable();
                String result = null;
                try {
                    result = database.serialize(usingTable, usingTable.put(arguments[0],
                            database.deserialize(usingTable, arguments[1])));
                } catch (ParseException e) {
                    System.out.println(e.getMessage());
                }

                if (result == null) {
                    System.out.println("new");
                } else {
                    System.out.println("overwrite");
                    System.out.println(result);
                }
            } catch (IllegalArgumentException e) {
                System.err.println(e.getMessage());
            }
        }
    }

    private static void getCmd(Database database,
                               DatabaseInterpreterState interpreterState,
                               String[] arguments) {
        String key = arguments[0];
        if (interpreterState.getUsingTable() == null) {
            System.out.println("no table");
        } else {
            try {
                Table usingTable = interpreterState.getUsingTable();
                String result = database.serialize(usingTable, usingTable.get(arguments[0]));
                if (result == null) {
                    System.out.println("not found");
                } else {
                    System.out.println("found");
                    System.out.println(result);
                }
            } catch (IllegalArgumentException e) {
                System.err.println(e.getMessage());
            }
        }
    }

    private static Database getDatabase(InterpreterState interpreterState) {
        DatabaseInterpreterState state = (DatabaseInterpreterState) interpreterState;
        Database database = state.getDatabase();
        return database;
    }

    static void remove(DatabaseInterpreterState interpreterState,
                       String[] arguments) {
        Database database = getDatabase(interpreterState);
        if (interpreterState.getUsingTable() == null) {
            System.out.println("no table");
        } else {
            try {
                if (interpreterState.getUsingTable().remove(arguments[0]) != null) {
                    System.out.println("removed");
                } else {
                    System.out.println("not found");
                }
            } catch (IllegalArgumentException  | IllegalStateException e) {
                System.err.println(e.getMessage());
            } catch (DatabaseFileStructureException | LoadOrSaveException e) {
                System.err.println(e.getMessage());
            }
        }
    }
}

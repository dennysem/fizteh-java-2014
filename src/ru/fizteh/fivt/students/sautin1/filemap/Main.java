package ru.fizteh.fivt.students.sautin1.filemap;

import ru.fizteh.fivt.students.sautin1.shell.Command;
import ru.fizteh.fivt.students.sautin1.shell.ExitCommand;
import ru.fizteh.fivt.students.sautin1.shell.Shell;
import ru.fizteh.fivt.students.sautin1.shell.UserInterruptException;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by sautin1 on 10/12/14.
 */
public class Main {
    private static final String FILENAME = "db.file";

    public static void main(String[] args) {
        /**/System.setProperty(FILENAME,
        /**/        "/home/sautin1/IdeaProjects/MIPTProjects/src/ru/fizteh/fivt/students/sautin1/filemap/db.file");

        int exitStatus = 0;
        try {
            StringTable table = new StringTable(FILENAME, true);
            Path filePath = Paths.get(System.getProperty(FILENAME));
            StringTableIOTools tableIOTools = new StringTableIOTools();
            tableIOTools.readTable(filePath, table);

            StringTableProvider provider = new StringTableProvider(filePath.getParent(), true, tableIOTools);
            StringDatabaseState databaseState = new StringDatabaseState(provider);
            databaseState.setActiveTable(table);
            Command[] commands = {
                    new PutCommand(), new GetCommand(), new ListCommand(),
                    new RemoveCommand(), new ExitCommand<StringDatabaseState>()
            };
            @SuppressWarnings("unchecked")
            Shell<StringDatabaseState> shell = new Shell<>(databaseState, commands);
            shell.startWork(args);

            tableIOTools.writeTable(filePath.getParent(), table);
        } catch (UserInterruptException e) {
            exitStatus = 0;
        } catch (Exception e) {
            System.err.println(e.getMessage());
            exitStatus = 1;
        }
        System.exit(exitStatus);
    }

}

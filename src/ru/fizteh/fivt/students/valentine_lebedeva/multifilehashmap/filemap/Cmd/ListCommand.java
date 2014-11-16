package ru.fizteh.fivt.students.valentine_lebedeva.multifilehashmap.filemap.Cmd;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import ru.fizteh.fivt.students.valentine_lebedeva.multifilehashmap.MultiFileHashMapManager;

public final class ListCommand extends FileMapCommand {
    @Override
    public void execute(final String[] args, MultiFileHashMapManager parser) {
        if (checkWorkTable(parser.getWorkTable())) {
            checkArgs(1, args);
            List<String> out = new ArrayList<>(parser.getWorkTable().getBase()
                    .size());
            Set<String> keys = parser.getWorkTable().getBase().keySet();
            for (String key : keys) {
                out.add(key);
            }
            System.out.println(String.join(", ", out));
        }
    }
}

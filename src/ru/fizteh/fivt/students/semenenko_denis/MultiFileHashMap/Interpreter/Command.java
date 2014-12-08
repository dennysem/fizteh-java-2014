package ru.fizteh.fivt.students.semenenko_denis.MultiFileHashMap.Interpreter;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

public class Command {
    private String name;
    private int numArguments;
    private BiConsumer<InterpreterState, String[]> callback;

    public Command(String name, int numArguments, BiConsumer<InterpreterState, String[]> callback) {
        this.name = name;
        this.numArguments = numArguments;
        this.callback = callback;
    }

    public String getName() {
        return name;
    }

    public void execute(InterpreterState interpreterState, String[] params) {
        if (numArguments == -1) {
            List<String> listParams = new ArrayList<>();
            for (String arg: params) {
                listParams.add(arg);
            }
            String inputedData = String.join(" ", listParams);
            String[] argument = new String[1];
            argument[0] = inputedData;
            callback.accept(interpreterState, argument);
        }
        if (numArguments != -1 && params.length != numArguments) {
            Utils.interpreterError("Invalid number of arguments: " + numArguments + " expected, " + params.length
                    + " found.");
        } else {
            callback.accept(interpreterState, params);
        }
    }
}

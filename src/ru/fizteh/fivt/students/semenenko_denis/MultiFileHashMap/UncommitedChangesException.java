package ru.fizteh.fivt.students.semenenko_denis.MultiFileHashMap;

/**
 * Created by denny_000 on 21.11.2014.
 */
public class UncommitedChangesException extends RuntimeException {
    public UncommitedChangesException(String message) {
        super(message);
    }

    public UncommitedChangesException(String message, Exception ex) {
        super(message, ex);
    }
}

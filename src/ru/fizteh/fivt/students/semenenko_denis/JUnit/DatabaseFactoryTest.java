package ru.fizteh.fivt.students.semenenko_denis.JUnit;

import org.junit.Test;
import ru.fizteh.fivt.students.semenenko_denis.MultiFileHashMap.DatabaseFactory;

import static org.junit.Assert.*;

public class DatabaseFactoryTest {
    @Test(expected = IllegalArgumentException.class)
    public void testThrowExceptionWhenCreateArgumentNull() {
        new DatabaseFactory().create(null);
    }
}


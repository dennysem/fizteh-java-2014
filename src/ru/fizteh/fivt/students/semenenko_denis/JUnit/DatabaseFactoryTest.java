package ru.fizteh.fivt.students.semenenko_denis.JUnit;


import junit.framework.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import ru.fizteh.fivt.students.semenenko_denis.MultiFileHashMap.DatabaseFactory;
import ru.fizteh.fivt.students.semenenko_denis.Storeable.Database;

import java.io.IOException;

import static org.junit.Assert.*;

public class DatabaseFactoryTest {


    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Test(expected = IllegalArgumentException.class)
    public void testThrowExceptionWhenCreateArgumentNull() {
        new DatabaseFactory().create(null);
    }

}


package ru.fizteh.fivt.students.semenenko_denis.Storeable;

import ru.fizteh.fivt.storage.structured.ColumnFormatException;
import ru.fizteh.fivt.storage.structured.Storeable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by denny_000 on 01.12.2014.
 */
public class StorableClass implements Storeable {
    private List<Object> list;
    private Set<Class<?>> typeSet = new HashSet<>();
    private List<Class<?>> signature;

    public StorableClass(List<Class<?>> newSignature) {
        initTypes();
        signature = newSignature;
        for (Class<?> type : signature) {
            if (!checkType(type)) {
                throw new ColumnFormatException("Invalid signature");
            }
        }
        list = new ArrayList<>();
        for (int i = 0; i < signature.size(); i++) {
            list.add(null);
        }
    }

    private void initTypes() {
        typeSet.add(Integer.class);
        typeSet.add(Long.class);
        typeSet.add(Byte.class);
        typeSet.add(Double.class);
        typeSet.add(Float.class);
        typeSet.add(Boolean.class);
        typeSet.add(String.class);
    }

    private boolean checkType(Class<?> type) {
        return typeSet.contains(type);
    }

    private void checkFormat(int columnIndex, Class<?> type)
            throws ColumnFormatException {
        if (!signature.get(columnIndex).equals(type)) {
            throw new ColumnFormatException("Invalid column format");
        }
    }

    private void checkBounds(int columnIndex)
            throws IndexOutOfBoundsException {
        if (columnIndex >= signature.size() || columnIndex < 0) {
            throw new IndexOutOfBoundsException("Invalid column index");
        }
    }

    @Override
    public void setColumnAt(int columnIndex, Object value) throws ColumnFormatException, IndexOutOfBoundsException {
        checkBounds(columnIndex);
        if (value != null) {
            checkFormat(columnIndex, value.getClass());
        }
        list.set(columnIndex, value);
    }

    @Override
    public Object getColumnAt(int columnIndex) throws IndexOutOfBoundsException {
        checkBounds(columnIndex);
        return list.get(columnIndex);
    }

    @Override
    public Integer getIntAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        checkBounds(columnIndex);
        checkFormat(columnIndex, Integer.class);
        return (Integer) list.get(columnIndex);
    }

    @Override
    public Long getLongAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        checkBounds(columnIndex);
        checkFormat(columnIndex, Long.class);
        return (Long) list.get(columnIndex);
    }

    @Override
    public Byte getByteAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        checkBounds(columnIndex);
        checkFormat(columnIndex, Byte.class);
        return (Byte) list.get(columnIndex);
    }

    @Override
    public Float getFloatAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        checkBounds(columnIndex);
        checkFormat(columnIndex, Float.class);
        return (Float) list.get(columnIndex);
    }

    @Override
    public Double getDoubleAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        checkBounds(columnIndex);
        checkFormat(columnIndex, Double.class);
        return (Double) list.get(columnIndex);
    }

    @Override
    public Boolean getBooleanAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        checkBounds(columnIndex);
        checkFormat(columnIndex, Boolean.class);
        return (Boolean) list.get(columnIndex);
    }

    @Override
    public String getStringAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        checkBounds(columnIndex);
        checkFormat(columnIndex, String.class);
        return (String) list.get(columnIndex);
    }
}


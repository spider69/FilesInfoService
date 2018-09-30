package com.yusalar.database;

import com.yusalar.attributes.validators.AttributeValidator;

import java.util.*;

public class MockedHBase implements DatabaseProvider {
    private Map<Long, ExtendedAttr> primaryTable = new LinkedHashMap<>(); // (id -> md5, zone, format, size)
    private Map<Long, Attr> secondaryTable = new LinkedHashMap<>(); //  (md5 -> zone, format, size)
    private final Object lock = new Object();

    public MockedHBase() {
    }

    /**
    Method for getting md5 by attributes performs searching in secondary table.
    For better performance in case of multiple getAttrByMd5 as well as getMd5ByAttrs it can be done in primary table to
    search in two separate HFiles simultaneously. (getAttrByMd5 -> secondary table, getMd5ByAttrs -> primary table)
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<Long> getMd5ByAttrs(Map<String, AttributeValidator> attrs) {
        List<Long> md5List = new ArrayList<>();
        for (Map.Entry<Long, Attr> entry : secondaryTable.entrySet()) {
            Attr attr = entry.getValue();
            if (attrs.get("zone").validate(attr.getZone()) &&
                    attrs.get("format").validate(attr.getFormat()) &&
                    attrs.get("size").validate(attr.getSize())
            ) {
                md5List.add(entry.getKey());
            }
        }
        return md5List;
    }

    /**
    Method for getting attributes by md5 performs searching by key in the secondary table.
     */
    @Override
    public Optional<Attr> getAttrsByMd5(long md5) {
        return Optional.ofNullable(secondaryTable.getOrDefault(md5, null));
    }

    /**
    Method for performing insertion (id, md5, zone). Better performance can be achieved by dividing table columns into two
    column families for each insert stream. First column family - (md5, zone), second - (format, size). For each column
    family we have separate HFile and can write to these files at the same time.
     */
    @Override
    public void insertFirstGroup(Iterator<FirstColumnsGroup> recordsIter) {
        try {
            Thread.currentThread().sleep(300); // artificial latency
        } catch (InterruptedException e) {}

        synchronized (lock) {
            while (recordsIter.hasNext()) {
                FirstColumnsGroup record = recordsIter.next();
                if (primaryTable.containsKey(record.getId())) {
                    ExtendedAttr attrs = primaryTable.get(record.getId());
                    ExtendedAttr newAttrs = new ExtendedAttr(record.getMd5(), record.getZone(), attrs.getFormat(), attrs.getSize());
                    primaryTable.remove(record.getId());
                    primaryTable.put(record.getId(), newAttrs);
                    secondaryTable.put(record.getMd5(), new Attr(record.getZone(), attrs.getFormat(), attrs.getSize()));
                } else {
                    primaryTable.put(record.getId(), new ExtendedAttr(record.getMd5(), record.getZone(), 0, 0));
                    secondaryTable.put(record.getMd5(), new Attr(record.getZone(), 0, 0));
                }
            }
        }
    }

    /**
    Method for performing insertion (id, format, size).
     */
    @Override
    public void insertSecondGroup(Iterator<SecondColumnsGroup> recordsIter) {
        try {
            Thread.currentThread().sleep(300); // artificial latency
        } catch (InterruptedException e) {}

        synchronized (lock) {
            while (recordsIter.hasNext()) {
                SecondColumnsGroup record = recordsIter.next();
                if (primaryTable.containsKey(record.getId())) {
                    ExtendedAttr attrs = primaryTable.get(record.getId());
                    ExtendedAttr newAttrs = new ExtendedAttr(attrs.getMd5(), attrs.getZone(), record.getFormat(), record.getSize());
                    primaryTable.remove(record.getId());
                    primaryTable.put(record.getId(), newAttrs);

                    // updating secondary table fields (format, size) for corresponding md5 from primary table
                    Attr secondaryAttrs = secondaryTable.get(attrs.getMd5());
                    Attr newSecondaryAttrs = new Attr(secondaryAttrs.getZone(), record.getFormat(), record.getSize());
                    secondaryTable.remove(attrs.getMd5());
                    secondaryTable.put(attrs.getMd5(), newSecondaryAttrs);
                } else {
                    primaryTable.put(record.getId(), new ExtendedAttr(0, 0, record.getFormat(), record.getSize()));
                }
            }
        }
    }

    private static class ExtendedAttr extends Attr {
        private final long md5;

        ExtendedAttr(long md5, long zone, int format, long size) {
            super(zone, format, size);
            this.md5 = md5;
        }

        long getMd5() {
            return md5;
        }
    }
}

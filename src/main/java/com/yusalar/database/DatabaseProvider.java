package com.yusalar.database;

import com.yusalar.attributes.validators.AttributeValidator;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

// common interface for database providing
public interface DatabaseProvider {
    // methods for getting from db
    List<Long> getMd5ByAttrs(Map<String, AttributeValidator> attrs);
    Optional<Attr> getAttrsByMd5(long md5);

    // methods for insertion to db
    void insertFirstGroup(Iterator<FirstColumnsGroup> records);
    void insertSecondGroup(Iterator<SecondColumnsGroup> records);

    // for flow (id, md5, zone)
    class FirstColumnsGroup {
        private final long id;
        private final long md5;
        private final int zone;

        public FirstColumnsGroup() {
            id = -1;
            md5 = 0;
            zone = 0;
        }

        public long getId() {
            return id;
        }

        public long getMd5() {
            return md5;
        }

        public int getZone() {
            return zone;
        }
    }

    // for flow (id, format, size)
    class SecondColumnsGroup {
        private final long id;
        private final int format;
        private final long size;

        public SecondColumnsGroup() {
            id = -1;
            format = 0;
            size = 0;
        }

        public long getId() {
            return id;
        }

        public int getFormat() {
            return format;
        }

        public long getSize() {
            return size;
        }
    }

    // for getting (zone, format, size) by md5
    class Attr {
        private final long zone;
        private final int format;
        private final long size;

        public Attr(long zone, int format, long size) {
            this.zone = zone;
            this.format = format;
            this.size = size;
        }

        public long getZone() {
            return zone;
        }

        public int getFormat() {
            return format;
        }

        public long getSize() {
            return size;
        }
    }
}

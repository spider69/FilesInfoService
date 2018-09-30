package com.yusalar.actors;

import com.yusalar.attributes.validators.AttributeValidator;
import com.yusalar.database.DatabaseProvider;

import java.util.List;
import java.util.Map;

public interface DatabaseMessages {
    class InsertFirstGroup {
        private final List<DatabaseProvider.FirstColumnsGroup> rows;

        public InsertFirstGroup(List<DatabaseProvider.FirstColumnsGroup> rows) {
            this.rows = rows;
        }

        public List<DatabaseProvider.FirstColumnsGroup> getRows() {
            return rows;
        }
    }

    class InsertSecondGroup {
        private final List<DatabaseProvider.SecondColumnsGroup> rows;

        public InsertSecondGroup(List<DatabaseProvider.SecondColumnsGroup> rows) {
            this.rows = rows;
        }

        public List<DatabaseProvider.SecondColumnsGroup> getRows() {
            return rows;
        }
    }

    class GetMd5ByAttrs {
        private final Map<String, AttributeValidator> attrs;

        public GetMd5ByAttrs(Map<String, AttributeValidator> attrs) {
            this.attrs = attrs;
        }

        public Map<String, AttributeValidator> getAttrs() {
            return attrs;
        }
    }

    class GetAttrsByMd5 {
        private final long md5;

        public GetAttrsByMd5(long md5) {
            this.md5 = md5;
        }

        long getMd5() {
            return md5;
        }
    }
}

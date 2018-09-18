package com.yusalar.actors;

import com.yusalar.database.DatabaseProvider;

import java.util.List;

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
        private final DatabaseProvider.AttrsDescription attrs;

        public GetMd5ByAttrs(DatabaseProvider.AttrsDescription attrs) {
            this.attrs = attrs;
        }

        public DatabaseProvider.AttrsDescription getAttrs() {
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

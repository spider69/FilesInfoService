package com.yusalar.actors;

import akka.actor.AbstractActor;
import com.yusalar.database.DatabaseProvider;

public class DatabaseAccessActor extends AbstractActor {
    private final DatabaseProvider database;

    public DatabaseAccessActor(DatabaseProvider database) {
        this.database = database;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(DatabaseMessages.InsertFirstGroup.class,
                        rowsForInserting -> {
                            database.insertFirstGroup(rowsForInserting.getRows().iterator());
                            getSender().tell("First group insertion complete\n", getSelf());
                        })
                .match(DatabaseMessages.InsertSecondGroup.class,
                        rowsForInserting -> {
                            database.insertSecondGroup(rowsForInserting.getRows().iterator());
                            getSender().tell("Second group insertion complete\n", getSelf());
                        })
                .match(DatabaseMessages.GetMd5ByAttrs.class,
                        getMd5ByAttrs -> getSender().tell(database.getMd5ByAttrs(getMd5ByAttrs.getAttrs()), getSelf()))
                .match(DatabaseMessages.GetAttrsByMd5.class,
                        getAttrsByMd5 -> getSender().tell(database.getAttrsByMd5(getAttrsByMd5.getMd5()), getSelf()))
                .build();
    }
}

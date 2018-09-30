package com.yusalar.routes;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.http.javadsl.common.EntityStreamingSupport;
import akka.http.javadsl.common.JsonEntityStreamingSupport;
import akka.http.javadsl.marshallers.jackson.Jackson;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.server.Route;
import akka.http.javadsl.unmarshalling.StringUnmarshallers;
import akka.http.javadsl.unmarshalling.Unmarshaller;
import akka.pattern.PatternsCS;
import akka.util.ByteString;
import com.yusalar.actors.DatabaseMessages;
import com.yusalar.attributes.validators.AttributeValidator;
import com.yusalar.attributes.AttributeValidatorsFactory;
import com.yusalar.database.DatabaseProvider;

import java.util.*;
import java.util.concurrent.CompletionStage;

import static akka.http.javadsl.server.Directives.*;

public class UserRoutes {
    private final ActorRef databaseAccessActorsPool;
    private final LoggingAdapter logger;

    private static int timeout = 3000; // 3 secs

    public UserRoutes(ActorSystem system, ActorRef databaseAccessActorsPool) {
        this.databaseAccessActorsPool = databaseAccessActorsPool;
        logger = Logging.getLogger(system, this);
    }

    public Route routes() {
        return route(
                path("insertFirst", this::insertFirstGroup),
                path("insertSecond", this::insertSecondGroup),
                path("get", () ->
                        route(
                                getAttrsByMd5(),
                                getMd5ByAttrs()
                        )
                )
        );
    }

    /**
    Method for streaming first flow of records [{"id": 1, "md5": 3, "zone": 5}, {"id": 2, "md5": 456, "zone": 2}, ...]
    from POST body.
     */
    private Route insertFirstGroup() {
        logger.info("Insertion query gained");
        // TODO: get md5 as hex instead of long
        final Unmarshaller<ByteString, DatabaseProvider.FirstColumnsGroup> firstColumnsGroupUnmarshaller =
                Jackson.byteStringUnmarshaller(DatabaseProvider.FirstColumnsGroup.class);
        return post(() ->
                extractMaterializer(mat -> {
                    final JsonEntityStreamingSupport jsonSupport = EntityStreamingSupport.json();
                    return entityAsSourceOf(firstColumnsGroupUnmarshaller, jsonSupport,
                            sourceOfRows -> {
                                final CompletionStage<List<DatabaseProvider.FirstColumnsGroup>> rows = sourceOfRows
                                        .runFold(new LinkedList<>(), (lst, row) -> {
                                            lst.add(row);
                                            return lst;
                                        }, mat);

                                // TODO: add fails handling
                                return onComplete(rows, gainedRows -> {
                                    CompletionStage<String> recordsInserted = PatternsCS
                                            .ask(databaseAccessActorsPool, new DatabaseMessages.InsertFirstGroup(gainedRows.get()), timeout)
                                            .thenApply(obj -> (String) obj);
                                    return onSuccess(() -> recordsInserted, performed -> {
                                        logger.info("Insertion query successfully performed");
                                        return complete(StatusCodes.OK, performed);
                                    });
                                });
                            });
                })
        );
    }

    /**
    Method for streaming second flow of records [{"id": 1, "format": 45, "size": 5}, {"id": 2, "format": 200, "size": 500}, ...]
    from POST body.
    */
    private Route insertSecondGroup() {
        logger.info("Insertion query gained");
        final Unmarshaller<ByteString, DatabaseProvider.SecondColumnsGroup> firstColumnsGroupUnmarshaller =
                Jackson.byteStringUnmarshaller(DatabaseProvider.SecondColumnsGroup.class);
        return post(() ->
                extractMaterializer(mat -> {
                    final JsonEntityStreamingSupport jsonSupport = EntityStreamingSupport.json();
                    return entityAsSourceOf(firstColumnsGroupUnmarshaller, jsonSupport,
                            sourceOfRows -> {
                                final CompletionStage<List<DatabaseProvider.SecondColumnsGroup>> rows = sourceOfRows
                                        .runFold(new LinkedList<>(), (lst, row) -> {
                                            lst.add(row);
                                            return lst;
                                        }, mat);

                                // TODO: add fails handling
                                return onComplete(rows, gainedRows -> {
                                    CompletionStage<String> recordsInserted = PatternsCS
                                            .ask(databaseAccessActorsPool, new DatabaseMessages.InsertSecondGroup(gainedRows.get()), timeout)
                                            .thenApply(obj -> (String) obj);
                                    return onSuccess(() -> recordsInserted, performed -> {
                                        logger.info("Insertion query successfully performed");
                                        return complete(StatusCodes.OK, performed);
                                    });
                                });
                            });
                })
        );
    }

    private Route getAttrsByMd5() {
        logger.info("Get attributes query gained");
        return get(() -> parameter(StringUnmarshallers.LONG_HEX, "md5", md5 -> {
                    CompletionStage<Optional<DatabaseProvider.Attr>> attrs = PatternsCS
                            .ask(databaseAccessActorsPool, new DatabaseMessages.GetAttrsByMd5(md5), timeout)
                            .thenApply(obj -> (Optional<DatabaseProvider.Attr>) obj);
                    return onSuccess(() -> attrs, receivedAttrs -> {
                        if (receivedAttrs.isPresent()) {
                            logger.info("Get attributes query successfully performed");
                            DatabaseProvider.Attr attributes = receivedAttrs.get();
                            return complete(StatusCodes.OK, String.format("zone: %d\nformat: %d\nsize: %d",
                                    attributes.getZone(),
                                    attributes.getFormat(),
                                    attributes.getSize()));
                        } else {
                            logger.info("Get attributes query failed");
                            return complete(StatusCodes.NOT_FOUND, String.format("Requested md5=%d not found", md5));
                        }
                    });
                }));
    }

    /**
    Method returns list of Md5 for corresponding attributes.
     */
    private Route getMd5ByAttrs() {
        logger.info("Get md5 query gained");
        return get(() -> parameterMap(params -> {
            Map<String, AttributeValidator> attrs = new HashMap<>();
            params.forEach((name, attrAsString) ->
                attrs.put(name, AttributeValidatorsFactory.getInstance().parseAttributeFromString(name, attrAsString))
            );
            CompletionStage<List<Long>> md5 = PatternsCS
                    .ask(databaseAccessActorsPool, new DatabaseMessages.GetMd5ByAttrs(attrs), timeout)
                    .thenApply(obj -> (List<Long>) obj);
            // TODO: return md5 as hex in list
            return onSuccess(() -> md5, listOfMd5 -> {
                if (listOfMd5.isEmpty()) {
                    logger.info("Get md5 query failed");
                    return complete(StatusCodes.NOT_FOUND, "Md5 for requested attributes doesn't exist");
                } else {
                    logger.info("Get md5 query successfully performed");
                    return complete(StatusCodes.OK, listOfMd5, Jackson.marshaller());
                }
            });
        }));
    }
}

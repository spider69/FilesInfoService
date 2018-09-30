package com.yusalar.routes;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.MediaTypes;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.testkit.JUnitRouteTest;
import akka.http.javadsl.testkit.TestRoute;
import akka.routing.RoundRobinPool;
import com.yusalar.Server;
import com.yusalar.actors.DatabaseAccessActor;
import com.yusalar.attributes.AttributeValidatorsFactory;
import com.yusalar.database.DatabaseProvider;
import com.yusalar.database.MockedHBase;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class UserRoutesTest extends JUnitRouteTest {
    private TestRoute appRoute;
    private DatabaseProvider database;

    @Before
    public void init() {
        ActorSystem system = ActorSystem.create("testServer");
        AttributeValidatorsFactory.getInstance().registerAttribute("zone", Long::parseLong);
        AttributeValidatorsFactory.getInstance().registerAttribute("format", Integer::parseInt);
        AttributeValidatorsFactory.getInstance().registerAttribute("size", Long::parseLong);
        database = new MockedHBase();
        ActorRef databaseBalancer = system.actorOf(new RoundRobinPool(5).props(Props.create(DatabaseAccessActor.class, database)));
        Server server = new Server(system, databaseBalancer);
        appRoute = testRoute(server.createRoute());
    }

    @Test
    public void simpleInsertGet() throws Exception {
        appRoute.run(HttpRequest.POST("/insertFirst")
                .withEntity(MediaTypes.APPLICATION_JSON.toContentType(),
                        "[{\"id\": 1, \"md5\": 15, \"zone\": 5},{\"id\": 2, \"md5\": 10, \"zone\": 9}]")
        );

        appRoute.run(HttpRequest.POST("/insertSecond")
                .withEntity(MediaTypes.APPLICATION_JSON.toContentType(),
                        "[{\"id\": 1, \"format\": 2, \"size\": 1024},{\"id\": 2, \"format\": 5, \"size\": 19}]")
        );

        Thread.currentThread().sleep(350); // waiting for insertion

        appRoute.run(HttpRequest.GET("/get?md5=F"))
                .assertStatusCode(StatusCodes.OK)
                .assertEntity("zone: 5\nformat: 2\nsize: 1024");

        appRoute.run(HttpRequest.GET("/get?md5=A"))
                .assertStatusCode(StatusCodes.OK)
                .assertEntity("zone: 9\nformat: 5\nsize: 19");
    }

    @Test
    public void insertGetInParallelThreads() {
        ExecutorService executor = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 5; ++i) {
            final int id = i;
            executor.execute(() -> {
                try {
                    appRoute.run(HttpRequest.POST("/insertFirst")
                            .withEntity(MediaTypes.APPLICATION_JSON.toContentType(),
                                    "[{\"id\": " + id + ", \"md5\": " + id +", \"zone\": 5}]")
                    );

                    appRoute.run(HttpRequest.POST("/insertSecond")
                            .withEntity(MediaTypes.APPLICATION_JSON.toContentType(),
                                    "[{\"id\": " + id + ", \"format\": 2, \"size\": 1024}]")
                    );

                    Thread.currentThread().sleep(400); // waiting for insertion

                    appRoute.run(HttpRequest.GET("/get?md5=" + id))
                            .assertStatusCode(StatusCodes.OK)
                            .assertEntity("zone: 5\nformat: 2\nsize: 1024");
                } catch (InterruptedException e) {
                }
            });
        }

        executor.shutdown();
        while(!executor.isTerminated()) {}
    }

    @Test
    public void getMd5() throws Exception {
        appRoute.run(HttpRequest.POST("/insertFirst")
                .withEntity(MediaTypes.APPLICATION_JSON.toContentType(),
                        "[{\"id\": 1, \"md5\": 13, \"zone\": 5},{\"id\": 2, \"md5\": 10, \"zone\": 9}]")
        );

        appRoute.run(HttpRequest.POST("/insertSecond")
                .withEntity(MediaTypes.APPLICATION_JSON.toContentType(),
                        "[{\"id\": 1, \"format\": 254, \"size\": 1024},{\"id\": 2, \"format\": 8, \"size\": 195}]")
        );

        Thread.currentThread().sleep(400);

        appRoute.run(HttpRequest.GET("/get?md5=D"))
                .assertStatusCode(StatusCodes.OK)
                .assertEntity("zone: 5\nformat: 254\nsize: 1024");
    }

    @Test
    public void notFoundMd5() {
        appRoute.run(HttpRequest.GET("/get?md5=A"))
                .assertStatusCode(StatusCodes.NOT_FOUND);
    }

    @Test
    public void getAttrsList() throws Exception {
        appRoute.run(HttpRequest.POST("/insertFirst")
                .withEntity(MediaTypes.APPLICATION_JSON.toContentType(),
                        "[{\"id\": 1, \"md5\": 15, \"zone\": 5},{\"id\": 2, \"md5\": 10, \"zone\": 5}]")
        );

        appRoute.run(HttpRequest.POST("/insertSecond")
                .withEntity(MediaTypes.APPLICATION_JSON.toContentType(),
                        "[{\"id\": 1, \"format\": 2, \"size\": 1024},{\"id\": 2, \"format\": 5, \"size\": 19}]")
        );

        Thread.currentThread().sleep(400);

        appRoute.run(HttpRequest.GET("/get?zone=5&format=2,5&size=1-1024"))
                .assertStatusCode(StatusCodes.OK)
                .assertEntity("[15,10]");
    }

    @Test
    public void getAttrsListComplex() throws Exception {
        appRoute.run(HttpRequest.POST("/insertFirst")
                .withEntity(MediaTypes.APPLICATION_JSON.toContentType(),
                        "[{\"id\": 1, \"md5\": 15, \"zone\": 5},{\"id\": 2, \"md5\": 10, \"zone\": 7}]")
        );

        appRoute.run(HttpRequest.POST("/insertSecond")
                .withEntity(MediaTypes.APPLICATION_JSON.toContentType(),
                        "[{\"id\": 1, \"format\": 2, \"size\": 1024},{\"id\": 2, \"format\": 5, \"size\": 19}]")
        );

        Thread.currentThread().sleep(400);

        appRoute.run(HttpRequest.GET("/get?zone=5-7&format=2-5&size=1-1024"))
                .assertStatusCode(StatusCodes.OK)
                .assertEntity("[15,10]");

        appRoute.run(HttpRequest.GET("/get?zone=5,7&format=2,5&size=19,1024"))
                .assertStatusCode(StatusCodes.OK)
                .assertEntity("[15,10]");

        appRoute.run(HttpRequest.GET("/get?zone=5&format=2&size=1024"))
                .assertStatusCode(StatusCodes.OK)
                .assertEntity("[15]");

        appRoute.run(HttpRequest.GET("/get?zone=5,8,15&format=2&size=1-1024"))
                .assertStatusCode(StatusCodes.OK)
                .assertEntity("[15]");
    }

    @Test
    public void notFoundAttrsList() {
        appRoute.run(HttpRequest.GET("/get?zone=5&format=2,5&size=1-1024"))
                .assertStatusCode(StatusCodes.NOT_FOUND);
    }

    /* TODO:
    - add tests for checking wrong params while getting attrs and md5
    - add high load tests with many requests
    - add tests for various database fails handling and proper retrying (after implementing of supervisor)
    * */
}

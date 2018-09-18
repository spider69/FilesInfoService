package com.yusalar;

import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.server.Route;
import akka.routing.RoundRobinPool;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import com.yusalar.actors.DatabaseAccessActor;
import com.yusalar.database.DatabaseProvider;
import com.yusalar.database.MockedHBase;
import com.yusalar.routes.UserRoutes;

import java.util.concurrent.CompletionStage;

public class Server {
    // default configuration
    // TODO: create conf file
    private static final String HOST = "localhost";
    private static final int PORT = 8080;
    private static final int PARALLEL_WORKERS_NUM = 16; // depends on total workload

    private final UserRoutes userRoutes;

    public Server(ActorSystem system, ActorRef databaseAccessActorsPool) {
        this.userRoutes = new UserRoutes(system, databaseAccessActorsPool);
    }

    public static void main(String[] args) throws Exception {
        final ActorSystem system = ActorSystem.create("server");

        final Http http = Http.get(system);
        final ActorMaterializer materializer = ActorMaterializer.create(system);

        DatabaseProvider database = new MockedHBase();
        Server app = new Server(system, getBalancer(system, database));
        final LoggingAdapter logger = Logging.getLogger(system, app);

        final Flow<HttpRequest, HttpResponse, NotUsed> routeFlow = app.createRoute().flow(system, materializer);
        final CompletionStage<ServerBinding> binding = http.bindAndHandle(routeFlow, ConnectHttp.toHost(HOST, PORT), materializer);

        logger.info(String.format("Server online on http://%s:%d", HOST, PORT));

        System.out.println(String.format("Server online at http://%s:%d\nPress RETURN to stop...", HOST, PORT));
        System.in.read();

        binding.thenCompose(ServerBinding::unbind).thenAccept(unbound -> system.terminate());
        logger.info(String.format("Server on http://%s:%d was shut down", HOST, PORT));
    }

    // rewrite this getter for smarter balancing
    private static ActorRef getBalancer(ActorSystem system, DatabaseProvider database) {
        // TODO: use supervisor with proper supervisorStrategy() for exception handling and queries retrying
        return system.actorOf(new RoundRobinPool(PARALLEL_WORKERS_NUM).props(Props.create(DatabaseAccessActor.class, database)));
    }

    public Route createRoute() {
        return userRoutes.routes();
    }
}

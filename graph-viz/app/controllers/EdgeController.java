package controllers;

import actors.BundleActor;
import actors.OriginalActor;
import akka.actor.ActorSystem;
import akka.stream.Materializer;
import play.libs.streams.ActorFlow;
import play.mvc.Controller;
import play.mvc.WebSocket;

import javax.inject.Inject;

public class EdgeController extends Controller {

    private final ActorSystem actorSystem;
    private final Materializer materializer;

    @Inject
    public EdgeController(ActorSystem actorSystem, Materializer materializer) {
        this.actorSystem = actorSystem;
        this.materializer = materializer;
    }

    public WebSocket original() {
        return WebSocket.Text.accept(
                request -> ActorFlow.actorRef(OriginalActor::props, actorSystem, materializer));
    }

    public WebSocket bundle() {
        return WebSocket.Text.accept(
                request -> ActorFlow.actorRef(BundleActor::props, actorSystem, materializer));
    }
}
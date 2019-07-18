package controllers;

import actors.BundleActor;
import actors.EchoActor;
import akka.actor.ActorSystem;
import akka.stream.Materializer;
import play.libs.streams.ActorFlow;
import play.mvc.Controller;
import play.mvc.WebSocket;

import javax.inject.Inject;

public class BundleController extends Controller {

    private final ActorSystem actorSystem;
    private final Materializer materializer;

    @Inject
    public BundleController(ActorSystem actorSystem, Materializer materializer) {
        this.actorSystem = actorSystem;
        this.materializer = materializer;
    }

    public WebSocket socket() {
        return WebSocket.Text.accept(
                request -> ActorFlow.actorRef(BundleActor::props, actorSystem, materializer));
    }
}
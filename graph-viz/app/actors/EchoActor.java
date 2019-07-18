package actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import controllers.GraphController;

public class EchoActor extends AbstractActor {

    private static GraphController graphController = new GraphController();

    public static Props props(ActorRef out) {
        return Props.create(EchoActor.class, out);
    }

    private final ActorRef out;

    public EchoActor(ActorRef out) {
        this.out = out;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(String.class, message -> graphController.index(message, this))
                .build();
    }

    public void returnData(String s) {
        out.tell(s, self());
    }
}


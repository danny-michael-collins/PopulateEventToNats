package com.optiva.tools.load;

import io.nats.client.*;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
public class ManySubjectsPull {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://amsoce03-eh-nats-0.amsoce03-eh-nats.amsoce03-eh.svc.cluster.local:4222")) {
            JetStream js = nc.jetStream();
            for (int i = 0; i < 100; i++) {
                Random randomNum = new Random();
                int filter = randomNum.nextInt(100000) - 1;
                JetStreamSubscription sub = js.subscribe("Events0."+filter, PullSubscribeOptions.builder().build());
                JetStreamReader reader = sub.reader(100, 50);
                String last = "";
                AtomicInteger count = new AtomicInteger();
                Message m = reader.nextMessage(1000);
                while (m != null) {
                    last = processMessage(count, m);
                    m = reader.nextMessage(1000);
                }
                System.out.println("# " + count + " -> " + last);
                Duration duration = Duration.ofSeconds(1);
                sub.drain(duration);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static String processMessage(AtomicInteger count, Message m) {
        String s = m.getSubject();
        int c = count.incrementAndGet();
        if (c == 1) {
            System.out.println("# " + c + " -> " + s);
        }
        else if (c % 1000 == 0) {
            System.out.println("# " + c);
        }
        m.ack();
        return s;
    }
}
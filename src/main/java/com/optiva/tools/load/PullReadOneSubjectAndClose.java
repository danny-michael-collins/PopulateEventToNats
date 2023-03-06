package com.optiva.tools.load;

import com.optiva.tools.addevents.NatsConfiguration;
import com.optiva.tools.addevents.NatsEventPublisher;
import com.optiva.tools.addevents.NatsReaderConfiguration;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamReader;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.ReplayPolicy;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class PullReadOneSubjectAndClose {
    private final NatsConfiguration natsConfiguration;
    private Connection connection;
    private JetStreamSubscription pullSub;

    public PullReadOneSubjectAndClose(NatsConfiguration natsConfiguration) {
        this.natsConfiguration = natsConfiguration;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> close()));
    }

    public void consume() {
        try {
            JetStream js = getConnection().jetStream(NatsEventPublisher.JET_STREAM_OPTIONS);

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                                                            .filterSubject(natsConfiguration.getSubjectName())
                                                            .deliverPolicy(DeliverPolicy.All)
                                                            .replayPolicy(ReplayPolicy.Instant)
                                                            .ackPolicy(AckPolicy.Explicit)
                                                            .build();

            PullSubscribeOptions pullOptions = PullSubscribeOptions.builder().build();
            pullSub = js.subscribe(natsConfiguration.getSubjectName(), pullOptions);

            consumeMessages();
                System.out.printf("Subscription pendingMsgCount %s, dropped Count %s, Delivered Count %s %n ",
                                  pullSub.getPendingMessageCount(),
                                  pullSub.getDroppedCount(),
                                  pullSub.getDeliveredCount());
        } catch (Exception e) {
            close();
            System.out.printf("I/O error communicating to the NATS server :: %s%n", e.getLocalizedMessage());
        } finally {
            close();
            System.exit(0);
        }

    }

    private Connection getConnection() {
        if (connection != null && connection.getStatus() != Connection.Status.CONNECTED) {
            close();
            connection = null;
        }
        return connection != null ? connection : createNatsConnection();
    }

    private Connection createNatsConnection() {
        Arrays.stream(((NatsReaderConfiguration) natsConfiguration).getUrls()).forEach(System.out::println);
        Options connectionOptions = new Options.Builder().servers(((NatsReaderConfiguration) natsConfiguration).getUrls())
                                                         .connectionTimeout(Duration.ofSeconds(30))
                                                         .maxReconnects(-1)
                                                         .reconnectBufferSize(natsConfiguration.getConnectionByteBufferSize())
                                                         .build();
        try {
            connection = Nats.connect(connectionOptions);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        return connection;
    }

    private void close() {
        try {
            if (pullSub != null && pullSub.isActive()) {
                pullSub.unsubscribe();
            }
            if (connection != null) {
                connection.close();
                connection = null;
            }
        } catch (InterruptedException e) {
            connection = null;
            Thread.currentThread().interrupt();
        }
    }

    private void consumeMessages() throws InterruptedException {
        JetStreamReader reader = pullSub.reader(100, 50);
        String last = "";
        AtomicInteger count = new AtomicInteger();
        Message m = reader.nextMessage(1000);
        while (m != null) {
            last = processMessage(count, m);
            m = reader.nextMessage(1000);
        }
        System.out.println("# " + count + " -> " + last);
        Duration duration = Duration.ofSeconds(1);
        pullSub.drain(duration);
    }

    private String processMessage(AtomicInteger count, Message m) {
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
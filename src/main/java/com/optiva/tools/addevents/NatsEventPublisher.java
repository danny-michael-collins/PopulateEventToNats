package com.optiva.tools.addevents;

import com.optiva.tools.load.AsyncPublishingConnectionManager;
import io.nats.client.JetStreamOptions;
import io.nats.client.api.PublishAck;

import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class NatsEventPublisher {

    public static final JetStreamOptions JET_STREAM_OPTIONS = JetStreamOptions.builder().publishNoAck(false).requestTimeout(Duration.ofMinutes(3)).build();
    private final NatsConfiguration natsConfiguration;

    public NatsEventPublisher(final NatsConfiguration configuration) {
        this.natsConfiguration = configuration;
    }

    /**
     * Publish the message to Nats using the JetStream server
     *
     * @param event   serialized JSON event
     * @param subject String representation of message subject e.g. 100.23.66.66
     * @throws NatsEventException custom exception to hold any errors while publishing
     */
    public void publish(ByteArrayOutputStream event, String subject, String streamName) throws NatsEventException {
        try {
            CompletableFuture<PublishAck> futureAck = AsyncPublishingConnectionManager.getInstance().publishAysnc(event, subject, streamName);
            if (futureAck != null) {
                PublishAck ack = futureAck.get(2, TimeUnit.SECONDS);
                if (ack.hasError()) {
                    String errMsg = "NatsEventPublisher#publish --> Message publishing ack returned an error : ";
                    throw new NatsEventException(errMsg + ack.getError());
                }
            } else {
                String errMsg = "NatsEventPublisher#publish --> Future<PublishAck> object was NULL ";
                throw new NatsEventException(errMsg);
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            String errMsg = "NatsEventPublisher#publish --> exception when publishing : ";
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new NatsEventException(errMsg + e.getMessage(), e);
        }
    }

    public void publish(ByteArrayOutputStream event, String subject) throws NatsEventException {
        publish(event, subject, natsConfiguration.getStreamName());
    }
}

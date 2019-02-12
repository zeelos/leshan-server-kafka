/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.zeelos.leshan.server.kafka;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.eclipse.californium.core.Utils;
import org.eclipse.leshan.core.node.LwM2mPath;
import org.eclipse.leshan.core.observation.Observation;
import org.eclipse.leshan.core.request.DownlinkRequest;
import org.eclipse.leshan.core.response.LwM2mResponse;
import org.eclipse.leshan.core.response.ObserveResponse;
import org.eclipse.leshan.core.response.ResponseCallback;
import org.eclipse.leshan.server.LwM2mServer;
import org.eclipse.leshan.server.observation.ObservationListener;
import org.eclipse.leshan.server.observation.ObservationService;
import org.eclipse.leshan.server.registration.Registration;
import org.eclipse.leshan.server.registration.RegistrationListener;
import org.eclipse.leshan.server.registration.RegistrationService;
import org.eclipse.leshan.server.registration.RegistrationUpdate;
import org.eclipse.leshan.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.zeelos.leshan.avro.AvroKey;
import io.zeelos.leshan.avro.registration.AvroRegistrationResponse;
import io.zeelos.leshan.avro.request.AvroRequest;
import io.zeelos.leshan.avro.request.AvroRequestKind;
import io.zeelos.leshan.avro.response.AvroGenericResponse;
import io.zeelos.leshan.avro.response.AvroResponse;
import io.zeelos.leshan.avro.response.AvroResponseCode;
import io.zeelos.leshan.avro.response.AvroResponseObserve;
import io.zeelos.leshan.avro.response.AvroResponsePayload;
import io.zeelos.leshan.server.kafka.serialization.avro.DownlinkRequestSerDes;
import io.zeelos.leshan.server.kafka.serialization.avro.RegistrationDeleteSerDes;
import io.zeelos.leshan.server.kafka.serialization.avro.RegistrationSerDes;
import io.zeelos.leshan.server.kafka.serialization.avro.RegistrationUpdateSerDes;
import io.zeelos.leshan.server.kafka.serialization.avro.ResponseObserveSerDes;
import io.zeelos.leshan.server.kafka.serialization.avro.ResponseSerDes;

/**
 * Responsible for forwarding LWM2M messages to a Kafka broker.
 */
public class KafkaForwarder implements RegistrationListener, ObservationListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaForwarder.class);

    /* registration topic (client registration interface) */
    private final String kafkaRegistrationsTopic;
    /* management topics (management and service enablement interface) */
    private final String kafkaManagementReqTopic;
    private final String kafkaManagementRepTopic;
    /* reporting topic (information reporting interface) */
    private final String kafkaObservationsTopic;

    private final LwM2mServer server;

    private final KafkaProducer<AvroKey, ? extends SpecificRecord> producer;
    private final KafkaConsumer<AvroKey, AvroRequest> consumer;

    private final String serverId;

    private final RegistrationService registrationService;
    private final ObservationService observationService;

    // holds the 'observation-id' to 'ticket' mapping
    private final Map<KafkaForwarder.KeyId, String> observatioIdToTicket;

    private final ExecutorService executorReqRepService;
    private final ExecutorService executorObserveService;

    public KafkaForwarder(LwM2mServer server, String serverId, String kafkaRegistrationsTopic,
            String kafkaObservationsTopic, String kafkaManagementReqTopic, String kafkaManagementRepTopic,
            KafkaProducer<AvroKey, ? extends SpecificRecord> producer, KafkaConsumer<AvroKey, AvroRequest> consumer) {
        this.server = server;
        this.producer = producer;
        this.consumer = consumer;

        this.observatioIdToTicket = new ConcurrentHashMap<>();

        this.registrationService = server.getRegistrationService();
        this.observationService = server.getObservationService();

        this.serverId = serverId;
        this.kafkaRegistrationsTopic = kafkaRegistrationsTopic;
        this.kafkaObservationsTopic = kafkaObservationsTopic;
        this.kafkaManagementReqTopic = kafkaManagementReqTopic;
        this.kafkaManagementRepTopic = kafkaManagementRepTopic;

        this.executorReqRepService = Executors
                .newCachedThreadPool(new NamedThreadFactory("KafkaForwarder reg/req/rep handler %d"));

        this.executorObserveService = Executors
                .newCachedThreadPool(new NamedThreadFactory("KafkaForwarder observation handler %d"));
    }

    public void start() {
        // initialize underlying frameworks

        // setup Leshan callbacks
        registrationService.addListener(this);
        observationService.addListener(this);

        // start Kafka Consumer request handler thread
        Thread kafkaConsumerThread = new KafkaConsumerThread();
        kafkaConsumerThread.start();
    }

    /* *************** Leshan Registration API **************** */
    @Override
    public void registered(final Registration reg, Registration previousReg,
            Collection<Observation> previousObsersations) {
        executorReqRepService.submit(() -> {
            try {
                AvroRegistrationResponse avroReg = RegistrationSerDes.aSerialize(serverId, reg);

                ProducerRecord<AvroKey, AvroRegistrationResponse> record = new ProducerRecord<>(kafkaRegistrationsTopic,
                        AvroKey.newBuilder().setEp(reg.getEndpoint()).build(), avroReg);

                publishMessage(record);

            } catch (Exception e) {
                log.error("Exception occurred:", e);
            }
        });
    }

    @Override
    public void updated(final RegistrationUpdate update, final Registration updatedRegistration,
            final Registration previousRegistration) {
        executorReqRepService.submit(() -> {
            try {
                AvroRegistrationResponse avroRegUpdate = RegistrationUpdateSerDes.aSerialize(serverId, update,
                        updatedRegistration);

                ProducerRecord<AvroKey, AvroRegistrationResponse> record = new ProducerRecord<>(kafkaRegistrationsTopic,
                        AvroKey.newBuilder().setEp(updatedRegistration.getEndpoint()).build(), avroRegUpdate);

                publishMessage(record);

            } catch (Exception e) {
                log.error("Exception occurred:", e);
            }
        });
    }

    @Override
    public void unregistered(final Registration reg, Collection<Observation> observations, boolean expired,
            Registration newReg) {
        executorReqRepService.submit(() -> {
            try {
                AvroRegistrationResponse avroReg = RegistrationDeleteSerDes.aSerialize(serverId, expired, reg);

                ProducerRecord<AvroKey, AvroRegistrationResponse> record = new ProducerRecord<>(kafkaRegistrationsTopic,
                        AvroKey.newBuilder().setEp(reg.getEndpoint()).build(), avroReg);

                publishMessage(record);

            } catch (Exception e) {
                log.error("Exception occurred:", e);
            }
        });
    }

    /* *************** Leshan Observation API **************** */
    @Override
    public void newObservation(Observation observation, Registration registration) {

    }

    @Override
    public void cancelled(Observation observation) {
        observatioIdToTicket.remove(new KafkaForwarder.KeyId(observation.getId()));
    }

    @Override
    public void onResponse(final Observation observation, final Registration registration,
            final ObserveResponse response) {
        executorObserveService.submit(() -> {
            String ticket = null;

            try {
                ticket = observatioIdToTicket.get(new KeyId(observation.getId()));

                handleNotification(ticket, registration.getEndpoint(), response);

            } catch (Exception e) {
                if (ticket != null)
                    sendError(AvroRequestKind.observe, observation.getPath().toString(),
                            AvroResponseCode.INTERNAL_SERVER_ERROR, e.getMessage(), ticket);

                log.error("Exception occurred:", e);
            }
        });
    }

    @Override
    public void onError(Observation observation, Registration registration, Exception error) {
        log.warn(String.format("Unable to handle notification of [%s:%s]", observation.getRegistrationId(),
                observation.getPath()), error);
    }

    private void publishMessage(final ProducerRecord record) {
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Kafka exception occurred: ", exception);
            }
        });
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void handleRequest(final String ticket, final AvroRequest message) {
        log.debug(message.toString());

        final String endpoint = message.getEp();

        // Get the registration for this endpoint
        Registration destination = registrationService.getByEndpoint(endpoint);
        if (destination == null) {
            sendError(message.getPayload().getKind(), /* copy the original request kind */
                    message.getPayload().getPath(), /* copy the original request path */
                    AvroResponseCode.NOT_FOUND, String.format("No registration for this endpoint '%s'", endpoint),
                    ticket);
            return;
        }

        // 'cancel observation' request is handled differently than others
        if (message.getPayload().getKind() == AvroRequestKind.observeCancel) {
            server.getObservationService().cancelObservations(destination, message.getPayload().getPath());
            return;
        }

        // Deserialize Request
        final DownlinkRequest<?> request = DownlinkRequestSerDes.deserialize(message);

        // Send it
        server.send(destination, request, (ResponseCallback) response -> {
            try {
                handleResponse(ticket, endpoint, request.getPath(), message.getPayload().getKind(), response);
            } catch (Exception e) {
                log.error("Exception occured: ", e);
            }
        }, e -> {
            // throws runtime exception with message:null
            // try to append the class name as an indication
            // of the error
            sendError(message.getPayload().getKind(), /* copy the original request kind */
                    message.getPayload().getPath(), /* copy the original request path */
                    AvroResponseCode.INTERNAL_SERVER_ERROR,
                    String.format("%s : %s", e.getClass().getName(), e.getMessage()), ticket);
        });
    }

    private void handleNotification(String ticket, String endpoint, ObserveResponse response) throws Exception {
        // setup kafka payload from the coap response
        AvroResponseObserve payload = ResponseObserveSerDes.aSerialize(serverId, ticket, endpoint, response);

        // construct kafka message
        ProducerRecord<AvroKey, AvroResponseObserve> record = new ProducerRecord<>(kafkaObservationsTopic,
                AvroKey.newBuilder().setEp(endpoint).build(), payload);

        // finally, publish to kafka
        publishMessage(record);
    }

    private void handleResponse(String ticket, String endpoint, LwM2mPath path, AvroRequestKind kind,
            LwM2mResponse response) throws Exception {
        // if OBSERVE response update observatioIdToTicket mapping
        if (response instanceof ObserveResponse) {
            Observation observation = ((ObserveResponse) response).getObservation();
            observatioIdToTicket.put(new KeyId(observation.getId()), ticket);
        }

        // setup kafka payload from the coap response
        AvroResponse payload = ResponseSerDes.aSerialize(serverId, ticket, endpoint, path, kind, response);

        // construct kafka message
        ProducerRecord<AvroKey, AvroResponse> record = new ProducerRecord<>(kafkaManagementRepTopic,
                AvroKey.newBuilder().setEp(endpoint).build(), payload);

        // finally, publish to kafka
        publishMessage(record);
    }

    private void sendError(final AvroRequestKind kind, String path, final AvroResponseCode code, final String error,
            final String ticket) {
        AvroResponse.Builder builder = AvroResponse.newBuilder();
        builder.setServerId(serverId);
        builder.setEp(serverId); // set both on error
        builder.setTicket(ticket);
        builder.setPath(path);
        builder.setTimestamp(System.currentTimeMillis());

        AvroResponsePayload.Builder payloadBuilder = AvroResponsePayload.newBuilder();
        payloadBuilder.setCode(code);
        payloadBuilder.setKind(kind);

        AvroGenericResponse.Builder aGenericResponseBuilder = AvroGenericResponse.newBuilder();
        aGenericResponseBuilder.setMessage(error);
        payloadBuilder.setBody(aGenericResponseBuilder.build());

        builder.setRep(payloadBuilder.build());

        // construct kafka message
        ProducerRecord<AvroKey, AvroResponse> record = new ProducerRecord<>(kafkaManagementRepTopic,
                AvroKey.newBuilder().setEp(serverId).build(), builder.build());

        // finally, publish to kafka
        publishMessage(record);
    }

    public void shutdown() {
        try {
            consumer.wakeup(); // instruct kafka consumer to break 'poll()' and close

            // finish all pending 'producing' tasks
            executorReqRepService.shutdown();
            executorReqRepService.awaitTermination(3, TimeUnit.SECONDS);
            executorObserveService.shutdown();
            executorObserveService.awaitTermination(3, TimeUnit.SECONDS);

        } catch (InterruptedException e) {
            executorReqRepService.shutdownNow();
            executorObserveService.shutdownNow();
        } finally {
            producer.close();
        }
    }

    private final class KafkaConsumerThread extends Thread {

        KafkaConsumerThread() {
            super("Kafka Consumer Main Thread");
        }

        @Override
        public void run() {
            try {
                consumer.subscribe(Collections.singletonList(kafkaManagementReqTopic));

                while (true) {
                    ConsumerRecords<AvroKey, AvroRequest> records = consumer.poll(Long.MAX_VALUE);

                    records.forEach(record -> {
                        executorReqRepService.submit(() -> handleRequest(record.value().getTicket(), record.value()));
                    });
                    consumer.commitSync();
                }
            } catch (WakeupException e) {
                // occurs upon 'shutdown' request, ignore it
            } finally {
                consumer.close(); // always close
            }
        }
    }

    private static final class KeyId {

        protected final byte[] id;
        private final int hash;

        public KeyId(byte[] token) {
            if (token == null)
                throw new NullPointerException();
            this.id = token;
            this.hash = Arrays.hashCode(token);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof KafkaForwarder.KeyId))
                return false;
            KafkaForwarder.KeyId key = (KafkaForwarder.KeyId) o;
            return Arrays.equals(id, key.id);
        }

        @Override
        public String toString() {
            return "KeyId[" + Utils.toHexString(id) + "]";
        }
    }
}

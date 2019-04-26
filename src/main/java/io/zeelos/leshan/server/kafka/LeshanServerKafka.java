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

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.zeelos.leshan.avro.AvroKey;
import io.zeelos.leshan.avro.request.AvroRequest;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.eclipse.californium.core.network.config.NetworkConfig;
import org.eclipse.californium.scandium.config.DtlsConnectorConfig;
import org.eclipse.californium.scandium.config.DtlsConnectorConfig.Builder;
import org.eclipse.californium.scandium.dtls.CertificateMessage;
import org.eclipse.californium.scandium.dtls.DTLSSession;
import org.eclipse.californium.scandium.dtls.HandshakeException;
import org.eclipse.californium.scandium.dtls.x509.CertificateVerifier;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.webapp.WebAppContext;
import org.eclipse.leshan.LwM2m;
import org.eclipse.leshan.core.model.ObjectLoader;
import org.eclipse.leshan.core.model.ObjectModel;
import org.eclipse.leshan.core.node.codec.DefaultLwM2mNodeDecoder;
import org.eclipse.leshan.core.node.codec.DefaultLwM2mNodeEncoder;
import org.eclipse.leshan.core.node.codec.LwM2mNodeDecoder;
import org.eclipse.leshan.server.californium.LeshanServerBuilder;
import org.eclipse.leshan.server.californium.impl.LeshanServer;
import org.eclipse.leshan.server.demo.LeshanServerDemo;
import org.eclipse.leshan.server.demo.servlet.ClientServlet;
import org.eclipse.leshan.server.demo.servlet.EventServlet;
import org.eclipse.leshan.server.demo.servlet.ObjectSpecServlet;
import org.eclipse.leshan.server.demo.servlet.SecurityServlet;
import org.eclipse.leshan.server.impl.FileSecurityStore;
import org.eclipse.leshan.server.model.LwM2mModelProvider;
import org.eclipse.leshan.server.model.StaticModelProvider;
import org.eclipse.leshan.server.security.EditableSecurityStore;
import org.eclipse.leshan.util.SecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jmdns.JmDNS;
import javax.jmdns.ServiceInfo;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.BindException;
import java.net.InetAddress;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

/**
 * The main entry point for the Leshan LWM2M to Kafka forwarder.
 */
public class LeshanServerKafka {

    private static final Logger log = LoggerFactory.getLogger(LeshanServerKafka.class);

    private final static String[] modelPaths = new String[] { "31024.xml",
            /* add modbus support */
            "26241.xml",

            "10241.xml", "10242.xml", "10243.xml", "10244.xml", "10245.xml", "10246.xml", "10247.xml", "10248.xml",
            "10249.xml", "10250.xml",

            "2048.xml", "2049.xml", "2050.xml", "2051.xml", "2052.xml", "2053.xml", "2054.xml", "2055.xml", "2056.xml",
            "2057.xml",

            "3200.xml", "3201.xml", "3202.xml", "3203.xml", "3300.xml", "3301.xml", "3302.xml", "3303.xml", "3304.xml",
            "3305.xml", "3306.xml", "3308.xml", "3310.xml", "3311.xml", "3312.xml", "3313.xml", "3314.xml", "3315.xml",
            "3316.xml", "3317.xml", "3318.xml", "3319.xml", "3320.xml", "3321.xml", "3322.xml", "3323.xml", "3324.xml",
            "3325.xml", "3326.xml", "3327.xml", "3328.xml", "3329.xml", "3330.xml", "3331.xml", "3332.xml", "3333.xml",
            "3334.xml", "3335.xml", "3336.xml", "3337.xml", "3338.xml", "3339.xml", "3340.xml", "3341.xml", "3342.xml",
            "3343.xml", "3344.xml", "3345.xml", "3346.xml", "3347.xml", "3348.xml",

            "Communication_Characteristics-V1_0.xml",

            "LWM2M_Lock_and_Wipe-V1_0.xml", "LWM2M_Cellular_connectivity-v1_0.xml",
            "LWM2M_APN_connection_profile-v1_0.xml", "LWM2M_WLAN_connectivity4-v1_0.xml",
            "LWM2M_Bearer_selection-v1_0.xml", "LWM2M_Portfolio-v1_0.xml", "LWM2M_DevCapMgmt-v1_0.xml",
            "LWM2M_Software_Component-v1_0.xml", "LWM2M_Software_Management-v1_0.xml",

            "Non-Access_Stratum_NAS_configuration-V1_0.xml" };

    private final static String USAGE = "java -jar leshan-server-demo.jar [OPTION]";

    private final static String DEFAULT_KEYSTORE_TYPE = KeyStore.getDefaultType();

    private final static String DEFAULT_KEYSTORE_ALIAS = "leshan";

    public static void main(String[] args) {
        // Define options for command line tools
        Options options = new Options();

        final StringBuilder X509Chapter = new StringBuilder();
        X509Chapter.append("\n .");
        X509Chapter.append("\n .");
        X509Chapter.append("\n ===============================[ X509 ]=================================");
        X509Chapter.append("\n | By default Leshan demo uses an embedded self-signed certificate and  |");
        X509Chapter.append("\n | trusts any client certificates.                                      |");
        X509Chapter.append("\n | If you want to use your own server keys, certificates and truststore,|");
        X509Chapter.append("\n | you can provide a keystore using -ks, -ksp, -kst, -ksa, -ksap.       |");
        X509Chapter.append("\n | To get helps about files format and how to generate it, see :        |");
        X509Chapter.append("\n | See https://github.com/eclipse/leshan/wiki/Credential-files-format   |");
        X509Chapter.append("\n ------------------------------------------------------------------------");

        options.addOption("h", "help", false, "Display help information.");
        options.addOption("n", "serverID", true, "Sets the unique identifier of this LWM2M server instance.");
        options.addOption("lh", "coaphost", true, "Set the local CoAP address.\n  Default: any local address.");
        options.addOption("lp", "coapport", true,
                String.format("Set the local CoAP port.\n  Default: %d.", LwM2m.DEFAULT_COAP_PORT));
        options.addOption("slh", "coapshost", true, "Set the secure local CoAP address.\nDefault: any local address.");
        options.addOption("slp", "coapsport", true,
                String.format("Set the secure local CoAP port.\nDefault: %d.", LwM2m.DEFAULT_COAP_SECURE_PORT));
        options.addOption("ks", "keystore", true,
                "Set the key store file. If set, X.509 mode is enabled, otherwise built-in RPK credentials are used.");
        options.addOption("ksp", "storepass", true, "Set the key store password.");
        options.addOption("kst", "storetype", true,
                String.format("Set the key store type.\nDefault: %s.", DEFAULT_KEYSTORE_TYPE));
        options.addOption("ksa", "alias", true, String.format(
                "Set the key store alias to use for server credentials.\nDefault: %s.", DEFAULT_KEYSTORE_ALIAS));
        options.addOption("ksap", "keypass", true, "Set the key store alias password to use.");
        options.addOption("wp", "webport", true, "Set the HTTP port for web server.\nDefault: 8080.");
        options.addOption("m", "modelsfolder", true, "A folder which contains object models in OMA DDF(.xml) format.");
        options.addOption("mdns", "publishDNSSdServices", false,
                "Publish leshan's services to DNS Service discovery" + X509Chapter);

        // kafka options
        options.addOption("kr", "regtopic", true,
                "Sets the kafka topic where the registration responses would be send.");
        options.addOption("ko", "observetopic", true,
                "Sets the kafka topic where the observation responses would be send.");
        options.addOption("kmr", "mngreqtopic", true,
                "Sets the kafka topic where the management requests would be send and this server will listen to.");
        options.addOption("kmp", "mngreptopic", true,
                "Sets the kafka topic where the management responses would be send.");
        options.addOption("kpc", "prdcompress", true, "Sets the compression algorithm for the producer.");
        options.addOption("kpb", "prdbatch", true, "Sets the batch size config for the producer.");
        options.addOption("kpl", "prdlingerms", true, "Sets the linger ms for the producer.");
        options.addOption("kb", "kbootstrap", true, "Set Kafka bootstrap server.");
        options.addOption("ksh", "kschema", true, "Set Kafka Schema Registry URL.");
        options.addOption("kssl", "kssl", true, "Set Kafka SSL properties file.");

        HelpFormatter formatter = new HelpFormatter();
        formatter.setOptionComparator(null);

        // Parse arguments
        CommandLine cl;
        try {
            cl = new DefaultParser().parse(options, args);
        } catch (ParseException e) {
            System.err.println("Parsing failed.  Reason: " + e.getMessage());
            formatter.printHelp(USAGE, options);
            return;
        }

        // Print help
        if (cl.hasOption("help")) {
            formatter.printHelp(USAGE, options);
            return;
        }

        // Abort if unexpected options
        if (cl.getArgs().length > 0) {
            System.err.println("Unexpected option or arguments : " + cl.getArgList());
            formatter.printHelp(USAGE, options);
            return;
        }

        // get local address
        String localAddress = cl.getOptionValue("lh");
        String localPortOption = cl.getOptionValue("lp");
        int localPort = LwM2m.DEFAULT_COAP_PORT;
        if (localPortOption != null) {
            localPort = Integer.parseInt(localPortOption);
        }

        // get secure local address
        String secureLocalAddress = cl.getOptionValue("slh");
        String secureLocalPortOption = cl.getOptionValue("slp");
        int secureLocalPort = LwM2m.DEFAULT_COAP_SECURE_PORT;
        if (secureLocalPortOption != null) {
            secureLocalPort = Integer.parseInt(secureLocalPortOption);
        }

        // get http port
        String webPortOption = cl.getOptionValue("wp");
        int webPort = 8080;
        if (webPortOption != null) {
            webPort = Integer.parseInt(webPortOption);
        }

        // Get models folder
        String modelsFolderPath = cl.getOptionValue("m");

        // Get keystore parameters
        String keyStorePath = cl.getOptionValue("ks");
        String keyStoreType = cl.getOptionValue("kst", KeyStore.getDefaultType());
        String keyStorePass = cl.getOptionValue("ksp");
        String keyStoreAlias = cl.getOptionValue("ksa");
        String keyStoreAliasPass = cl.getOptionValue("ksap");

        // get server Id
        String serverId = cl.getOptionValue("n", System.getenv("SERVERID"));
        if (serverId == null) {
            System.err.println("'serverId' is mandatory !");
            formatter.printHelp(USAGE, options);
            return;
        }

        // get kafka topic naming for 'registrations/observations/management'
        String kafkaRegistrationsTopic = cl.getOptionValue("kr", System.getenv("KAFKA_TOPIC_REGISTRATIONS"));
        String kafkaObservationsTopic = cl.getOptionValue("ko", System.getenv("KAFKA_TOPIC_OBSERVATIONS"));
        String kafkaManagementReqTopic = cl.getOptionValue("kmr", System.getenv("KAFKA_TOPIC_MANAGEMENT_REQ"));
        String kafkaManagementRepTopic = cl.getOptionValue("kmp", System.getenv("KAFKA_TOPIC_MANAGEMENT_REP"));

        if (kafkaRegistrationsTopic == null || kafkaObservationsTopic == null || kafkaManagementReqTopic == null
                || kafkaManagementRepTopic == null) {
            System.err.println("missing kafka topic naming parameters where messages would be send and receive!");
            formatter.printHelp(USAGE, options);
            return;
        }

        // get kafka bootstrap server
        String kafkaBootstrapServer = cl.getOptionValue("kb", System.getenv("KAFKA_BOOTSTRAP_SERVER"));
        // get kafka schema registry server
        String kafkaSchemaRegistryURL = cl.getOptionValue("ksh", System.getenv("KAFKA_SCHEMA_REGISTRY_URL"));

        if (kafkaBootstrapServer == null || kafkaSchemaRegistryURL == null) {
            System.err.println("missing kafka bootstrap/schema registry parameters!");
            formatter.printHelp(USAGE, options);
            return;
        }

        // load kafka ssl properties files (if any)
        String sslkafkaPropsFile = cl.getOptionValue("kssl", System.getenv("KAFKA_SSL_PROPERTIES_LOCATION"));
        Properties sslPros = new Properties();
        if (sslkafkaPropsFile != null) {
            try {
                InputStream is = new FileInputStream(sslkafkaPropsFile);
                sslPros.load(is);

            } catch (IOException e) {
                log.error("Unable to load Kafka SSL properties file..", e);
                System.exit(1);
            }
        }

        String producerCompressionType = cl.getOptionValue("kpc", System.getenv("KAFKA_PRODUCER_COMPRESSION_TYPE"));
        if (producerCompressionType == null) {
            producerCompressionType = "none"; // see ProducerConfig.COMPRESSION_TYPE_CONFIG
        }

        String producerBatchSize = cl.getOptionValue("kpb", System.getenv("KAFKA_PRODUCER_BATCH_SIZE_CONFIG"));
        if (producerBatchSize == null) {
            producerBatchSize = "16384"; // see ProducerConfig.BATCH_SIZE_CONFIG
        }
        String producerLingerMs = cl.getOptionValue("kpl", System.getenv("KAFKA_PRODUCER_LINGER_MS"));
        if (producerLingerMs == null) {
            producerLingerMs = "0"; // see ProducerConfig.LINGER_MS_CONFIG
        }

        // Get mDNS publish switch
        Boolean publishDNSSdServices = cl.hasOption("mdns");

        try {
            LeshanServer lwm2mServer = createAndStartServer(webPort, localAddress, localPort, secureLocalAddress,
                    secureLocalPort, modelsFolderPath, keyStorePath, keyStoreType, keyStorePass, keyStoreAlias,
                    keyStoreAliasPass, publishDNSSdServices);

            KafkaForwarder kafkaForwarder = connectToKafkaBroker(serverId, kafkaBootstrapServer, kafkaSchemaRegistryURL,
                    kafkaRegistrationsTopic, kafkaObservationsTopic, kafkaManagementReqTopic, kafkaManagementRepTopic,
                    producerCompressionType, producerBatchSize, producerLingerMs, sslPros, lwm2mServer);

            // add shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    log.info("Shutting down..");

                    kafkaForwarder.shutdown();
                    lwm2mServer.stop();
                }
            });

        } catch (BindException e) {
            System.err.println(
                    String.format("Web port %s is already used, you could change it using 'webport' option.", webPort));
            formatter.printHelp(USAGE, options);
            System.exit(1);
        } catch (Exception e) {
            log.error("An unexpected exception occured:", e);
            System.exit(1);
        }
    }

    public static LeshanServer createAndStartServer(int webPort, String localAddress, int localPort,
            String secureLocalAddress, int secureLocalPort, String modelsFolderPath, String keyStorePath,
            String keyStoreType, String keyStorePass, String keyStoreAlias, String keyStoreAliasPass,
            boolean publishDNSSdServices) throws Exception {
        // Prepare LWM2M server
        LeshanServerBuilder builder = new LeshanServerBuilder();
        builder.setLocalAddress(localAddress, localPort);
        builder.setLocalSecureAddress(secureLocalAddress, secureLocalPort);
        builder.setEncoder(new DefaultLwM2mNodeEncoder());
        LwM2mNodeDecoder decoder = new DefaultLwM2mNodeDecoder();
        builder.setDecoder(decoder);

        // Create CoAP Config
        NetworkConfig coapConfig;
        File configFile = new File(NetworkConfig.DEFAULT_FILE_NAME);
        if (configFile.isFile()) {
            coapConfig = new NetworkConfig();
            coapConfig.load(configFile);
        } else {
            coapConfig = LeshanServerBuilder.createDefaultNetworkConfig();
            coapConfig.store(configFile);
        }
        builder.setCoapConfig(coapConfig);

        X509Certificate serverCertificate = null;

        // Set up X.509 mode
        if (keyStorePath != null) {
            try {
                KeyStore keyStore = KeyStore.getInstance(keyStoreType);
                try (FileInputStream fis = new FileInputStream(keyStorePath)) {
                    keyStore.load(fis, keyStorePass == null ? null : keyStorePass.toCharArray());
                    List<Certificate> trustedCertificates = new ArrayList<>();
                    for (Enumeration<String> aliases = keyStore.aliases(); aliases.hasMoreElements();) {
                        String alias = aliases.nextElement();
                        if (keyStore.isCertificateEntry(alias)) {
                            trustedCertificates.add(keyStore.getCertificate(alias));
                        } else if (keyStore.isKeyEntry(alias) && alias.equals(keyStoreAlias)) {
                            List<X509Certificate> x509CertificateChain = new ArrayList<>();
                            Certificate[] certificateChain = keyStore.getCertificateChain(alias);
                            if (certificateChain == null || certificateChain.length == 0) {
                                log.error("Keystore alias must have a non-empty chain of X509Certificates.");
                                System.exit(-1);
                            }

                            for (Certificate certificate : certificateChain) {
                                if (!(certificate instanceof X509Certificate)) {
                                    log.error("Non-X.509 certificate in alias chain is not supported: {}", certificate);
                                    System.exit(-1);
                                }
                                x509CertificateChain.add((X509Certificate) certificate);
                            }

                            Key key = keyStore.getKey(alias,
                                    keyStoreAliasPass == null ? new char[0] : keyStoreAliasPass.toCharArray());
                            if (!(key instanceof PrivateKey)) {
                                log.error("Keystore alias must have a PrivateKey entry, was {}",
                                        key == null ? null : key.getClass().getName());
                                System.exit(-1);
                            }
                            builder.setPrivateKey((PrivateKey) key);
                            serverCertificate = (X509Certificate) keyStore.getCertificate(alias);
                            builder.setCertificateChain(
                                    x509CertificateChain.toArray(new X509Certificate[x509CertificateChain.size()]));
                        }
                    }
                    builder.setTrustedCertificates(
                            trustedCertificates.toArray(new Certificate[trustedCertificates.size()]));
                }
            } catch (KeyStoreException | IOException e) {
                log.error("Unable to initialize X.509.", e);
                System.exit(-1);
            }
        }
        // Otherwise, set up RPK mode
        else {
            try {
                PrivateKey privateKey = SecurityUtil.privateKey.readFromResource("credentials/server_privkey.der");
                serverCertificate = SecurityUtil.certificate.readFromResource("credentials/server_cert.der");
                builder.setPrivateKey(privateKey);
                builder.setCertificateChain(new X509Certificate[] { serverCertificate });

                // Use a certificate verifier which trust all certificates by default.
                Builder dtlsConfigBuilder = new DtlsConnectorConfig.Builder();
                dtlsConfigBuilder.setCertificateVerifier(new CertificateVerifier() {
                    @Override
                    public void verifyCertificate(CertificateMessage message, DTLSSession session)
                            throws HandshakeException {
                        // trust all means never raise HandshakeException
                    }

                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }
                });
                builder.setDtlsConfig(dtlsConfigBuilder);

            } catch (Exception e) {
                log.error("Unable to load embedded X.509 certificate.", e);
                System.exit(-1);
            }
        }

        // Define model provider
        List<ObjectModel> models = ObjectLoader.loadDefault();
        models.addAll(ObjectLoader.loadDdfResources("/models/", modelPaths));
        if (modelsFolderPath != null) {
            models.addAll(ObjectLoader.loadObjectsFromDir(new File(modelsFolderPath)));
        }
        LwM2mModelProvider modelProvider = new StaticModelProvider(models);
        builder.setObjectModelProvider(modelProvider);

        // Set securityStore & registrationStore
        EditableSecurityStore securityStore = new FileSecurityStore();
        builder.setSecurityStore(securityStore);

        // Create and start LWM2M server
        LeshanServer lwServer = builder.build();

        // Now prepare Jetty
        Server server = new Server(webPort);
        WebAppContext root = new WebAppContext();
        root.setContextPath("/");
        root.setResourceBase(LeshanServerDemo.class.getClassLoader().getResource("webapp").toExternalForm());
        root.setParentLoaderPriority(true);
        server.setHandler(root);

        // Create Servlet
        EventServlet eventServlet = new EventServlet(lwServer, lwServer.getSecuredAddress().getPort());
        ServletHolder eventServletHolder = new ServletHolder(eventServlet);
        root.addServlet(eventServletHolder, "/event/*");

        ServletHolder clientServletHolder = new ServletHolder(new ClientServlet(lwServer));
        root.addServlet(clientServletHolder, "/api/clients/*");

        ServletHolder securityServletHolder = new ServletHolder(new SecurityServlet(securityStore, serverCertificate));
        root.addServlet(securityServletHolder, "/api/security/*");

        ServletHolder objectSpecServletHolder = new ServletHolder(new ObjectSpecServlet(lwServer.getModelProvider()));
        root.addServlet(objectSpecServletHolder, "/api/objectspecs/*");

        // Register a service to DNS-SD
        if (publishDNSSdServices) {

            // Create a JmDNS instance
            JmDNS jmdns = JmDNS.create(InetAddress.getLocalHost());

            // Publish Leshan HTTP Service
            ServiceInfo httpServiceInfo = ServiceInfo.create("_http._tcp.local.", "leshan", webPort, "");
            jmdns.registerService(httpServiceInfo);

            // Publish Leshan CoAP Service
            ServiceInfo coapServiceInfo = ServiceInfo.create("_coap._udp.local.", "leshan", localPort, "");
            jmdns.registerService(coapServiceInfo);

            // Publish Leshan Secure CoAP Service
            ServiceInfo coapSecureServiceInfo = ServiceInfo.create("_coaps._udp.local.", "leshan", secureLocalPort, "");
            jmdns.registerService(coapSecureServiceInfo);
        }

        // Start Jetty & Leshan
        lwServer.start();
        server.start();
        log.info("Web server started at {}.", server.getURI());

        return lwServer;
    }

    public static KafkaForwarder connectToKafkaBroker(String serverId, String kafkaBootstrapServer,
            String kafkaSchemaRegistryURL, String kafkaRegistrationsTopic, String kafkaObservationsTopic,
            String kafkaManagementReqTopic, String kafkaManagementRepTopic, String producerCompressionType,
            String producerBatchSize, String producerLingerMs, Properties sslProps, LeshanServer lwServer) {
        log.info("Setting up connection to Kafka broker '{}' and Schema Registry '{}' ", kafkaBootstrapServer,
                kafkaSchemaRegistryURL);

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        // TODO: should be really user configurable depending on edge machine
        // capabilities
        producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, producerCompressionType);
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.valueOf(producerBatchSize));
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, Integer.valueOf(producerLingerMs));
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 300 * 1000); // 5 mins
        /*
         * "We recommend testing how long it takes to recover from a crashed broker
         * (i.e., how long until all partitions get new leaders) and setting the number
         * of retries and delay between them such that the total amount of time spent
         * retrying will be longer than the time it takes the Kafka cluster to recover
         * from the crash—otherwise, the producer will give up too soon."
         *
         * Excerpt from
         * "Kafka - The Definitive Guide by Neha Narkhede,‎ Gwen Shapira,‎ Todd Palino"
         */
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 20); // 20 retries
        producerProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 6000); // 6 sec's between retries (in total wait ~2
                                                                         // min before giving up)

        producerProps.put("schema.registry.url", kafkaSchemaRegistryURL);

        // set kafka ssl props (if any)
        producerProps.putAll(sslProps);

        KafkaProducer<AvroKey, ? extends SpecificRecord> producer = new KafkaProducer<>(producerProps);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, serverId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProps.put("schema.registry.url", kafkaSchemaRegistryURL);
        consumerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        // set kafka ssl props (if any)
        consumerProps.putAll(sslProps);

        KafkaConsumer<AvroKey, AvroRequest> consumer = new KafkaConsumer<>(consumerProps);

        // setup forwarder and start
        KafkaForwarder forwarder = new KafkaForwarder(lwServer, serverId, kafkaRegistrationsTopic,
                kafkaObservationsTopic, kafkaManagementReqTopic, kafkaManagementRepTopic, producer, consumer);
        forwarder.start();

        return forwarder;
    }
}

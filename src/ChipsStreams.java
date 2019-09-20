/**
 * Copyright (c) 2016, 2019, Oracle and/or its affiliates. All rights reserved.
 */
import static java.nio.charset.StandardCharsets.UTF_8;
import com.google.common.util.concurrent.Uninterruptibles;
import com.oracle.bmc.ConfigFileReader;
import com.oracle.bmc.auth.AuthenticationDetailsProvider;
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider;
import com.oracle.bmc.streaming.StreamAdminClient;
import com.oracle.bmc.streaming.StreamClient;
import com.oracle.bmc.streaming.model.*;
import com.oracle.bmc.streaming.model.CreateCursorDetails.Type;
import com.oracle.bmc.streaming.model.Stream.LifecycleState;
import com.oracle.bmc.streaming.requests.CreateCursorRequest;
import com.oracle.bmc.streaming.requests.CreateGroupCursorRequest;
import com.oracle.bmc.streaming.requests.CreateStreamRequest;
import com.oracle.bmc.streaming.requests.DeleteStreamRequest;
import com.oracle.bmc.streaming.requests.GetMessagesRequest;
import com.oracle.bmc.streaming.requests.GetStreamRequest;
import com.oracle.bmc.streaming.requests.ListStreamsRequest;
import com.oracle.bmc.streaming.requests.PutMessagesRequest;
import com.oracle.bmc.streaming.responses.CreateCursorResponse;
import com.oracle.bmc.streaming.responses.CreateGroupCursorResponse;
import com.oracle.bmc.streaming.responses.CreateStreamResponse;
import com.oracle.bmc.streaming.responses.GetMessagesResponse;
import com.oracle.bmc.streaming.responses.GetStreamResponse;
import com.oracle.bmc.streaming.responses.ListStreamsResponse;
import com.oracle.bmc.streaming.responses.PutMessagesResponse;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides an example of basic streaming usage.
 * - List streams
 * - Get a stream
 * - Create a stream
 * - Delete a stream
 * - Publish to a stream
 * - Consume from a stream partition, using a cursor
 * - Consume from a stream, using a group cursor
 */
public class ChipsStreams {
    // Set the logger to the class.
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getSimpleName());

    public static void main(String[] args) throws Exception {
        final String configurationFilePath = ".oci/config";
        final String profile = "DEFAULT";

        //Testing Config file output
        ConfigFileReader.ConfigFile config = ConfigFileReader.parse(configurationFilePath, profile);
        logger.info("Check to see if config parameters are pulling correctly:");
        logger.info("User:"+config.get("user"));
        logger.info("OCI Compartment ID set to:"+config.get("compartment-id"));
        logger.info("Fingerprint:"+config.get("fingerprint"));
        logger.info("Tenancy:"+config.get("tenancy"));

        //Create Connection
        final AuthenticationDetailsProvider provider = new ConfigFileAuthenticationDetailsProvider(configurationFilePath, profile);
        // Create an admin-client
        final StreamAdminClient adminClient = new StreamAdminClient(provider);
        //
        final String compartmentId = config.get("compartment-id");
        final String StreamName = "chips_stream";
        final int partitions = 1;
         String streamId ="";

        Stream stream ;
        //Create a Stream
        if (activeStreamCheck(adminClient,compartmentId, StreamName)) {
            logger.info("Stream exists, proceed to next step.");
            stream = getStream(adminClient,compartmentId, StreamName);
            logger.info("Stream id set to: "+stream.getId());
            logger.info("Stream endpoint set to: "+stream.getMessagesEndpoint());
        }
        else {
            logger.info("Creating Stream");
            stream =  createStream(adminClient,compartmentId, StreamName, partitions);
            if (streamBuildCheck(adminClient,compartmentId, StreamName)) {
                logger.info("Stream creation complete, proceed to next step.");
                logger.info("Stream id set to: "+stream.getId());
            }
        }

/*
        // Streams are assigned a specific endpoint url based on where they are provisioned.
        // Create a stream client using the provided message endpoint.
        StreamClient streamClient = new StreamClient(provider);
        streamClient.setEndpoint(stream.getMessagesEndpoint());
        streamId = stream.getId();
        // publish some messages to the stream
        publishMessage(streamClient, streamId, "1","Chip is awesome.");

 */

    }


    private static void deleteStream(StreamAdminClient adminClient, String streamId) {
        logger.info("Deleting stream " + streamId);
        adminClient.deleteStream(DeleteStreamRequest.builder().streamId(streamId).build());
    }





    private static Stream getStream(StreamAdminClient adminClient, String compartmentId, String StreamName) {
        String streamId = "";

        ListStreamsRequest listRequestActive = ListStreamsRequest.builder()
                .compartmentId(compartmentId)
                .lifecycleState(LifecycleState.Active)
                .name(StreamName)
                .build();
        ListStreamsResponse listResponse = adminClient.listStreams(listRequestActive);
        if (!listResponse.getItems().isEmpty()) {

            for (StreamSummary value : listResponse.getItems()) {
                if (value.getName().equals(StreamName)) {
                    streamId = value.getId();
                }
            }

        }
        GetStreamResponse getResponse = adminClient.getStream(GetStreamRequest.builder().streamId(streamId).build());
        return getResponse.getStream();
    }

    //Baseline check to see if stream exists.
    private static boolean activeStreamCheck(StreamAdminClient adminClient, String compartmentId, String StreamName){

        //wait for async completion
        AtomicBoolean isActive = new AtomicBoolean(false);

        ListStreamsRequest listRequestActive =  ListStreamsRequest.builder()
                .compartmentId(compartmentId)
                .lifecycleState(LifecycleState.Active)
                .name(StreamName)
                .build();
        logger.info("Checking for Active streams.");
        ListStreamsResponse listResponse = adminClient.listStreams(listRequestActive);
        //check for name of your stream in active response
        if (!listResponse.getItems().isEmpty()) {
            //if stream is in array break loop return true
            listResponse.getItems().forEach(value -> {
                if (value.getName().equals(StreamName)) {
                    isActive.set(true);
                }
                else {
                    isActive.set(false);
                }
            });
        }
        //catchall return
        return isActive.get();
    }


    //code to check and display status of stream being active
    private static boolean streamBuildCheck(StreamAdminClient adminClient, String compartmentId, String StreamName)
            throws Exception {
//Check to see if Stream active by creating request
        //Check to see if Stream active by creating request
        ListStreamsRequest listRequestCreating =  ListStreamsRequest.builder()
                .compartmentId(compartmentId)
                .lifecycleState(LifecycleState.Creating)
                .name(StreamName)
                .build();

        ListStreamsRequest listRequestActive =  ListStreamsRequest.builder()
                .compartmentId(compartmentId)
                .lifecycleState(LifecycleState.Active)
                .name(StreamName)
                .build();

        ListStreamsRequest listRequestFailed =  ListStreamsRequest.builder()
                .compartmentId(compartmentId)
                .lifecycleState(LifecycleState.Failed)
                .name(StreamName)
                .build();


        //wait for async completion
        AtomicBoolean ready = new AtomicBoolean(true);
        ListStreamsResponse listResponse;
        int i = 0;
        do {
            i++;
            logger.info("Checking for Streams being created. Attempt number: "+ i);
            listResponse = adminClient.listStreams(listRequestCreating);
            logger.info("There are: "+listResponse.getItems().size()+" streams being created.");
            logger.info("Checking for Active streams. Attempt number: "+ i);
            listResponse = adminClient.listStreams(listRequestActive);

            //check for name of your stream in active response
            if (!listResponse.getItems().isEmpty()) {
                //if stream is in array break loop return true
                listResponse.getItems().forEach(value -> {
                    if (value.getName().equals(StreamName)) {
                        ready.set(false);
                        logger.info("Your stream"+value.getName()+" is active with id:" + value.getId());

                    }
                });

            }
                // if > 100 tries check for errored stream creation and output result
                if (i>10) {
                    logger.info("Checking for Failed streams. Attempt number: "+ i);
                    listResponse = adminClient.listStreams(listRequestFailed);
                    //if stream is in array break loop return true
                    listResponse.getItems().forEach(value -> {
                        if (value.getName().equals(StreamName)) {
                            ready.set(false);
                            logger.info("Your stream"+value.getName()+" is Failed with id:" + value.getId());
                            throw new IllegalArgumentException("Stream Creation Failed see cloud console for details.");
                        }
                    });
                }

            Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);

        } while (ready.get());

        return true;

    }

    //Core Code to create a stream
    private static Stream createStream(
            StreamAdminClient adminClient,
            String compartmentId,
            String streamName,
            int partitions) {
        logger.info(String.format("Creating stream %s with %s partitions", streamName, partitions));

        CreateStreamDetails streamDetails =
                CreateStreamDetails.builder()
                        .compartmentId(compartmentId)
                        .name(streamName)
                        .partitions(partitions)
                        .build();
        CreateStreamRequest createStreamRequest = CreateStreamRequest.builder().createStreamDetails(streamDetails).build();
        CreateStreamResponse createResponse = adminClient.createStream(createStreamRequest);
        return createResponse.getStream();
    }

    private static void publishExampleMessages(StreamClient streamClient, String streamId) {
        // build up a putRequest and publish some messages to the stream
        List<PutMessagesDetailsEntry> messages = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            messages.add(
                    PutMessagesDetailsEntry.builder()
                            .key(String.format("messageKey-%s", i).getBytes(UTF_8))
                            .value(String.format("messageValue-%s", i).getBytes(UTF_8))
                            .build());
        }

        System.out.println(
                String.format("Publishing %s messages to stream %s.", messages.size(), streamId));
        PutMessagesDetails messagesDetails =
                PutMessagesDetails.builder().messages(messages).build();

        PutMessagesRequest putRequest =
                PutMessagesRequest.builder()
                        .streamId(streamId)
                        .putMessagesDetails(messagesDetails)
                        .build();

        PutMessagesResponse putResponse = streamClient.putMessages(putRequest);

        // the putResponse can contain some useful metadata for handling failures
        for (PutMessagesResultEntry entry : putResponse.getPutMessagesResult().getEntries()) {
            if (StringUtils.isNotBlank(entry.getError())) {
                System.out.println(
                        String.format("Error(%s): %s", entry.getError(), entry.getErrorMessage()));
            } else {
                System.out.println(
                        String.format(
                                "Published message to partition %s, offset %s.",
                                entry.getPartition(),
                                entry.getOffset()));
            }
        }
    }

    private static void publishMessage(StreamClient streamClient, String streamId, String key, String value) {
        // build up a putRequest and publish some messages to the stream
        List<PutMessagesDetailsEntry> messages = new ArrayList<>();

        messages.add(PutMessagesDetailsEntry.builder()
                            .key(String.format(key).getBytes(UTF_8))
                            .value(String.format(value).getBytes(UTF_8))
                            .build());

        logger.info(String.format("Publishing %s messages to stream %s.", messages.size(), streamId));
        PutMessagesDetails messagesDetails = PutMessagesDetails.builder().messages(messages).build();

        PutMessagesRequest putRequest =
                PutMessagesRequest.builder()
                        .streamId(streamId)
                        .putMessagesDetails(messagesDetails)
                        .build();

        PutMessagesResponse putResponse = streamClient.putMessages(putRequest);

        // the putResponse can contain some useful metadata for handling failures
        for (PutMessagesResultEntry entry : putResponse.getPutMessagesResult().getEntries()) {
            if (StringUtils.isNotBlank(entry.getError())) {
                logger.info(String.format("Error(%s): %s", entry.getError(), entry.getErrorMessage()));
            } else {
                System.out.println(
                        String.format(
                                "Published message to partition %s, offset %s.",
                                entry.getPartition(),
                                entry.getOffset()));
            }//else
        }//for
    }




}

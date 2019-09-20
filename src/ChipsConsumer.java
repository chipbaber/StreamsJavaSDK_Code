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

/*Class provides an example consumer API for Streams. */

public class ChipsConsumer {

    // Set the logger to the class.
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getSimpleName());

    public static void main(String[] args) throws Exception {
    logger.info("Starting to Consume  a Stream.");
        final String configurationFilePath = ".oci/config";
        final String profile = "DEFAULT";

        //Testing Config file output
        ConfigFileReader.ConfigFile config = ConfigFileReader.parse(configurationFilePath, profile);

        //Create Connection
        final AuthenticationDetailsProvider provider = new ConfigFileAuthenticationDetailsProvider(configurationFilePath, profile);
        // Create an admin-client
        final StreamAdminClient adminClient = new StreamAdminClient(provider);
        //Set variables
        final String compartmentId = config.get("compartment-id");
        final String StreamName = "chips_stream";
        final int partitions = 1;


        try {
            StreamClient streamClient = new StreamClient(provider);
            Stream stream = getStream(adminClient,compartmentId, StreamName);
            streamClient.setEndpoint(stream.getMessagesEndpoint());
            String partitionCursor = getCursorByPartition(streamClient, stream.getId(), "0");
            simpleMessageLoop(streamClient, stream.getId(), partitionCursor);

        }
        catch (Exception e) {
            logger.error(e.toString());
        }

    }

    private static String getCursorByPartition(
            StreamClient streamClient, String streamId, String partition) {
        logger.info(String.format("Monitor partition with cursor partition number %s.", partition));

        CreateCursorDetails cursorDetails =  CreateCursorDetails.builder().partition(partition).type(Type.TrimHorizon).build();

        CreateCursorRequest createCursorRequest =
                CreateCursorRequest.builder()
                        .streamId(streamId)
                        .createCursorDetails(cursorDetails)
                        .build();

        CreateCursorResponse cursorResponse = streamClient.createCursor(createCursorRequest);
        return cursorResponse.getCursor().getValue();
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

    private static String getCursorByGroup(
            StreamClient streamClient, String streamId, String groupName, String instanceName) {
        logger.info(String.format("Creating a cursor for group %s, instance %s.", groupName, instanceName));

        CreateGroupCursorDetails cursorDetails =
                CreateGroupCursorDetails.builder()
                        .groupName(groupName)
                        .instanceName(instanceName)
                        .type(CreateGroupCursorDetails.Type.TrimHorizon)
                        .commitOnGet(true)
                        .build();

        CreateGroupCursorRequest createCursorRequest =
                CreateGroupCursorRequest.builder()
                        .streamId(streamId)
                        .createGroupCursorDetails(cursorDetails)
                        .build();

        CreateGroupCursorResponse groupCursorResponse =
                streamClient.createGroupCursor(createCursorRequest);
        return groupCursorResponse.getCursor().getValue();
    }

    private static void simpleMessageLoop(StreamClient streamClient, String streamId, String initialCursor) {
        String cursor = initialCursor;
        for (int i = 0; i < 100; i++) {

            GetMessagesRequest getRequest =
                    GetMessagesRequest.builder()
                            .streamId(streamId)
                            .cursor(cursor)
                            .limit(10)
                            .build();

            GetMessagesResponse getResponse = streamClient.getMessages(getRequest);

            // process the messages
            logger.info(String.format("Read %s messages.", getResponse.getItems().size()));
            for (Message message : getResponse.getItems()) {
                System.out.println(
                        String.format(
                                "%s: %s",
                                new String(message.getKey(), UTF_8),
                                new String(message.getValue(), UTF_8)));
            }

            // getMessages is a throttled method; clients should retrieve sufficiently large message
            // batches, as to avoid too many http requests.
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

            // use the next-cursor for iteration
            cursor = getResponse.getOpcNextCursor();
        }
    }
}

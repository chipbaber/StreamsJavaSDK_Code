import com.google.common.util.concurrent.Uninterruptibles;
import com.oracle.bmc.ConfigFileReader;
import com.oracle.bmc.auth.AuthenticationDetailsProvider;
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider;
import com.oracle.bmc.streaming.StreamClient;
import com.oracle.bmc.streaming.model.PutMessagesDetails;
import com.oracle.bmc.streaming.model.PutMessagesDetailsEntry;
import com.oracle.bmc.streaming.model.PutMessagesResultEntry;
import com.oracle.bmc.streaming.requests.PutMessagesRequest;
import com.oracle.bmc.streaming.responses.PutMessagesResponse;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import static java.nio.charset.StandardCharsets.UTF_8;


/*
* This Class will pull the hosts CPU usage's and stream at a 1 second delay to the server. This is a sample program to show the streams API's in action.
* */
public class getCPU {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getSimpleName());
    private JsonArrayBuilder osStats = Json.createArrayBuilder();
    private String machineName ="";
    private JsonObject jsonParams;
    private LocalDateTime localDateTime ;
    //Set OCI Variables to connect
    final String configurationFilePath = ".oci/config";
    final String profile = "DEFAULT";

    /*
    Method getCPU initialize the program. It will capture the machine name for later use. Then set the core connection details to send messages to Oracle Streams.
     */
    public getCPU() {
        try {
            machineName = InetAddress.getLocalHost().getHostName();
            logger.info("Machine Name Collected: "+machineName);
        }
        catch (Exception e) {
            logger.error("Error in getCPU instantiation: "+ e.toString());
        }
    }

    private void startStream(int sendCount) {
        try {
            //set Config & variables for method
            ConfigFileReader.ConfigFile config = ConfigFileReader.parse(configurationFilePath, profile);
            PutMessagesRequest putRequest;
            PutMessagesResponse putResponse;
            PutMessagesDetails messagesDetails;
            List<PutMessagesDetailsEntry> messages = new ArrayList<>();
            int i=0;
            //Create Connection
            final AuthenticationDetailsProvider provider = new ConfigFileAuthenticationDetailsProvider(configurationFilePath, profile);
            // Create a stream client using the provided message endpoint.
            StreamClient streamClient = new StreamClient(provider);
            streamClient.setEndpoint(config.get("streamEndpoint"));

            while (i<sendCount) {

                    //Build Message json details
                    getOsValues();
                    buildElement();

                    //build Message
                    messages.add(PutMessagesDetailsEntry.builder()
                            .key(machineName.getBytes(UTF_8))
                            .value(jsonParams.toString().getBytes(UTF_8))
                            .build());


                messagesDetails = PutMessagesDetails.builder().messages(messages).build();

                putRequest = PutMessagesRequest.builder()
                                .streamId(config.get("streamId"))
                                .putMessagesDetails(messagesDetails)
                                .build();


                //Send Message
                putResponse = streamClient.putMessages(putRequest);
                checkResponse(putResponse);
                Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
                i++;
                messages.clear();
            }//while


        }
        catch (Exception e) {
            logger.error("Error in startStream instantiation: "+ e.toString());
        }

    }

    /*
    * Method CheckResponse evaluates the result of a message send to Oracle Streams. If in error it outputs error on producer side.
    * If successful provides the offset and partition.
    * */

    private void checkResponse(PutMessagesResponse putResponse) {
        //Check the response for errors.
        for (PutMessagesResultEntry entry : putResponse.getPutMessagesResult().getEntries()) {
            if (StringUtils.isNotBlank(entry.getError())) {
                logger.error(String.format("Error(%s): %s", entry.getError(), entry.getErrorMessage()));
            } else {
                logger.info(
                        String.format(
                                "Published message to partition %s, offset %s.",
                                entry.getPartition(),
                                entry.getOffset()));
            }//else
        }//for
    }
/*
Method gets core stats from the operating system running the java code. Adds elements to jsonArray.
 */
    private void getOsValues(){
        OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
        for (Method method : operatingSystemMXBean.getClass().getDeclaredMethods()) {
            method.setAccessible(true);
            if (method.getName().startsWith("get") && Modifier.isPublic(method.getModifiers())) {
                Object value;
                try {
                    value = method.invoke(operatingSystemMXBean);
                } catch (Exception e) {
                    value = e;
                } // try
               // System.out.println(method.getName() + " = " + value);
                //convert to double for json
                if (method.getName().equals("getSystemCpuLoad") || method.getName().equals("getProcessCpuLoad")) {
                    double x= Double.parseDouble(value.toString());
                    addElementToArray(method.getName(), x);
                }
                else {
                    addElementToArray(method.getName(), Long.parseLong(value.toString()));
                }
            } // if
        } // for
    }

    //Adds element to JSON Array.
    public void addElementToArray(String key, String value) {
        osStats.add(Json.createObjectBuilder()
                .add(key, value));
    }

    //overwrite for Long
    public void addElementToArray(String key, Long value) {
        osStats.add(Json.createObjectBuilder()
                .add(key, value));
    }

    //overwrite for double
    public void addElementToArray(String key, double value) {
        osStats.add(Json.createObjectBuilder()
                .add(key, value));
    }

    public void buildElement() {
        localDateTime =LocalDateTime.now();
        jsonParams = Json.createObjectBuilder()
                .add("machineName",machineName)
                .add("messageTime",localDateTime.toString())
                .add("machineData", osStats.build())
                .build();
    }

    private void printJSON () {
        System.out.println(jsonParams.toString());
    }

    public void buildJSON() {
       /* JsonObject json = Json.createObjectBuilder()
                .add("name", "Falco")
                .add("age", BigDecimal.valueOf(3))
                .add("biteable", Boolean.FALSE).build();

        */
         osStats.add(Json.createObjectBuilder()
                    .add("name", "Chip").add("name", "Chip"));



        JsonObject jsonParams = Json.createObjectBuilder()
                .add("machine", "test")
                .add("test", osStats.build())
                .build();

        String result = jsonParams.toString();

        System.out.println(result);
    }


    public static void main(String[] args) throws Exception {
        getCPU a = new getCPU();
         a.startStream(60);
        /*a.getOsValues();
        a.buildElement();
        a.printJSON();*/



    }


}

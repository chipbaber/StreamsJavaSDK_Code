##Intro
This code base is an exploration in the Streams SDK in Java. The goal was to test API's and create a simple example for Java Developers. 

##Producer Example
Inside getCPU.java the code captures machine information about the host enviroments runtime statistics using OperatingSystemMXBean. 
The information is then packaged into json through the javax.json libraries. Once packaged the code base becomes a producer and sends the data to the cloud. 
The key will be the machine name, the value the json payload. Here is a short example video. 

##Getting Started: Steps to Leverage OCI Java SDK
- Download Full Java SDK, https://github.com/oracle/oci-java-sdk/releases. Extract to your local machine.
    
- Open Intelj and create a new java project. Set the classpath to include the /lib  and /addons directories.

- Download sl4j-simple-1.7.28.jar and add to classpath. 

- Download javax.json-api-1.1.jar, javax.json-api-1.1.jar and add to classpath. 

- Create Pem Keys
openssl genrsa -out ~/.oci/oci_api_key.pem 2048
openssl rsa -pubout -in ~/.oci/oci_api_key.pem -out ~/.oci/oci_api_key_public.pem -passin stdin

- Create Fingerprint

- Create .oci directory

- create config file and copy over details

- Download SL4J jar file and map classpath

- Set JDK Class paths
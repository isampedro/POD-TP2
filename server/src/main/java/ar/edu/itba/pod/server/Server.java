package ar.edu.itba.pod.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;

import java.io.FileNotFoundException;

public class Server {
    private final static Logger logger = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args) throws FileNotFoundException {
        logger.info("Starting HazelcastServer");
        Config config = new XmlConfigBuilder("../../../hazelcast.xml").build();
        Hazelcast.newHazelcastInstance(config);
    }
}

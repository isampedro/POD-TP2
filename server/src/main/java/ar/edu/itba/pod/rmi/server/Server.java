package ar.edu.itba.pod.rmi.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class Server {
    private final static Logger logger = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args) {

        logger.info("Airport Manager Server Starting ...");

        final Servant servant = new Servant();
        logger.info("Servant created");
        try {
            final Registry registry = LocateRegistry.getRegistry();
            final Remote adminRemote = UnicastRemoteObject.exportObject(servant,0);
            registry.rebind("Airport-Service",adminRemote);
            logger.info("Server is ready");
        }catch (RemoteException e){
            logger.error("Connection to registry failed");
            logger.error(e.getMessage());
        }

    }
}

package ar.edu.itba.pod.api.rmi.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Servant implements AirportOpsService, LaneRequesterService, FlightTracingService, QueryService {
    private final ReadWriteLock laneLock = new ReentrantReadWriteLock();

    private final Map<Integer, List<Lane>> laneMap;
    private final Map<String, List<Flight>> flightHistory;
    private final Map<String,Map<Integer,ArrayList<Notifications>>> registeredAirlines;
    private final static Logger logger = LoggerFactory.getLogger(Servant.class);


    public Servant() {
        this.laneMap = new HashMap<>();
        this.registeredAirlines = new HashMap<>();
        this.flightHistory = new HashMap<>();
        for (int i = 1; i <= Categories.maxAuthorization() ; i++) {
            laneMap.put(i, new LinkedList<>());
        }
    }

    //------------------------------------------Admin---------------------------------------//


    public void addLane( String laneName, Categories category ) throws LaneNameAlreadyExistsException {
        laneLock.writeLock().lock();
        try {
            for( Integer key : laneMap.keySet() ) {
                if (laneMap.get(key).stream().anyMatch((lane) -> lane.getName().equals(laneName))) {
                    throw new LaneNameAlreadyExistsException(laneName);
                }
            }
            laneMap.get(category.getAuthorization()).add( new Lane(laneName, category));
            logger.info("Added lane: {}",laneName);
            sortLanes();
        } finally {
            laneLock.writeLock().unlock();
        }
    }
    private void sortLanes() {
        for( Integer key : laneMap.keySet() ) {
            laneMap.get(key).sort( (lane1, lane2) -> {
                if( lane1.getCategory().equals(lane2.getCategory()) ) {
                    if( lane1.getFlightsQuantity() == lane2.getFlightsQuantity() ) {
                        return 0;
                    }
                    return lane1.getFlightsQuantity() < lane2.getFlightsQuantity() ? -1: 1;
                }
                return lane2.getCategory().isHigherOrEqual(lane1.getCategory()) ? 1:-1;
            });
        }
    }

    public boolean isOpen( String laneName ) throws LaneNotExistentException {
        laneLock.readLock().lock();
        try {
            for( Integer key : laneMap.keySet() ) {
                for( Lane lane: laneMap.get(key) ) {
                    if( lane.getName().equals(laneName) ) {
                        return lane.getState().equals(LaneState.OPEN);
                    }
                }
            }
            throw new LaneNotExistentException(laneName);
        } finally {
            laneLock.readLock().unlock();
        }
    }

    private void setLaneState( String laneName, LaneState state ) throws  SameLaneStateException, LaneNotExistentException {
        laneLock.writeLock().lock();
        try {
            for( Integer key : laneMap.keySet() ) {
                for( Lane lane: laneMap.get(key) ) {
                    if( lane.getName().equals(laneName) ) {
                        if( !lane.getState().equals(state) ) {
                            lane.setState(state);
                            return;
                        } else {
                            throw new SameLaneStateException(state.equals(LaneState.OPEN));
                        }
                    }
                }
            }

            throw new LaneNotExistentException(laneName);
        } finally {
            laneLock.writeLock().unlock();
        }
    }

    public void openLane( String laneName ) throws SameLaneStateException, LaneNotExistentException {
        for( Integer key : laneMap.keySet() ) {
            if( laneMap.get(key).stream().anyMatch( (lane) -> lane.getName().equals(laneName))) {
                setLaneState( laneName, LaneState.OPEN );
                logger.info("Opened lane {}",laneName);
                return;
            }
        }

        throw new LaneNotExistentException(laneName);
    }

    public void closeLane( String laneName ) throws SameLaneStateException, LaneNotExistentException {
        for( Integer key : laneMap.keySet() ) {
            if( laneMap.get(key).stream().anyMatch( (lane) -> lane.getName().equals(laneName))) {
                setLaneState( laneName, LaneState.CLOSED );
                logger.info("Closed lane {}",laneName);
                return;
            }
        }

        throw new LaneNotExistentException(laneName);
    }

    public List<Integer> emitDeparture() {
        Flight flight = null;
        Lane flightLane = null;

        final List<Integer> departed = new LinkedList<>();
        final Map<Lane, List<Flight>> flightsNotification = new HashMap<>();
        laneLock.writeLock().lock();
        try {
            for (Integer key : laneMap.keySet()) {
                for (Lane lane : laneMap.get(key)) {
                    if (lane.flightsAreAwaiting() && lane.getState().equals(LaneState.OPEN)) {
                        flight = lane.departFlight();
                        flightLane = lane;
                        departed.add(flight.getId());
                        logger.info("Flight {} departed in lane {}", flight.getId(), lane.getName());
                        flightHistory.putIfAbsent(lane.getName(), new ArrayList<>());
                        flightHistory.get(lane.getName()).add(flight);
                        flightsNotification.put(lane, lane.getFlightsList());
                    }
                }
            }
            for (Integer key : laneMap.keySet()) {
                for (Lane lane : laneMap.get(key)) {
                    lane.getFlightsList().forEach(Flight::increaseTakeOffsOrdersQuantity);
                }
            }
        } finally {
            laneLock.writeLock().unlock();
        }

        if (flight != null && flightLane != null) {
            notifyAirlines(flight, Events.DEPARTURE, flightLane);
        }

        flightsNotification.keySet().forEach(key -> flightsNotification.get(key).forEach(f -> notifyAirlines(f, Events.ADVANCE, key)));

        return departed;
    }

    public Map<Boolean, List<Integer>> emitReorder() {
        Queue<Flight> flights = new LinkedList<>();
        Map<Boolean, List<Integer>> reorderedFlights = new HashMap<>();
        reorderedFlights.put(true, new ArrayList<>());
        reorderedFlights.put(false, new ArrayList<>());

        laneLock.writeLock().lock();
        try {
            while( !emptyAirport() ) {
                for( Integer key : laneMap.keySet() ) {
                    for (Lane lane : laneMap.get(key)) {
                        if (lane.flightsAreAwaiting()) {
                            flights.offer(lane.departFlight());
                        }
                    }
                }
            }
            while (!flights.isEmpty()){
                Flight flight = flights.poll();
                try {
                    addFlightToLane(flight.getId(),flight.getDestinyAirport(),flight.getAirline(),flight.getCategory());
                    reorderedFlights.get(true).add(flight.getId());
                } catch (NoAvailableLaneException e){
                    reorderedFlights.get(false).add(flight.getId());
                }
            }
        } finally {
            laneLock.writeLock().unlock();
        }
        return reorderedFlights;
    }

    private boolean emptyAirport() {
        for( Integer key : laneMap.keySet() ) {
            for (Lane lane : laneMap.get(key)) {
                if (lane.flightsAreAwaiting()) {
                    return false;
                }
            }
        }
        return true;
    }

    //------------------------------------------Lane Requester---------------------------------------//
    @Override
    public void addFlightToLane(int flightId, String destinyAirport, String airline, Categories minimumCategory) throws NoAvailableLaneException {
        Flight flight = new Flight(flightId, minimumCategory, airline, destinyAirport);
        Lane minLane = null;
        Integer minimumAuth = minimumCategory.getAuthorization();

        laneLock.writeLock().lock();
        try {
            for (int i = minimumAuth; i <= Categories.maxAuthorization(); i++) {
                for (Lane lane : laneMap.get(i)) {
                    if(lane.getCategory().isHigherOrEqual(flight.getCategory()) &&
                            lane.getState().equals(LaneState.OPEN)){
                        if(minLane == null)
                            minLane = lane;
                        else if(lane.getFlightsQuantity() < minLane.getFlightsQuantity())
                            minLane = lane;
                    }
                }
            }
            if (minLane == null)
                throw new NoAvailableLaneException(flightId);
            else {
                minLane.addNewFlight(flight);
                logger.info("Flight {} added to lane {}",flight.getId(),minLane.getName());
                sortLanes();
            }
        } finally {
            laneLock.writeLock().unlock();
        }
        notifyAirlines(flight,Events.ASSIGNED,minLane);
    }

    //------------------------------------------Flight Tracer---------------------------------------//

    @Override
    public synchronized void registerAirline(String airline, int flightId, Notifications handler){
        registeredAirlines.putIfAbsent(airline,new HashMap<>());
        registeredAirlines.get(airline).putIfAbsent(flightId,new ArrayList<>());
        registeredAirlines.get(airline).get(flightId).add(handler);

        logger.info("Added airline: {}",airline);
        logger.info(registeredAirlines.keySet().toString());

    }
AirportExceptions
    private void notifyAirlines(Flight flight, Events event, Lane lane) {
        if(!registeredAirlines.containsKey(flight.getAirline()))
            return;
        registeredAirlines.getOrDefault(flight.getAirline(), new HashMap<>()).getOrDefault(flight.getId(), new ArrayList<>()).forEach(handler->{
           try {
               if (event.equals(Events.DEPARTURE))
                   handler.notifyEvent(event, flight.getDestinyAirport(), lane.getName(), 0);
               else {
                   handler.notifyEvent(event, flight.getDestinyAirport(), lane.getName(), lane.getFlightsAhead(flight));
               }
           }catch (RemoteException e){
               e.printStackTrace();
           }
        });
    }

    //------------------------------------------Query---------------------------------------//

    @Override
    public List<String> getTakeoffsForAirport() throws RemoteException {
        final List<String> flights = new LinkedList<>();
        laneLock.readLock().lock();
        try {
            this.flightHistory
                    .forEach((key, value) -> this.flightHistory.get(key)
                    .forEach(flight -> flights.add( flight.getTakeOffsOrdersQuantity() + ";" + key + ";" + flight.getId() + ";"
                            + flight.getDestinyAirport() + ";" + flight.getAirline())));
        }
        finally {
            laneLock.readLock().unlock();
        }
        logger.info(flights.toString());
        return flights;
    }

    @Override
    public List<String> getTakeoffsForAirline(String airline) throws RemoteException {
        final List<String> flights = new LinkedList<>();
        laneLock.readLock().lock();
        try {
            this.flightHistory
                    .forEach((key, value) -> this.flightHistory.get(key)
                            .stream()
                            .filter(flight -> flight.getAirline().equals(airline))
                            .forEach(flight -> flights.add(flight.getTakeOffsOrdersQuantity() + ";" + key + ";" + flight.getId() + ";"
                                    + flight.getDestinyAirport() + ";" + flight.getAirline())));

        }
        finally {
            laneLock.readLock().unlock();
        }

        logger.info(flights.toString());
        return flights;
    }

    @Override
    public List<String> getTakeoffsForLane(String laneName) throws RemoteException {
        final List<String> flightsTakeOff = new LinkedList<>();
        laneLock.readLock().lock();
        try {
            Optional.ofNullable(this.flightHistory.get(laneName))
                    .ifPresent(flights -> flights
                            .forEach(flight -> flightsTakeOff.add(flight.getTakeOffsOrdersQuantity() + ";" + laneName + ";" + flight.getId() + ";"
                                    + flight.getDestinyAirport() + ";" + flight.getAirline())));
        } finally {
            laneLock.readLock().unlock();
        }
        logger.info(flightsTakeOff.toString());
        return flightsTakeOff;
    }


    //------------------------------------------Testing---------------------------------------//

    public void printAirports() {
        System.out.println("Airport lanes: ");
        laneLock.readLock().lock();
        try {
            laneMap.values().forEach( lane -> lane.forEach(System.out::println));
        } finally {
            laneLock.readLock().unlock();
        }
    }

    public int getLanesQuantity() {
        int size = 0;
        laneLock.readLock().lock();
        try {
            for (Integer key : laneMap.keySet()) {
                size += laneMap.get(key).size();
            }
        } finally {
            laneLock.readLock().unlock();
        }
        return size;
    }
}

package flights.topology;

import flights.serde.Serde;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.checkerframework.checker.units.qual.A;
import radar.AirportKpi;
import radar.AirportUpdateEvent;
import radar.Flight;
import radar.FlightUpdateEvent;

public class TopologyBuilder implements Serde {

    // Stateless & stateful transformation

    private Properties config;

    public TopologyBuilder(Properties properties) {
        this.config = properties;
    }

    private static final Logger logger = LogManager.getLogger(TopologyBuilder.class);

    public Topology build() {
        StreamsBuilder builder = new StreamsBuilder();
        String schemaRegistry = config.getProperty("kafka.schema.registry.url");

        // Exercise 1 - data stream for flights & FlightUpdateEvents
        KStream<String, FlightUpdateEvent> flightInputStream = builder.stream(
                config.getProperty("kafka.topic.flight.update.events"),
                Consumed.with(Serde.stringSerde, Serde.specificSerde(FlightUpdateEvent.class, schemaRegistry)));

        readFlightStream(flightInputStream);

        // Exercise 2
        GlobalKTable<String, AirportUpdateEvent> airportTable = builder.globalTable(
                config.getProperty("kafka.topic.airport.update.events"),
                Consumed.with(Serde.stringSerde, Serde.specificSerde(AirportUpdateEvent.class, schemaRegistry)));

        //readAirportTable(flightInputStream, airportInputStream, airportTable);

        return builder.build();
    }

    private void readFlightStream(KStream<String, FlightUpdateEvent> flightStream) {
        System.out.println("[mrmi]: stream:" + flightStream.peek((key, value) -> System.out.println("[mrmi]: keval: " + key + value)));
        String schemaRegistry = config.getProperty("kafka.schema.registry.url");
        flightStream
                // Filter by non canceled flights
                .filter((key, value) -> !value.getStatus().toString().equals("CANCELED"))
                // Map them to Flight objects
                .map(
                        (key, value) -> {
                            System.out.println("[MRMI] ucitao 1 ");
                            return KeyValue.pair(key, getFlight(value));
                        }
                )
                // Send to stream
                .to(
                        config.getProperty("kafka.topic.radar.flights"),
                        Produced.with(Serde.stringSerde, Serde.specificSerde(Flight.class, schemaRegistry)));
        flightStream.peek((key, val) -> System.out.println("[mrmi]: key " + key + " val : " + val));
    }

    private Flight getFlight(FlightUpdateEvent val) {
        Flight flight = new Flight();
        // "Vienna/Austria(VIE)->Belgrade/Serbia(BEG)"
        String[] destination = val.getDestination().toString().split("->");
        flight.setId(val.getId());
        flight.setTo(destination[1]);
        flight.setFrom(destination[0]);
        flight.setArrivalTimestamp(val.getSTA());
        flight.setDepartureTimestamp(val.getSTD());
        flight.setStatus(val.getStatus());

        // ISO 8601 datetime format
        SimpleDateFormat isoSdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");

        flight.setArrivalTime(isoSdf.format(new Date(val.getSTA())));
        flight.setDepartureTime(isoSdf.format(new Date(val.getSTD())));

        flight.setDuration(val.getSTA() - val.getSTD());

        flight.setArrivalAirportCode(destination[1].substring(destination[1].indexOf('('), destination[1].length() - 1));
        flight.setDepartureAirportCode(destination[0].substring(destination[0].indexOf('('), destination[0].length() - 1));

        return flight;
    }

    private void readDepartered(KStream<String, FlightUpdateEvent> flightStream, GlobalKTable<String, AirportUpdateEvent> airportTable) {
        /*
        () -> flightStream
                // Keep only flights that aren't canceled or late
                .filter((key, value) -> !value.getStatus().equals("CANCELED")
                        && !value.getStatus().equals("LATE"))
                // Join table & flight stream
                .join(airportTable,
                        (key, val) -> {
                            String destinationStr = val.getDestination().toString();
                            destinationStr = destinationStr.substring(destinationStr.indexOf('('), destinationStr.indexOf(')'));

                            return destinationStr;
                        },
                        (key, val) -> getAirportKpi(val))
                // Group by airports
                .groupBy((key, val) -> val.getAirport())
                // Hopping window
                .windowedBy(TimeWindows.of(Duration.ofMinutes(5)).advanceBy(Duration.ofMinutes(1)))
                .aggregate(() -> {
                    System.out.println("Dosta od nas");
                            return null;
                        },
                        (key, value, aggregate) -> aggregate)
                .toStream()
                .to("radar.flights");
        // Isto odraditi za otkazane i poslati na radar.airports.kpi
        */
    }

    private AirportKpi getAirportKpi(AirportUpdateEvent flightUpdateEvent) {
        AirportKpi airportKpi = new AirportKpi();
        airportKpi.setAirport(flightUpdateEvent.getCode());
        return airportKpi;
    }
}

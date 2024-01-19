CREATE TABLE IF NOT EXISTS flight_data
(
    carrier VARCHAR(50) NOT NULL,
    "date" date,
    national_flights INTEGER,
    international_flights INTEGER,
    avg_ticket_price_dollars REAL,
    avg_flight_time_hours INTEGER,
    avg_distance_traveled_km REAL,
    total_distance_traveled_km REAL,
    cancelled_flights INTEGER,
    delayed_flights INTEGER,
    avg_delay_hours REAL
);
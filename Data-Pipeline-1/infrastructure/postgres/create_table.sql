CREATE TABLE IF NOT EXISTS flight_data
(
    carrier VARCHAR(50) NOT NULL,
    for_date date,
    international_flights INTEGER,
    national_flights INTEGER,
    avg_flight_time_min INTEGER,
    avg_ticket_price_dollars REAL,
    total_distance_km REAL,
    cancelled_flights INTEGER,
    delayed_flights INTEGER,
    avg_delay_wait_time_min REAL
);
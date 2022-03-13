package be.pxl.paj.flights;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Allows clients to query and update the database in order to log in, search
 * for flights, reserve seats, show reservations, and cancel reservations.
 */
public class FlightsDB {

	/**
	 * Maximum number of reservations to allow on one flight.
	 */
	private static int MAX_FLIGHT_BOOKINGS = 3;

	/**
	 * Holds the connection to the database.
	 */
	private Connection conn;

	private PreparedStatement queryLoginCustomer;
	private PreparedStatement queryFlights;
	private PreparedStatement queryFlightsWithHops;
	private PreparedStatement insertReservation;
	private PreparedStatement countUserReservationsOnDay;
	private PreparedStatement countReservationsForFlight;
	private PreparedStatement queryFlightsForUser;
	private PreparedStatement deleteReservation;


	/**
	 * Opens a connection to the database using the given settings.
	 */
	public void open(Properties settings) throws Exception {
		// Make sure the JDBC driver is loaded.
		// Open a connection to our database.
		conn = DriverManager.getConnection(
				settings.getProperty("flightservice.url"),
				settings.getProperty("flightservice.username"),
				settings.getProperty("flightservice.password"));
	}

	/**
	 * Closes the connection to the database.
	 */
	public void close() throws SQLException {
		conn.close();
		conn = null;
	}

	/**
	 * Performs additional preparation after the connection is opened.
	 */
	public void init() throws SQLException {
		// TODO: create prepared statements here
		queryLoginCustomer = conn.prepareStatement(
				"SELECT uid, name, handle FROM CUSTOMER WHERE handle=? AND password=?"
		);
		queryFlights = conn.prepareStatement(
				"SELECT fid, name, flight_num, origin_city, dest_city, actual_time " +
						"FROM FLIGHTS F1, CARRIERS " +
						"WHERE carrier_id = cid AND actual_time IS NOT NULL AND " +
						"    year = ? AND month_id = ? AND day_of_month = ? AND " +
						"    origin_city = ? AND dest_city = ? " +
						"ORDER BY actual_time ASC LIMIT 99"
		);
		queryFlightsWithHops = conn.prepareStatement(
				"SELECT F1.fid as fid1, C1.name as name1, " +
						"    F1.flight_num as flight_num1, F1.origin_city as origin_city1, " +
						"    F1.dest_city as dest_city1, F1.actual_time as actual_time1, " +
						"    F2.fid as fid2, C2.name as name2, " +
						"    F2.flight_num as flight_num2, F2.origin_city as origin_city2, " +
						"    F2.dest_city as dest_city2, F2.actual_time as actual_time2\n" +
						"FROM FLIGHTS F1, FLIGHTS F2, CARRIERS C1, CARRIERS C2\n" +
						"WHERE F1.carrier_id = C1.cid AND F1.actual_time IS NOT NULL AND " +
						"    F2.carrier_id = C2.cid AND F2.actual_time IS NOT NULL AND " +
						"    F1.year = ? AND F1.month_id = ? AND F1.day_of_month = ? AND " +
						"    F2.year = ? AND F2.month_id = ? AND F2.day_of_month = ? AND " +
						"    F1.origin_city = ? AND F2.dest_city = ? AND" +
						"    F1.dest_city = F2.origin_city\n" +
						"ORDER BY F1.actual_time + F2.actual_time ASC LIMIT 99"
		);
		insertReservation = conn.prepareStatement(
				"INSERT INTO RESERVATION (uid, fid) VALUES (?, ?)"
		);

		countUserReservationsOnDay = conn.prepareStatement("""
						SELECT f.fid num
						FROM RESERVATION r
							JOIN FLIGHTS f ON f.fid=r.fid
						WHERE r.uid=? AND f.day_of_month=?
						""");
		countReservationsForFlight = conn.prepareStatement("""
						SELECT fid, COUNT(fid) num
						FROM RESERVATION r
						WHERE r.fid = ?
						GROUP BY fid
						HAVING COUNT(fid) >= ?
						""");
		countReservationsForFlight.setInt(2, MAX_FLIGHT_BOOKINGS);

		queryFlightsForUser = conn.prepareStatement("""
						SELECT f.fid, f.YEAR, f.month_id, f.day_of_month, c.name, f.flight_num, f.origin_city, f.dest_city, f.actual_time
						FROM RESERVATION r
							JOIN FLIGHTS f ON f.fid=r.fid
							JOIN CARRIERS c ON f.carrier_id=c.cid
						WHERE r.uid=?
						""");

		deleteReservation = conn.prepareStatement("""
						DELETE FROM RESERVATION
						WHERE uid = ? AND fid = ?
						""");
	}

	/**
	 * Tries to log in as the given user.
	 *
	 * @return The authenticated user or null if login failed.
	 */
	public User logIn(String handle, String password) throws SQLException {
		User user = null;

		queryLoginCustomer.setString(1, handle);
		queryLoginCustomer.setString(2, password);

		ResultSet result = queryLoginCustomer.executeQuery();
		if (result.next()) {
			user = new User(result.getInt("uid"), result.getString("handle"), result.getString("name"));
		}
		result.close();

		return user;
	}

	/**
	 * Returns the list of all flights between the given cities on the given day.
	 */
	public List<Flight[]> getFlights(LocalDate date, String originCity, String destCity) throws SQLException {

		List<Flight[]> results = new ArrayList<>();

		queryFlights.setInt(1, date.getYear());
		queryFlights.setInt(2, date.getMonthValue());
		queryFlights.setInt(3, date.getDayOfMonth());
		queryFlights.setString(4, originCity);
		queryFlights.setString(5, destCity);

		ResultSet directResults = queryFlights.executeQuery();
		while (directResults.next()) {
			results.add(new Flight[] {
					new Flight(directResults.getInt("fid"), date,
							directResults.getString("name"),
							directResults.getString("flight_num"),
							directResults.getString("origin_city"),
							directResults.getString("dest_city"),
							(int) directResults.getFloat("actual_time"))
			});
		}
		directResults.close();

		queryFlightsWithHops.setInt(1, date.getYear());
		queryFlightsWithHops.setInt(2, date.getMonthValue());
		queryFlightsWithHops.setInt(3, date.getDayOfMonth());
		queryFlightsWithHops.setInt(4, date.getYear());
		queryFlightsWithHops.setInt(5, date.getMonthValue());
		queryFlightsWithHops.setInt(6, date.getDayOfMonth());
		queryFlightsWithHops.setString(7, originCity);
		queryFlightsWithHops.setString(8, destCity);

//		ResultSet twoHopResults = queryFlightsWithHops.executeQuery();
//
//		while (twoHopResults.next()) {
//			results.add(new Flight[] {
//					new Flight(twoHopResults.getInt("fid1"), date,
//							twoHopResults.getString("name1"),
//							twoHopResults.getString("flight_num1"),
//							twoHopResults.getString("origin_city1"),
//							twoHopResults.getString("dest_city1"),
//							(int) twoHopResults.getFloat("actual_time1")),
//					new Flight(twoHopResults.getInt("fid2"), date,
//							twoHopResults.getString("name2"),
//							twoHopResults.getString("flight_num2"),
//							twoHopResults.getString("origin_city2"),
//							twoHopResults.getString("dest_city2"),
//							(int) twoHopResults.getFloat("actual_time2"))
//			});
//		}
//		twoHopResults.close();

		return results;
	}

	/**
	 * Returns the list of all flights reserved by the given user.
	 */
	public List<Flight> getReservations(User user) throws SQLException {

		List<Flight> results = new ArrayList<>();

		queryFlightsForUser.setInt(1, user.getId());

		ResultSet rawResults = queryFlightsForUser.executeQuery();
		while (rawResults.next()) {
			LocalDate date = LocalDate.of(
					rawResults.getInt("YEAR"),
					rawResults.getInt("month_id"),
					rawResults.getInt("day_of_month")
					);

			results.add(
					new Flight(
							rawResults.getInt("fid"),
							date,
							rawResults.getString("name"),
							rawResults.getString("origin_city"),
							rawResults.getString("flight_num"),
							rawResults.getString("dest_city"),
							(int) rawResults.getFloat("actual_time")
					)
			);
		}

		rawResults.close();

		return results;
	}

	/**
	 * Indicates that a reservation was added successfully.
	 */
	public static final int RESERVATION_ADDED = 1;

	/**
	 * Indicates the reservation could not be made because the flight is full
	 * (i.e., 3 users have already booked).
	 */
	public static final int RESERVATION_FLIGHT_FULL = 2;

	/**
	 * Indicates the reservation could not be made because the user already has a
	 * reservation on that day.
	 */
	public static final int RESERVATION_DAY_FULL = 3;

	/**
	 * Attempts to add a reservation for the given user on the given flights, all
	 * occurring on the given day.
	 *
	 * @return One of the {@code RESERVATION_*} codes above.
	 */
	public int addReservations(User user, LocalDate date, List<Flight> flights)
			throws SQLException {

		conn.setAutoCommit(false);

		// Check for free places
		for (var flight :
				flights) {
			countReservationsForFlight.setInt(1, flight.getId());
			try (ResultSet results = countReservationsForFlight.executeQuery()) {
				if (results.next()) {
					return RESERVATION_FLIGHT_FULL;
				}
			}
		}



		// Check if user has already reservations

		countUserReservationsOnDay.setInt(1, user.getId());
		countUserReservationsOnDay.setInt(2, flights.get(0).getDate().getDayOfMonth());

		try (ResultSet results = countUserReservationsOnDay.executeQuery()) {
			if (results.next()) {
					return RESERVATION_DAY_FULL;
			}
		}

		for (var flight :
				flights) {
			insertReservation.setInt(1, user.getId());
			insertReservation.setInt(2, flight.getId());
			insertReservation.executeUpdate();
		}

		conn.commit();
		conn.setAutoCommit(true);

		return RESERVATION_ADDED;
	}

	/**
	 * Cancels all reservations for the given user on the given flights.
	 */
	public void removeReservations(User user, List<Flight> flights)
			throws SQLException {

		conn.setAutoCommit(false);
		deleteReservation.setInt(1, user.getId());

		for (var flight :
				flights) {
			deleteReservation.setInt(2, flight.getId());
			deleteReservation.execute();
		}

		conn.commit();
		conn.setAutoCommit(true);
	}
}

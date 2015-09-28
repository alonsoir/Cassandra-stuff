import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class MyCassandraConector {

	private static String serverIP = "127.0.0.1";
	private static String system_keyspace = "system";

	private static String my_first_keyspace = "myfirstcassandradb";

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		System.out.println("Starting...");

		Cluster cluster = Cluster.builder().addContactPoints(serverIP).build();

		Session session = cluster.connect(system_keyspace);

		String cqlStatement = "SELECT * FROM local";
		for (Row row : session.execute(cqlStatement)) {
			System.out.println("row: " + row.toString());
		}

		session.close();

		session = cluster.connect(my_first_keyspace);

		System.out.println("Connected to " + my_first_keyspace);

		cqlStatement = "DROP KEYSPACE myfirstcassandradb";

		session.execute(cqlStatement);
		System.out.println("keyspace myfirstcassandradb deleted...");

		// Creating a Keyspace, similar to a SQL schema
		cqlStatement = "CREATE KEYSPACE myfirstcassandradb WITH "
				+ "replication = {'class':'SimpleStrategy','replication_factor':1}";

		session.execute(cqlStatement);

		System.out.println("keyspace myfirstcassandradb created...");

		cqlStatement ="USE myfirstcassandradb";
		
		session.execute(cqlStatement);
		
		System.out.println("changed to keyspace myfirstcassandradb...");
		
		cqlStatement = "CREATE TABLE users ("
				+ " user_name varchar PRIMARY KEY," + " password varchar "
				+ ");";

		session.execute(cqlStatement);

		System.out.println("table users created...");
		
		// for all three it works the same way (as a note the 'system' keyspace
		// cant
		// be modified by users so below im using a keyspace name
		// 'exampkeyspace' and
		// a table (or columnfamily) called users
		String cqlStatementC = "INSERT INTO myfirstcassandradb.users (user_name, password) "
				+ "VALUES ('Serenity', 'fa3dfQefx')";

		session.execute(cqlStatementC); // interchangeable, put any of the
		// statements u wish.
		System.out.println("Inserted row...");

		//update users set age=37 where lastname=’Isidoro’;
		
		String cqlStatementU = "UPDATE myfirstcassandradb.users SET password = 'zzaEcvAf32hla' WHERE user_name = 'Serenity';";

		session.execute(cqlStatementU);

		System.out.println("row updated...");

		String cqlStatementD = "DELETE FROM myfirstcassandradb.users WHERE user_name = 'Serenity';";

		session.execute(cqlStatementD);

		System.out.println("row deleted...");

		session.closeAsync();

		System.out.println("Done!");
	}

}

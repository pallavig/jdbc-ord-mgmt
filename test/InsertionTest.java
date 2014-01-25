import org.junit.*;
import sun.tools.jar.resources.jar;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static junit.framework.Assert.assertEquals;

public class InsertionTest {
    static Connection connection;
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        String url = "jdbc:mariadb://localhost:3306/test";
        connection = DriverManager.getConnection(url,"pallavi","password");
        Statement statement = connection.createStatement();

    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        connection.close();
    }

    @Before
    public void setUp() throws Exception {
        Statement statement = connection.createStatement();
        String createCustomerQuery = "create table jdbc_customer ( cust_id int primary key," +
                "cust_name varchar(20)," +
                "adds1 varchar(20)," +
                "adds2 varchar(20)," +
                "state varchar(20)," +
                "city varchar(20));";

        boolean result = statement.execute(createCustomerQuery);
    }
    @After
    public void tearDown() throws Exception {
        Statement statement = connection.createStatement();
        String createCustomerQuery = "drop table jdbc_customer;";
        boolean result = statement.execute(createCustomerQuery);
    }
    @Test
    public void testInsertingRecordInCustomer() throws Exception {
        Statement statement;
        int result;
        ResultSet record;
        statement = connection.createStatement();
        result = statement.executeUpdate("insert into jdbc_customer (cust_id,cust_name,adds1,adds2,city,state) values (1,\"prateek\",\"delhi\",\"delhi\",\"delhi\",\"delhi\");");
        assertEquals(result,1);
        record =statement.executeQuery("select cust_name from jdbc_customer where cust_id = 1");
        record.next();
        assert("prateek".equals(record.getString("cust_name")));
    }

    @Test
    public void testDeletingRecordFromCustomer() throws Exception {
        Statement statement = connection.createStatement();
        int result;
        ResultSet records;
        result = statement.executeUpdate("insert into jdbc_customer (cust_id,cust_name,adds1,adds2,city,state) values (1,\"prateek\",\"delhi\",\"delhi\",\"delhi\",\"delhi\");");
        assertEquals(result,1);
        result = statement.executeUpdate("delete from jdbc_customer where cust_id=1;");
        assert(result==1);
        records = statement.executeQuery("select cust_name from jdbc_customer where cust_id = 1;");
        assertEquals(records.next(),false);
    }

    @Test(expected = java.sql.SQLIntegrityConstraintViolationException.class)
    public void testAddingSingleRecordTwiceInCustomerShouldFail() throws Exception {
        Statement statement = connection.createStatement();
        int result;
        ResultSet records;
        result = statement.executeUpdate("insert into jdbc_customer (cust_id,cust_name,adds1,adds2,city,state) values (1,\"prateek\",\"delhi\",\"delhi\",\"delhi\",\"delhi\");");
        assertEquals(result,1);
        result = statement.executeUpdate("insert into jdbc_customer (cust_id,cust_name,adds1,adds2,city,state) values (1,\"prateek\",\"delhi\",\"delhi\",\"delhi\",\"delhi\");");
    }
}
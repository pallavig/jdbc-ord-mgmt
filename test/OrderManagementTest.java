import org.junit.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static junit.framework.Assert.assertEquals;

public class OrderManagementTest {
    static Connection connection;
    static Statement statement;
    static ResultSet records;
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        String url = "jdbc:mariadb://localhost:3306/test";
        connection = DriverManager.getConnection(url, "pallavi", "password");
        statement = connection.createStatement();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        connection.close();
    }

    @Before
    public void setUp() throws Exception {
        records = null;
        String createCustomerQuery = "create table jdbc_customer ( cust_id int primary key," +
                "cust_name varchar(20)," +
                "adds1 varchar(20)," +
                "adds2 varchar(20)," +
                "state varchar(20)," +
                "city varchar(20));";
        statement.execute(createCustomerQuery);
        String createProductQuery = "create table jdbc_products (\n" +
                "product_id int primary key,\n" +
                "product_name varchar(20),\n" +
                "unit_price float\n" +
                ");";
        statement.execute(createProductQuery);
        String createOrdersQuery  = "create table jdbc_order (\n" +
                "order_id int primary key auto_increment,\n" +
                "cust_id int,\n" +
                "date_of_order date,\n" +
                "date_of_delivery date,\n" +
                "total_bill float );";
        statement.execute(createOrdersQuery);
        String alterOrdersToAddCustomerIdForeignKey = "alter table jdbc_order add constraint cust_id_jdbc_fk" +
                " foreign key(cust_id) references jdbc_customer(cust_id);";
        boolean res = statement.execute(alterOrdersToAddCustomerIdForeignKey);
        String createOrderItemQuery = "create table jdbc_order_item (\n" +
                "order_item_id int auto_increment primary key,\n" +
                "product_id int,\n" +
                "order_id int,\n" +
                "quantity float,\n" +
                "price float\n" +
                ");";
        statement.execute(createOrderItemQuery);
        String alterOrderItemToAddProductIdForeignKey = "alter table jdbc_order_item add constraint " +
                "product_id_jdbc_order_item_fk foreign key(product_id) references jdbc_products(product_id);";
        String alterOrderItemToAddOrderIdForeignKey = "alter table jdbc_order_item add constraint " +
                "order_id_jdbc_order_item_fk foreign key(order_id) references jdbc_order(order_id);";
        statement.execute(alterOrderItemToAddOrderIdForeignKey);
    }

    @After
    public void tearDown() throws Exception {
        String dropOrderItemQuery = "drop table jdbc_order_item";
        statement.execute(dropOrderItemQuery);
        String dropOrdersQuery = "drop table jdbc_order;";
        statement.execute(dropOrdersQuery);
        String dropCustomerQuery = "drop table jdbc_customer;";
        statement.execute(dropCustomerQuery);
        String dropProductQuery = "drop table jdbc_products;";
        statement.execute(dropProductQuery);
    }

    @Test
    public void testInsertingRecordInCustomer() throws Exception {
        int affectedRows;
        affectedRows = statement.executeUpdate("insert into jdbc_customer (cust_id,cust_name,adds1,adds2,city,state) values (1,\"prateek\",\"delhi\",\"delhi\",\"delhi\",\"delhi\");");
        assertEquals(affectedRows, 1);
        records = statement.executeQuery("select cust_name from jdbc_customer where cust_id = 1");
        records.next();
        assert ("prateek".equals(records.getString("cust_name")));
    }

    @Test
    public void testDeletingRecordFromCustomerWhenNoDependentRows() throws Exception {
        int affectedRows;
        affectedRows = statement.executeUpdate("insert into jdbc_customer (cust_id,cust_name,adds1,adds2,city,state) values (1,\"prateek\",\"delhi\",\"delhi\",\"delhi\",\"delhi\");");
        assertEquals(affectedRows, 1);
        affectedRows = statement.executeUpdate("delete from jdbc_customer where cust_id=1;");
        assertEquals(affectedRows,1);
    }

    @Test(expected = java.sql.SQLIntegrityConstraintViolationException.class)
    public void testDeletingRecordFromCustomerWhenDependentRowsArePresent() throws Exception {
        String customerInsertionQuery = "insert into jdbc_customer (cust_id,cust_name,adds1,adds2,city,state) values (1,\"prateek\",\"delhi\",\"delhi\",\"delhi\",\"delhi\");";
        String orderInsertionQuery = "insert into jdbc_order (cust_id,date_of_order,date_of_delivery,total_bill) values" +
                "(1,current_date,current_date,100)";
        String customerDeletionQuery = "delete from jdbc_customer";
        statement.executeUpdate(customerInsertionQuery);
        statement.executeUpdate(orderInsertionQuery);
        statement.executeUpdate(customerDeletionQuery);

    }

    @Test(expected = java.sql.SQLIntegrityConstraintViolationException.class)
    public void testAddingSingleRecordTwiceInCustomerShouldFail() throws Exception {
        int affectedRows;
        affectedRows = statement.executeUpdate("insert into jdbc_customer (cust_id,cust_name,adds1,adds2,city,state) values (1,\"prateek\",\"delhi\",\"delhi\",\"delhi\",\"delhi\");");
        assertEquals(affectedRows, 1);
        statement.executeUpdate("insert into jdbc_customer (cust_id,cust_name,adds1,adds2,city,state) values (1,\"prateek\",\"delhi\",\"delhi\",\"delhi\",\"delhi\");");
    }

    @Test
    public void testInsertingSingleProductInProductsTable() throws Exception {
        String insertionQuery = "insert into jdbc_products(product_id,product_name,unit_price) values (1,'sundaram notebook',100);";
        String selectionQuery = "select product_name from jdbc_products where product_id = 1;";
        int affectedRows;
        affectedRows = statement.executeUpdate(insertionQuery);
        assertEquals(affectedRows,1);
        records = statement.executeQuery(selectionQuery);
        records.next();
        assertEquals(records.getString("product_name"),"sundaram notebook");
    }

    @Test
    public void testDeletingProductFromProductsTable() throws Exception {
        String insertionQuery = "insert into jdbc_products(product_id,product_name,unit_price) values (1,'sundaram notebook',100);";
        String deletionQuery = "delete from jdbc_products where product_id=1";
        statement.executeUpdate(insertionQuery);
        statement.executeUpdate(deletionQuery);
    }

    @Test(expected = java.sql.SQLIntegrityConstraintViolationException.class)
    public void testAddingSameProductTwice() throws Exception {
        String insertionQuery = "insert into jdbc_products(product_id,product_name,unit_price) values (1,'sundaram notebook',100);";
        statement.executeUpdate(insertionQuery);
        statement.executeUpdate(insertionQuery);
    }

    @Test
    public void testAddingItemsForAnOrder() throws Exception {
        String productInsertionQuery = "insert into jdbc_products(product_id,product_name,unit_price) values (1,'sundaram notebook',100);";
        String customerInsertionQuery = "insert into jdbc_customer (cust_id,cust_name,adds1,adds2,city,state) values (1,\"prateek\",\"delhi\",\"delhi\",\"delhi\",\"delhi\");";
        String orderInsertionQuery = "insert into jdbc_order (cust_id,date_of_order,date_of_delivery,total_bill) values" +
                "(1,current_date,current_date,100)";
        String orderItemInsertionQuery = "insert into jdbc_order_item (product_id,order_id,quantity,price) values(1,1,1,100)";
        statement.executeUpdate(productInsertionQuery);
        statement.executeUpdate(customerInsertionQuery);
        statement.executeUpdate(orderInsertionQuery);
        int affectedRows = statement.executeUpdate(orderItemInsertionQuery);
        assert(affectedRows == 1);
    }
}
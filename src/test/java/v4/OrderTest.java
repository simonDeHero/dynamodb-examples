package v4;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTransactionWriteExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedScanList;
import com.amazonaws.services.dynamodbv2.datamodeling.TransactionWriteRequest;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TimeToLiveSpecification;
import com.amazonaws.services.dynamodbv2.model.TransactionCanceledException;
import com.amazonaws.services.dynamodbv2.model.UpdateTimeToLiveRequest;
import com.amazonaws.waiters.WaiterParameters;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class OrderTest {

    private static DynamoDBProxyServer server;
    private static AmazonDynamoDB client;
    private static DynamoDBMapper mapper;

    @BeforeAll
    public static void beforeAll() throws Exception {

        System.setProperty("sqlite4java.library.path", "native-libs");
        server = ServerRunner.createServerFromCommandLineArgs(new String[]{"-inMemory", "-port", "4566"});
        server.start();

        client = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("foo", "bar")))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:4566", "eu-west-1"))
                .build();

        CreateTableRequest createTableRequest = new CreateTableRequest()
                .withTableName(OrderByPoidGeid.TABLE_NAME)
                .withAttributeDefinitions(new AttributeDefinition("pOIDgEID", ScalarAttributeType.S))
                .withKeySchema(new KeySchemaElement("pOIDgEID", KeyType.HASH))
                .withBillingMode(BillingMode.PAY_PER_REQUEST);

        CreateTableResult createTableResult = client.createTable(createTableRequest);
        System.out.println(createTableResult.getTableDescription());

        client.waiters().tableExists().run(new WaiterParameters<>(new DescribeTableRequest(OrderByPoidGeid.TABLE_NAME)));
        System.out.println("table " + OrderByPoidGeid.TABLE_NAME + " is ACTIVE now");

        UpdateTimeToLiveRequest updateTimeToLiveRequest = new UpdateTimeToLiveRequest()
                .withTableName(OrderByPoidGeid.TABLE_NAME)
                .withTimeToLiveSpecification(new TimeToLiveSpecification()
                        .withAttributeName("ttl")
                        .withEnabled(true));
        client.updateTimeToLive(updateTimeToLiveRequest);

        createTableRequest = new CreateTableRequest()
                .withTableName(OrderByPoidGk.TABLE_NAME)
                .withAttributeDefinitions(new AttributeDefinition("pOIDgK", ScalarAttributeType.S))
                .withKeySchema(new KeySchemaElement("pOIDgK", KeyType.HASH))
                .withBillingMode(BillingMode.PAY_PER_REQUEST);

        createTableResult = client.createTable(createTableRequest);
        System.out.println(createTableResult.getTableDescription());

        client.waiters().tableExists().run(new WaiterParameters<>(new DescribeTableRequest(OrderByPoidGk.TABLE_NAME)));
        System.out.println("table " + OrderByPoidGeid.TABLE_NAME + " is ACTIVE now");

        updateTimeToLiveRequest = new UpdateTimeToLiveRequest()
                .withTableName(OrderByPoidGk.TABLE_NAME)
                .withTimeToLiveSpecification(new TimeToLiveSpecification()
                        .withAttributeName("ttl")
                        .withEnabled(true));
        client.updateTimeToLive(updateTimeToLiveRequest);

        DynamoDBMapperConfig dynamoDBMapperConfig = DynamoDBMapperConfig.builder()
                .withConsistentReads(DynamoDBMapperConfig.ConsistentReads.EVENTUAL)
                .build();
        mapper = new DynamoDBMapper(client, dynamoDBMapperConfig);
    }

    @AfterEach
    public void afterEach() {
        PaginatedScanList<OrderByPoidGeid> allOrdersByPoidGeid = mapper.scan(OrderByPoidGeid.class, new DynamoDBScanExpression());
        allOrdersByPoidGeid.forEach(order -> mapper.delete(order));
        PaginatedScanList<OrderByPoidGk> allOrdersByPoidGk = mapper.scan(OrderByPoidGk.class, new DynamoDBScanExpression());
        allOrdersByPoidGk.forEach(order -> mapper.delete(order));
    }

    @AfterAll
    public static void afterAll() throws Exception {
        client.deleteTable(OrderByPoidGeid.TABLE_NAME);
        client.deleteTable(OrderByPoidGk.TABLE_NAME);
        server.stop();
    }

    // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html#default-limits-throughput-capacity-modes
    // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transactions.html
    // 2 writes per item (one to prepare TX, one to commit TX) -> 2 order items per TX -> 4 writes -> 2 WRU per write due to TX -> 8 WRU ?
    /*
     * https://calculator.s3.amazonaws.com/index.html -> reads are super cheap, writes are super expensive
     * - 300K orders per day, per order 2 order items are written in TX -> 18 mio items written in TX per month
     * - in EU vendor relation writes per day: 120
     * - in US: 180K !!!!! we have to invent a smarter algorithm for vendor relation updates (smarter than now: delete everything and write everything)
     */
    @Test
    public void testThrowExceptionWhenInsertingDuplicate() {

        OrderByPoidGeid orderByPoidGeid = new OrderByPoidGeid("1234", "PY_AR", "PY", "grocery", Instant.now().plus(Duration.ofDays(30)));
        OrderByPoidGk orderByPoidGk = new OrderByPoidGk("1234", "PY_AR", "PY", "grocery", Instant.now().plus(Duration.ofDays(30)));

        TransactionWriteRequest transactionWriteRequest = new TransactionWriteRequest()
                // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-example.html
                .addPut(orderByPoidGeid, new DynamoDBTransactionWriteExpression().withConditionExpression("attribute_not_exists(pOIDgEID)"))
                .addPut(orderByPoidGk, new DynamoDBTransactionWriteExpression().withConditionExpression("attribute_not_exists(pOIDgK)"));

        mapper.transactionWrite(transactionWriteRequest);


        // violates uniqueness
        OrderByPoidGeid orderByPoidGeidDuplicateGeid = new OrderByPoidGeid("1234", "PY_AR", "FOO", "grocery", Instant.now().plus(Duration.ofDays(30)));
        // does not violate uniqueness
        OrderByPoidGk orderByPoidGkDuplicateGeid = new OrderByPoidGk("1234", "PY_AR", "FOO", "grocery", Instant.now().plus(Duration.ofDays(30)));

        TransactionWriteRequest duplicateGeidWriteRequest = new TransactionWriteRequest()
                .addPut(orderByPoidGeidDuplicateGeid, new DynamoDBTransactionWriteExpression().withConditionExpression("attribute_not_exists(pOIDgEID)"))
                .addPut(orderByPoidGkDuplicateGeid, new DynamoDBTransactionWriteExpression().withConditionExpression("attribute_not_exists(pOIDgK)"));

        assertThrows(TransactionCanceledException.class, () -> mapper.transactionWrite(duplicateGeidWriteRequest));

        // check, nothing has been persisted to both tables!

        PaginatedScanList<OrderByPoidGeid> allOrdersByPoidGeid = mapper.scan(OrderByPoidGeid.class, new DynamoDBScanExpression());
        assertEquals(1, allOrdersByPoidGeid.size());
        OrderByPoidGeid fetchedOrderByPoidGeid = allOrdersByPoidGeid.get(0);
        assertEquals(orderByPoidGeid.getCleanupAfter().truncatedTo(ChronoUnit.SECONDS), fetchedOrderByPoidGeid.getCleanupAfter().truncatedTo(ChronoUnit.SECONDS));
        assertEquals(orderByPoidGeid.getGlobalEntityId(), fetchedOrderByPoidGeid.getGlobalEntityId());
        assertEquals(orderByPoidGeid.getGlobalKey(), fetchedOrderByPoidGeid.getGlobalKey());
        assertEquals(orderByPoidGeid.getHashKey(), fetchedOrderByPoidGeid.getHashKey());
        assertEquals(orderByPoidGeid.getOrderJsonVersion(), fetchedOrderByPoidGeid.getOrderJsonVersion());
        assertEquals(orderByPoidGeid.getPlatformOrderId(), fetchedOrderByPoidGeid.getPlatformOrderId());
        assertEquals(orderByPoidGeid.getTtl(), fetchedOrderByPoidGeid.getTtl());

        PaginatedScanList<OrderByPoidGk> allOrdersByPoidGk = mapper.scan(OrderByPoidGk.class, new DynamoDBScanExpression());
        assertEquals(1, allOrdersByPoidGk.size());
        OrderByPoidGk fetchedOrderByPoidGk = allOrdersByPoidGk.get(0);
        assertEquals(orderByPoidGk.getCleanupAfter().truncatedTo(ChronoUnit.SECONDS), fetchedOrderByPoidGk.getCleanupAfter().truncatedTo(ChronoUnit.SECONDS));
        assertEquals(orderByPoidGk.getGlobalEntityId(), fetchedOrderByPoidGk.getGlobalEntityId());
        assertEquals(orderByPoidGk.getGlobalKey(), fetchedOrderByPoidGk.getGlobalKey());
        assertEquals(orderByPoidGk.getHashKey(), fetchedOrderByPoidGk.getHashKey());
        assertEquals(orderByPoidGk.getOrderJsonVersion(), fetchedOrderByPoidGk.getOrderJsonVersion());
        assertEquals(orderByPoidGk.getPlatformOrderId(), fetchedOrderByPoidGk.getPlatformOrderId());
        assertEquals(orderByPoidGk.getTtl(), fetchedOrderByPoidGk.getTtl());

        // does not violate uniqueness
        OrderByPoidGeid orderByPoidGeidDuplicateGk = new OrderByPoidGeid("1234", "PY_DO", "PY", "grocery", Instant.now().plus(Duration.ofDays(30)));
        // violates uniqueness
        OrderByPoidGk orderByPoidGkDuplicateGk = new OrderByPoidGk("1234", "PY_DO", "PY", "grocery", Instant.now().plus(Duration.ofDays(30)));

        TransactionWriteRequest duplicateGkWriteRequest = new TransactionWriteRequest()
                .addPut(orderByPoidGeidDuplicateGk, new DynamoDBTransactionWriteExpression().withConditionExpression("attribute_not_exists(pOIDgEID)"))
                .addPut(orderByPoidGkDuplicateGk, new DynamoDBTransactionWriteExpression().withConditionExpression("attribute_not_exists(pOIDgK)"));

        assertThrows(TransactionCanceledException.class, () -> mapper.transactionWrite(duplicateGkWriteRequest));

        // check, nothing has been persisted to both tables!

        allOrdersByPoidGeid = mapper.scan(OrderByPoidGeid.class, new DynamoDBScanExpression());
        assertEquals(1, allOrdersByPoidGeid.size());
        fetchedOrderByPoidGeid = allOrdersByPoidGeid.get(0);
        assertEquals(orderByPoidGeid.getCleanupAfter().truncatedTo(ChronoUnit.SECONDS), fetchedOrderByPoidGeid.getCleanupAfter().truncatedTo(ChronoUnit.SECONDS));
        assertEquals(orderByPoidGeid.getGlobalEntityId(), fetchedOrderByPoidGeid.getGlobalEntityId());
        assertEquals(orderByPoidGeid.getGlobalKey(), fetchedOrderByPoidGeid.getGlobalKey());
        assertEquals(orderByPoidGeid.getHashKey(), fetchedOrderByPoidGeid.getHashKey());
        assertEquals(orderByPoidGeid.getOrderJsonVersion(), fetchedOrderByPoidGeid.getOrderJsonVersion());
        assertEquals(orderByPoidGeid.getPlatformOrderId(), fetchedOrderByPoidGeid.getPlatformOrderId());
        assertEquals(orderByPoidGeid.getTtl(), fetchedOrderByPoidGeid.getTtl());

        allOrdersByPoidGk = mapper.scan(OrderByPoidGk.class, new DynamoDBScanExpression());
        assertEquals(1, allOrdersByPoidGk.size());
        fetchedOrderByPoidGk = allOrdersByPoidGk.get(0);
        assertEquals(orderByPoidGk.getCleanupAfter().truncatedTo(ChronoUnit.SECONDS), fetchedOrderByPoidGk.getCleanupAfter().truncatedTo(ChronoUnit.SECONDS));
        assertEquals(orderByPoidGk.getGlobalEntityId(), fetchedOrderByPoidGk.getGlobalEntityId());
        assertEquals(orderByPoidGk.getGlobalKey(), fetchedOrderByPoidGk.getGlobalKey());
        assertEquals(orderByPoidGk.getHashKey(), fetchedOrderByPoidGk.getHashKey());
        assertEquals(orderByPoidGk.getOrderJsonVersion(), fetchedOrderByPoidGk.getOrderJsonVersion());
        assertEquals(orderByPoidGk.getPlatformOrderId(), fetchedOrderByPoidGk.getPlatformOrderId());
        assertEquals(orderByPoidGk.getTtl(), fetchedOrderByPoidGk.getTtl());
    }
}

package v3;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBSaveExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedScanList;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TimeToLiveSpecification;
import com.amazonaws.services.dynamodbv2.model.UpdateTimeToLiveRequest;
import com.amazonaws.waiters.WaiterParameters;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

// https://www.baeldung.com/dynamodb-local-integration-tests
// or https://stackoverflow.com/questions/26901613/easier-dynamodb-local-testing
public class DynamoDbTest {

    private static DynamoDBProxyServer server;
    private static AmazonDynamoDB amazonDynamoDB;
    private static DynamoDBMapper dynamoDBMapper;

    @BeforeAll
    public static void beforeAll() throws Exception {

        System.setProperty("sqlite4java.library.path", "native-libs");
        server = ServerRunner.createServerFromCommandLineArgs(new String[]{"-inMemory", "-port", "4566"});
        server.start();

        amazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("foo", "bar")))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:4566", "eu-west-1"))
                .build();

        CreateTableRequest createTableRequest = new CreateTableRequest()
                .withTableName("testTable")
                .withAttributeDefinitions(
                        new AttributeDefinition("pOIDgEID", ScalarAttributeType.S),
                        new AttributeDefinition("pOIDgK", ScalarAttributeType.S) // backed by GSI
                )
                .withKeySchema(new KeySchemaElement("pOIDgEID", KeyType.HASH))
                .withGlobalSecondaryIndexes(new GlobalSecondaryIndex()
                        .withIndexName("pOIDgKGsi")
                        .withKeySchema(new KeySchemaElement("pOIDgK", KeyType.HASH))
                        // usually items accessed by GSI would also need all columns from table, but here we don't need
                        // them, as we only use GSI for checking unique order ids
                        .withProjection(new Projection().withProjectionType(ProjectionType.KEYS_ONLY)))
                .withBillingMode(BillingMode.PAY_PER_REQUEST);

        CreateTableResult createTableResult = amazonDynamoDB.createTable(createTableRequest);
        System.out.println(createTableResult.getTableDescription());

        amazonDynamoDB.waiters().tableExists()
                .run(new WaiterParameters<>(new DescribeTableRequest("testTable")));
        System.out.println("testTable is ACTIVE now");

        // https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDBClient.html#updateTimeToLive-com.amazonaws.services.dynamodbv2.model.UpdateTimeToLiveRequest-
        UpdateTimeToLiveRequest updateTimeToLiveRequest = new UpdateTimeToLiveRequest()
                .withTableName("testTable")
                .withTimeToLiveSpecification(new TimeToLiveSpecification()
                        .withAttributeName("ttl")
                        .withEnabled(true));
        amazonDynamoDB.updateTimeToLive(updateTimeToLiveRequest);

        DynamoDBMapperConfig dynamoDBMapperConfig = DynamoDBMapperConfig.builder()
                .withConsistentReads(DynamoDBMapperConfig.ConsistentReads.EVENTUAL)
                .build();
        dynamoDBMapper = new DynamoDBMapper(amazonDynamoDB, dynamoDBMapperConfig);
    }

    @AfterEach
    public void afterEach() {
        PaginatedScanList<Order> allOrders = dynamoDBMapper.scan(Order.class, new DynamoDBScanExpression());
        allOrders.forEach(order -> dynamoDBMapper.delete(order));
    }

    @AfterAll
    public static void afterAll() throws Exception {
        amazonDynamoDB.deleteTable("testTable");
        server.stop();
    }

    @Test
    public void testSaveAndLoadByPkAndByGsi() {

        Order order = new Order("1234", "PY_AR", "PY", "grocery", Instant.now().plus(Duration.ofDays(30)));

        dynamoDBMapper.save(order);

        Order orderFetchedByHash = dynamoDBMapper.load(Order.class, order.getPoidGeid());
        // all fields contained
        System.out.println(orderFetchedByHash);

        // e.g. for cancel, where we don't get the gEID in the request, but we only have the gK in the auth
        DynamoDBQueryExpression<Order> gsiQuery = new DynamoDBQueryExpression<Order>()
                .withIndexName("pOIDgKGsi")
                .withConsistentRead(false) // mandatory here, cannot use consistent read on GSI
                .withKeyConditionExpression("pOIDgK = :pOIDgK")
                .withExpressionAttributeValues(Map.of(":pOIDgK", new AttributeValue(order.getPoidGk())));
        PaginatedQueryList<Order> ordersFetchedByGsi = dynamoDBMapper.query(Order.class, gsiQuery);
        // NOT all fields are projected into GSI and so not available here
        ordersFetchedByGsi.forEach(System.out::println);
    }

    @Test
    public void testThrowExceptionWhenInsertingDuplicateHashKey() {

        Order order = new Order("1234", "PY_AR", "PY", "grocery", Instant.now().plus(Duration.ofDays(30)));

        dynamoDBMapper.save(order);

        Order orderDuplicate = new Order("1234", "PY_AR", "FOO", "grocery", Instant.now().plus(Duration.ofDays(30)));

        DynamoDBSaveExpression saveExpression = new DynamoDBSaveExpression()
                .withExpected(Map.of("pOIDgEID", new ExpectedAttributeValue(false)));
        assertThrows(ConditionalCheckFailedException.class, () -> dynamoDBMapper.save(orderDuplicate, saveExpression));
    }

    // https://stackoverflow.com/questions/24393944/uniqueness-in-dynamodb-secondary-index
    // "Indexes are updated asynchronously, meaning they'll be eventually consistent, and which also means that dynamodb
    // won't be able to enforce uniqueness at the time when you make the update call"
    @Test
    public void testThrowExceptionWhenInsertingDuplicateGsiKey() {

        Order order = new Order("1234", "PY_AR", "PY", "grocery", Instant.now().plus(Duration.ofDays(30)));

        dynamoDBMapper.save(order);

        Order orderDuplicate = new Order("1234", "FOO", "PY", "grocery", Instant.now().plus(Duration.ofDays(30)));

        DynamoDBSaveExpression saveExpression = new DynamoDBSaveExpression()
                .withExpected(Map.of("pOIDgK", new ExpectedAttributeValue(false)));
        assertThrows(ConditionalCheckFailedException.class, () -> dynamoDBMapper.save(orderDuplicate, saveExpression));
    }
}

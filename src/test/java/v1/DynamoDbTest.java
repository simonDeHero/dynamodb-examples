package v1;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
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
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ConditionalOperator;
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
import com.amazonaws.waiters.WaiterParameters;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.CoreComponents.html#HowItWorks.CoreComponents.PrimaryKey
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DynamoDbTest {

    private AmazonDynamoDB amazonDynamoDB;
    private DynamoDBMapper dynamoDBMapper;

    @BeforeAll
    public void beforeAll() {

        amazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:4566", "eu-west-1"))
                .build();

        // https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/examples-dynamodb-tables.html
        CreateTableRequest createTableRequest = new CreateTableRequest()
                .withTableName("testTable")
                .withAttributeDefinitions(
                        new AttributeDefinition("pOID_gEID", ScalarAttributeType.S),
                        new AttributeDefinition("pOID_gK", ScalarAttributeType.S) // backed by GSI
                        // https://stackoverflow.com/questions/30866030/number-of-attributes-in-key-schema-must-match-the-number-of-attributes-defined-i/30924384_30924384
                        // new AttributeDefinition("cleanupAt", ScalarAttributeType.S), // only include key attributes
                )
                .withKeySchema(new KeySchemaElement("pOID_gEID", KeyType.HASH))
                // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSIJavaDocumentAPI.html
                // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSIJavaDocumentAPI.Example.html
                .withGlobalSecondaryIndexes(new GlobalSecondaryIndex()
                        .withIndexName("pOIDgKGsi")
                        .withKeySchema(new KeySchemaElement("pOID_gK", KeyType.HASH))
                        // usually items accessed by GSI would also need all columns from table, but here we don't need
                        // them, as we only use GSI for checking unique order ids
                        .withProjection(new Projection().withProjectionType(ProjectionType.KEYS_ONLY)))
                .withBillingMode(BillingMode.PAY_PER_REQUEST);

        CreateTableResult createTableResult = amazonDynamoDB.createTable(createTableRequest);
        System.out.println(createTableResult.getTableDescription());

        // https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/javav2/example_code/dynamodb/src/main/java/com/example/dynamodb/CreateTable.java
        amazonDynamoDB.waiters().tableExists()
                .run(new WaiterParameters<>(new DescribeTableRequest("testTable")));
        System.out.println("testTable is ACTIVE now");

        // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBMapper.OptionalConfig.html
        DynamoDBMapperConfig dynamoDBMapperConfig = DynamoDBMapperConfig.builder()
                .withConsistentReads(DynamoDBMapperConfig.ConsistentReads.EVENTUAL)
                .build();
        dynamoDBMapper = new DynamoDBMapper(amazonDynamoDB, dynamoDBMapperConfig);
    }

    @AfterAll
    public void afterAll() {
        amazonDynamoDB.deleteTable("testTable");
    }

    @AfterEach
    public void afterEach() {
        PaginatedScanList<Order> allOrders = dynamoDBMapper.scan(Order.class, new DynamoDBScanExpression());
        allOrders.forEach(order -> dynamoDBMapper.delete(order));
    }

    // https://docs.aws.amazon.com/de_de/amazondynamodb/latest/developerguide/DynamoDBMapper.CRUDExample1.html
    @Test
    public void testSaveAndLoadByPkAndByGsi() {

        Order order = new Order();
        order.setCleanupAfter(Instant.now().plus(Duration.ofDays(30)).toString());
        order.setPoidGeid("1234_PY_AR");
        order.setPoidGk("1234_PY");

        dynamoDBMapper.save(order);

        Order orderFetchedByHash = dynamoDBMapper.load(Order.class, "1234_PY_AR");
        System.out.println(orderFetchedByHash);

        // https://docs.amazonaws.cn/en_us/amazondynamodb/latest/developerguide/DAX.client.QueryGSI.html
        DynamoDBQueryExpression<Order> gsiQuery = new DynamoDBQueryExpression<Order>()
                .withIndexName("pOIDgKGsi")
                .withConsistentRead(false) // mandatory here, cannot use consistent read on GSI
                .withKeyConditionExpression("pOID_gK = :pOIDgK")
                .withExpressionAttributeValues(Map.of(":pOIDgK", new AttributeValue("1234_PY")));
        PaginatedQueryList<Order> ordersFetchedByGsi = dynamoDBMapper.query(Order.class, gsiQuery);
        ordersFetchedByGsi.forEach(System.out::println);
    }

    @Test
    public void testHashKeyUnique_justOverwrites() {

        Order order = new Order();
        order.setCleanupAfter(Instant.now().plus(Duration.ofDays(30)).toString());
        order.setPoidGeid("1234_PY_AR");
        order.setPoidGk("1234_PY");

        dynamoDBMapper.save(order);

        Order otherOrder = new Order();
        otherOrder.setCleanupAfter(Instant.now().plus(Duration.ofDays(30)).toString());
        otherOrder.setPoidGeid("1234_PY_AR");
        otherOrder.setPoidGk("1234_FOO");

        // no exception, just updates the order with hash 1234_PY_AR
        dynamoDBMapper.save(otherOrder);

        PaginatedScanList<Order> allOrders = dynamoDBMapper.scan(Order.class, new DynamoDBScanExpression());
        allOrders.forEach(System.out::println);
    }

    @Test
    public void testHashKeyUnique_throwsExceptionForDuplicate() {

        Order order = new Order();
        order.setCleanupAfter(Instant.now().plus(Duration.ofDays(30)).toString());
        order.setPoidGeid("1234_FOO_DE");
        order.setPoidGk("1234_FOO");

        dynamoDBMapper.save(order);

        // https://stackoverflow.com/questions/25350256/dynamodb-key-uniqueness-across-primary-and-global-secondary-index
        // https://stackoverflow.com/questions/12457130/what-will-happen-if-we-insert-into-dynamo-db-with-a-duplicate-hash-key/12460690#12460690
        // https://aws.amazon.com/blogs/database/simulating-amazon-dynamodb-unique-constraints-using-transactions/
        // https://stackoverflow.com/questions/12920884/is-there-a-way-to-enforce-unique-constraint-on-a-property-field-other-than-the

        // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html#WorkingWithItems.ConditionalUpdate
        DynamoDBSaveExpression saveExpression = new DynamoDBSaveExpression();
        Map<String, ExpectedAttributeValue> expectedAttributes = Map.of(
                "pOID_gEID", new ExpectedAttributeValue(false),
                "pOID_gK", new ExpectedAttributeValue(false));
        saveExpression.setExpected(expectedAttributes);
        saveExpression.setConditionalOperator(ConditionalOperator.AND);

        Order orderDuplicateGeid = new Order();
        orderDuplicateGeid.setCleanupAfter(Instant.now().plus(Duration.ofDays(30)).toString());
        orderDuplicateGeid.setPoidGeid("1234_FOO_DE"); // already existing in order
        orderDuplicateGeid.setPoidGk("1234_OTHER");

        assertThrows(ConditionalCheckFailedException.class, () -> dynamoDBMapper.save(orderDuplicateGeid, saveExpression));

        // TODO does not work, why?
        // do we need uniqueness on p
        Order orderDuplicateGK = new Order();
        orderDuplicateGK.setCleanupAfter(Instant.now().plus(Duration.ofDays(30)).toString());
        orderDuplicateGK.setPoidGeid("1234_OTHER");
        orderDuplicateGK.setPoidGk("1234_FOO"); // already existing in order

        assertThrows(ConditionalCheckFailedException.class, () -> dynamoDBMapper.save(orderDuplicateGK, saveExpression));
    }

    @Test
    public void testSaveMultipleChangesInTX() {

//        v1.Order order = new v1.Order();
//        order.setCleanupAfter(Instant.now().plus(Duration.ofDays(30)).toString());
//        order.setpOIDgEID("1234_PY_AR");
//        order.setpOIDgK("1234_PY");
//
//        dynamoDBMapper.save(order);
//
//        v1.Order otherOrder = new v1.Order();
//        otherOrder.setCleanupAfter(Instant.now().plus(Duration.ofDays(30)).toString());
//        otherOrder.setpOIDgEID("1234_PY_AR");
//        otherOrder.setpOIDgK("1234_FOO");
//
//        TransactionWriteRequest transactionWriteRequest = new TransactionWriteRequest()
//                .addPut(order)
//                .addPut(otherOrder);
//        dynamoDBMapper.transactionWrite(transactionWriteRequest);
//
//        PaginatedScanList<v1.Order> allOrders = dynamoDBMapper.scan(v1.Order.class, new DynamoDBScanExpression());
//        allOrders.forEach(System.out::println);
    }
}

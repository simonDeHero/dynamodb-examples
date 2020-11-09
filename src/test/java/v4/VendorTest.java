package v4;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedScanList;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.waiters.WaiterParameters;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class VendorTest {

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
                .withTableName(Vendor.TABLE_NAME)
                .withAttributeDefinitions(
                        new AttributeDefinition("pVIDgK", ScalarAttributeType.S),
                        new AttributeDefinition("rVID", ScalarAttributeType.S)
                )
                .withKeySchema(new KeySchemaElement("pVIDgK", KeyType.HASH))
                .withGlobalSecondaryIndexes(new GlobalSecondaryIndex()
                        .withIndexName("rVIDGsi")
                        .withKeySchema(new KeySchemaElement("rVID", KeyType.HASH))
                        .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))
                .withBillingMode(BillingMode.PAY_PER_REQUEST);

        CreateTableResult createTableResult = client.createTable(createTableRequest);
        System.out.println(createTableResult.getTableDescription());

        client.waiters().tableExists().run(new WaiterParameters<>(new DescribeTableRequest(Vendor.TABLE_NAME)));
        System.out.println("table " + Vendor.TABLE_NAME + " is ACTIVE now");

        DynamoDBMapperConfig dynamoDBMapperConfig = DynamoDBMapperConfig.builder()
                .withConsistentReads(DynamoDBMapperConfig.ConsistentReads.EVENTUAL)
                .build();
        mapper = new DynamoDBMapper(client, dynamoDBMapperConfig);
    }

    @AfterEach
    public void afterEach() {
        PaginatedScanList<Vendor> allVendors = mapper.scan(Vendor.class, new DynamoDBScanExpression());
        allVendors.forEach(vendor -> mapper.delete(vendor));
    }

    @AfterAll
    public static void afterAll() throws Exception {
        client.deleteTable(Vendor.TABLE_NAME);
        server.stop();
    }

    @Test
    public void testDeleteNonExistingVendor() {

        Vendor vendor = new Vendor("asdf", "LH", "1234", Instant.now(), "");
        mapper.save(vendor);
        // vendor.version is 1 now

        Vendor toBeStale = mapper.load(vendor);

        mapper.delete(vendor); // works

        // ConditionalCheckFailedException, as it is expected, that the vendor still exists in DB with version 1
        assertThrows(ConditionalCheckFailedException.class, () -> mapper.delete(toBeStale));

        assertNull(mapper.load(vendor));
    }
}

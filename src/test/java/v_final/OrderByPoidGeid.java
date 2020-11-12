package v_final;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIgnore;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

import java.time.Instant;

// https://docs.aws.amazon.com/de_de/amazondynamodb/latest/developerguide/DynamoDBMapper.html
@DynamoDBTable(tableName = OrderByPoidGeid.TABLE_NAME)
public class OrderByPoidGeid {

    public static final String TABLE_NAME = "orderByPoidGeid";

    private String platformOrderId;
    private String globalEntityId;
    private String globalKey;
    private String orderJsonVersion;
    private Instant cleanupAfter;

    public OrderByPoidGeid(String platformOrderId, String globalEntityId, String globalKey, String orderJsonVersion, Instant cleanupAfter) {
        this.platformOrderId = platformOrderId;
        this.globalEntityId = globalEntityId;
        this.globalKey = globalKey;
        this.orderJsonVersion = orderJsonVersion;
        this.cleanupAfter = cleanupAfter;
    }

    // needed for instantiation by mapper
    public OrderByPoidGeid() {
    }

    @DynamoDBIgnore
    public String getPlatformOrderId() {
        return platformOrderId;
    }

    public void setPlatformOrderId(String platformOrderId) {
        this.platformOrderId = platformOrderId;
    }

    @DynamoDBIgnore
    public String getGlobalEntityId() {
        return globalEntityId;
    }

    public void setGlobalEntityId(String globalEntityId) {
        this.globalEntityId = globalEntityId;
    }

    @DynamoDBAttribute(attributeName = "gK")
    public String getGlobalKey() {
        return globalKey;
    }

    public void setGlobalKey(String globalKey) {
        this.globalKey = globalKey;
    }

    @DynamoDBAttribute(attributeName = "oJV")
    public String getOrderJsonVersion() {
        return orderJsonVersion;
    }

    public void setOrderJsonVersion(String orderJsonVersion) {
        this.orderJsonVersion = orderJsonVersion;
    }

    // must be on getter/setter. does not work on public field
    @DynamoDBHashKey(attributeName = "pOIDgEID")
    public String getHashKey() {
        return platformOrderId + "<<>>" + globalEntityId;
    }

    public void setHashKey(String hashKey) {
        String[] parts = hashKey.split("<<>>"); // delimiter must not be used within pOID or gEID
        platformOrderId = parts[0];
        globalEntityId = parts[1];
    }

    @DynamoDBIgnore
    public Instant getCleanupAfter() {
        return cleanupAfter;
    }

    public void setCleanupAfter(Instant cleanupAfter) {
        this.cleanupAfter = cleanupAfter;
    }

    // https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDBClient.html#updateTimeToLive-com.amazonaws.services.dynamodbv2.model.UpdateTimeToLiveRequest-
    @DynamoDBAttribute(attributeName = "ttl")
    public long getTtl() {
        return cleanupAfter.getEpochSecond();
    }

    public void setTtl(long ttl) {
        this.cleanupAfter = Instant.ofEpochSecond(ttl);
    }

    @Override
    public String toString() {
        return "OrderByPoidGeid{" +
                "platformOrderId='" + platformOrderId + '\'' +
                ", globalEntityId='" + globalEntityId + '\'' +
                ", globalKey='" + globalKey + '\'' +
                ", orderJsonVersion='" + orderJsonVersion + '\'' +
                ", cleanupAfter='" + cleanupAfter + '\'' +
                '}';
    }
}

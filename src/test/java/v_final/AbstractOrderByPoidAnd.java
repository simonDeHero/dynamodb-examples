package v_final;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBDocument;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIgnore;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.time.Instant;

@DynamoDBDocument
public abstract class AbstractOrderByPoidAnd {

    protected String platformOrderId;
    protected String globalEntityId;
    protected String globalKey;
    protected String orderJsonVersion;
    protected Instant cleanupAfter;

    public AbstractOrderByPoidAnd(String platformOrderId, String globalEntityId, String globalKey, String orderJsonVersion, Instant cleanupAfter) {
        this.platformOrderId = platformOrderId;
        this.globalEntityId = globalEntityId;
        this.globalKey = globalKey;
        this.orderJsonVersion = orderJsonVersion;
        this.cleanupAfter = cleanupAfter;
    }

    public AbstractOrderByPoidAnd() {
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

    @DynamoDBIgnore
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
        return new ToStringBuilder(this)
                .append("platformOrderId", platformOrderId)
                .append("globalEntityId", globalEntityId)
                .append("globalKey", globalKey)
                .append("orderJsonVersion", orderJsonVersion)
                .append("cleanupAfter", cleanupAfter)
                .toString();
    }
}

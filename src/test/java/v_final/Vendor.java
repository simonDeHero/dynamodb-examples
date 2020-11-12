package v_final;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIgnore;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIndexHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBVersionAttribute;

import java.time.Instant;

@DynamoDBTable(tableName = Vendor.TABLE_NAME)
public class Vendor {

    public static final String TABLE_NAME = "vendor";

    private String platformId;
    private String globalKey;
    private String rpsId;
    private Instant timestamp;
    private String config;
    private boolean isDeleted;

    public Vendor(String platformId, String globalKey, String rpsId, Instant timestamp, String config) {
        this.platformId = platformId;
        this.globalKey = globalKey;
        this.rpsId = rpsId;
        this.timestamp = timestamp;
        this.config = config;
    }

    public Vendor() {
    }

    @DynamoDBIgnore
    public String getPlatformId() {
        return platformId;
    }

    public void setPlatformId(String platformId) {
        this.platformId = platformId;
    }

    @DynamoDBIgnore
    public String getGlobalKey() {
        return globalKey;
    }

    public void setGlobalKey(String globalKey) {
        this.globalKey = globalKey;
    }

    @DynamoDBIndexHashKey(attributeName = "rVID", globalSecondaryIndexName = "rVIDGsi")
    public String getRpsId() {
        return rpsId;
    }

    public void setRpsId(String rpsId) {
        this.rpsId = rpsId;
    }

    @DynamoDBIgnore
    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    @DynamoDBAttribute(attributeName = "config")
    public String getConfig() {
        return config;
    }

    public void setConfig(String config) {
        this.config = config;
    }

    @DynamoDBAttribute(attributeName = "ts")
    public String getCreatedAt() {
        return timestamp.toString();
    }

    public void setCreatedAt(String createdAt) {
        timestamp = Instant.parse(createdAt);
    }

    @DynamoDBHashKey(attributeName = "pVIDgK")
    public String getHashKey() {
        return platformId + "<<>>" + globalKey;
    }

    public void setHashKey(String hashKey) {
        String[] parts = hashKey.split("<<>>"); // delimiter must not be used within pOID or gEID
        platformId = parts[0];
        globalKey = parts[1];
    }

    @DynamoDBAttribute(attributeName = "isDeleted")
    public String isDeleted() {
        return Boolean.toString(isDeleted);
    }

    public void setDeleted(String deleted) {
        isDeleted = Boolean.parseBoolean(deleted);
    }

    @Override
    public String toString() {
        return "Vendor{" +
                "platformId='" + platformId + '\'' +
                ", globalKey='" + globalKey + '\'' +
                ", rpsId='" + rpsId + '\'' +
                ", timestamp=" + timestamp +
                ", config='" + config + '\'' +
                ", isDeleted=" + isDeleted +
                '}';
    }
}

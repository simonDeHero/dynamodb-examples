package v1;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIndexHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

// https://docs.aws.amazon.com/de_de/amazondynamodb/latest/developerguide/DynamoDBMapper.html
@DynamoDBTable(tableName = "testTable")
public class Order {

    private String pOIDgEID;
    private String pOIDgK;
    private String cleanupAfter;

    // must be on getter/setter. does not work on public field
    @DynamoDBHashKey(attributeName = "pOID_gEID")
    public String getPoidGeid() {
        return pOIDgEID;
    }

    public void setPoidGeid(String pOIDgEID) {
        this.pOIDgEID = pOIDgEID;
    }

    @DynamoDBIndexHashKey(attributeName = "pOID_gK", globalSecondaryIndexName = "pOIDgKGsi")
    public String getPoidGk() {
        return pOIDgK;
    }

    public void setPoidGk(String pOIDgK) {
        this.pOIDgK = pOIDgK;
    }

    @DynamoDBAttribute(attributeName = "ttl")
    public String getCleanupAfter() {
        return cleanupAfter;
    }

    public void setCleanupAfter(String cleanupAfter) {
        this.cleanupAfter = cleanupAfter;
    }

    @Override
    public String toString() {
        return "v1.Order{" +
                "pOIDgEID='" + pOIDgEID + '\'' +
                ", pOIDgK='" + pOIDgK + '\'' +
                ", cleanupAfter='" + cleanupAfter + '\'' +
                '}';
    }
}

package v_final;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

import java.time.Instant;

// https://docs.aws.amazon.com/de_de/amazondynamodb/latest/developerguide/DynamoDBMapper.html
@DynamoDBTable(tableName = OrderByPoidGeid.TABLE_NAME)
public class OrderByPoidGeid extends AbstractOrderByPoidAnd {

    public static final String TABLE_NAME = "orderByPoidGeid";

    public OrderByPoidGeid(String platformOrderId, String globalEntityId, String globalKey, String orderJsonVersion, Instant cleanupAfter) {
        super(platformOrderId, globalEntityId, globalKey, orderJsonVersion, cleanupAfter);
    }

    // needed for instantiation by mapper
    public OrderByPoidGeid() {
        super();
    }

    @DynamoDBAttribute(attributeName = "gK")
    public String getGlobalKey() {
        return globalKey;
    }

    public void setGlobalKey(String globalKey) {
        this.globalKey = globalKey;
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
}

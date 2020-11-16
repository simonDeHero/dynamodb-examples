package v_final;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

import java.time.Instant;

// https://docs.aws.amazon.com/de_de/amazondynamodb/latest/developerguide/DynamoDBMapper.html
@DynamoDBTable(tableName = OrderByPoidGk.TABLE_NAME)
public class OrderByPoidGk extends AbstractOrderByPoidAnd {

    public static final String TABLE_NAME = "orderByPoidGk";

    public OrderByPoidGk(String platformOrderId, String globalEntityId, String globalKey, String orderJsonVersion, Instant cleanupAfter) {
        super(platformOrderId, globalEntityId, globalKey, orderJsonVersion, cleanupAfter);
    }

    // needed for instantiation by mapper
    public OrderByPoidGk() {
        super();
    }

    @DynamoDBAttribute(attributeName = "gEID")
    public String getGlobalEntityId() {
        return globalEntityId;
    }

    public void setGlobalEntityId(String globalEntityId) {
        this.globalEntityId = globalEntityId;
    }

    // must be on getter/setter. does not work on public field
    @DynamoDBHashKey(attributeName = "pOIDgK")
    public String getHashKey() {
        return platformOrderId + "<<>>" + globalKey;
    }

    public void setHashKey(String hashKey) {
        String[] parts = hashKey.split("<<>>"); // delimiter must not be used within pOID or gK
        platformOrderId = parts[0];
        globalKey = parts[1];
    }
}

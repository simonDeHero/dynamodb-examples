package v6;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTransactionWriteExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;
import com.amazonaws.services.dynamodbv2.datamodeling.TransactionWriteRequest;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class VendorListenerLogic {

    private final AmazonDynamoDB client;
    private final DynamoDBMapperConfig mapperConfigEventual;
    private final DynamoDBMapperConfig mapperConfigConsistent;
    private final DynamoDBMapper mapper;

    public VendorListenerLogic(AmazonDynamoDB client) {
        this.client = client;
        mapperConfigEventual = DynamoDBMapperConfig.builder()
                .withConsistentReads(DynamoDBMapperConfig.ConsistentReads.EVENTUAL)
                .build();
        mapperConfigConsistent = DynamoDBMapperConfig.builder()
                .withConsistentReads(DynamoDBMapperConfig.ConsistentReads.CONSISTENT)
                .build();
        mapper = new DynamoDBMapper(client);
    }

    public void update(Set<Vendor> latestVendorsForRvid, String rVID, Instant latestTimeStamp) {

        DynamoDBQueryExpression<Vendor> gsiQuery = new DynamoDBQueryExpression<Vendor>()
                .withIndexName("rVIDGsi")
                .withConsistentRead(false)
                .withKeyConditionExpression("rVID = :rVID")
                .withExpressionAttributeValues(Map.of(":rVID", new AttributeValue(rVID)));
        PaginatedQueryList<Vendor> existingVendors = mapper.query(Vendor.class, gsiQuery, mapperConfigConsistent);
        existingVendors.loadAllResults();

        // all which are existing in table, but not in set
        Set<Vendor> existingVendorsToDelete = existingVendors.stream()
                .filter(existingVendor -> latestVendorsForRvid.stream()
                        .noneMatch(latestVendor -> latestVendor.getHashKey().equals(existingVendor.getHashKey())))
                .collect(Collectors.toSet());

        // all which are existing in set, but not in table
        Set<Vendor> queueMessageVendorsToAdd = latestVendorsForRvid.stream()
                .filter(latestVendor -> existingVendors.stream()
                        .noneMatch(existingVendor -> latestVendor.getHashKey().equals(existingVendor.getHashKey())))
                .collect(Collectors.toSet());

        // all which are existing in both
        Set<Vendor> existingVendorsToUpdate = existingVendors.stream()
                .filter(existingVendor -> latestVendorsForRvid.stream()
                        .anyMatch(latestVendor -> latestVendor.getHashKey().equals(existingVendor.getHashKey())))
                .collect(Collectors.toSet());

        // TODO all of those must have the same timestamp, as they have been set in a TX (the formerly given latestTimeStamp).
        //  only if the current latestTimeStamp is newer, go on and update

        // with TX, all changes are made together, or none. so 1 TX wins, if concurrent ongoing.
        // 1 of the concurrent ones is the most recent. it is NOT SURE, that this one wins. but 1 of the concurrent ongoing ones wins.
        // problem we save out of date info. maybe due to issue in queue we get at the same time 2 messages for same rVID, which have been generated
        // actually seconds apart. here the older one might win.
        // TODO only max 25 vendors per TX can be modified!!
        TransactionWriteRequest transactionWriteRequest = new TransactionWriteRequest();
        existingVendorsToDelete.forEach(transactionWriteRequest::addDelete);
        existingVendorsToUpdate.forEach(v -> {
            v.setTimestamp(latestTimeStamp);
            transactionWriteRequest.addUpdate(v, new DynamoDBTransactionWriteExpression().withConditionExpression("ts < " + latestTimeStamp)); // better use epoch seconds
        });
        queueMessageVendorsToAdd.forEach(v -> {
            // TODO convert to vendor
            v.setTimestamp(latestTimeStamp);
            transactionWriteRequest.addPut(v, new DynamoDBTransactionWriteExpression().withConditionExpression("attribute_not_exists(pVIDgK)"));
        });
        mapper.transactionWrite(transactionWriteRequest);

        // TODO if a TransactionCanceledException happens then a condition was not met. we retry the whole function
        //  (load, check if update still needed by latestTimeStamp, and try TX). at one point in time, we stop as
        //  latestTimeStamp is out dated. or we successfully apply the latest data.
    }
}

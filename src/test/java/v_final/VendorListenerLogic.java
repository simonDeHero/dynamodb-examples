package v_final;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBSaveExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ConditionalOperator;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class VendorListenerLogic {

    private final AmazonDynamoDB client;
    private final DynamoDBMapperConfig mapperConfigConsistent;
    private final DynamoDBMapper mapper;

    public VendorListenerLogic(AmazonDynamoDB client) {
        this.client = client;
        mapperConfigConsistent = DynamoDBMapperConfig.builder()
                .withConsistentReads(DynamoDBMapperConfig.ConsistentReads.CONSISTENT)
                .build();
        mapper = new DynamoDBMapper(client);
    }

    // these vendors are actually not of type `Vendor`, but the type from the queue message!
    public void update(Set<Vendor> latestVendorsForRvid, String rVID, Instant eventTimeStamp) {

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

        handleUpdates(existingVendorsToUpdate, latestVendorsForRvid, eventTimeStamp, rVID);

        // these vendors to add are actually not of type `Vendor`, but the type from the queue message!
        handleAddOrDelete(queueMessageVendorsToAdd, eventTimeStamp, rVID, "false");
        handleAddOrDelete(existingVendorsToDelete, eventTimeStamp, rVID, "true");
    }

    private void handleUpdates(Set<Vendor> existingVendorsToUpdate, Set<Vendor> latestVendorsForRvid,
                               Instant eventTimeStamp, String rVID) {

        String ets = String.valueOf(eventTimeStamp.toEpochMilli());

        DynamoDBSaveExpression saveExpression = new DynamoDBSaveExpression()
                .withExpected(Map.of("ts", new ExpectedAttributeValue(new AttributeValue().withN(ets))
                        .withComparisonOperator(ComparisonOperator.LT)));

        for (Vendor vendor : existingVendorsToUpdate) {

            Vendor latestVendor = latestVendorsForRvid.stream()
                    .filter(v -> vendor.getHashKey().equals(v.getHashKey())).findFirst().get();

            try {
                vendor.setRpsId(rVID);
                vendor.setDeleted("false");
                vendor.setTimestamp(eventTimeStamp);
                vendor.setConfig(latestVendor.getConfig());

                // if not existing, insert. if existing, update.
                mapper.save(vendor, saveExpression);

                System.out.println(Thread.currentThread().getName() + " : successful update");

            } catch (ConditionalCheckFailedException e) {
                // queue message outdated, skip
//                System.out.println(Thread.currentThread().getName()
//                        + " : update failed, latest vendor older than existing one");
            }
        }
    }

    /*
    the vendors are found by rVID, but they are updated by hashkey (pVID, gK). so if we get a exception, due to outdated,
    we just skip. otherwise the platform vendor is added (if not existing) or updated (if existing) according to the
    given info, which must be correct, as it is newer.
     */
    private void handleAddOrDelete(Set<Vendor> vendorsToModify, Instant eventTimeStamp, String rVID, String deleted) {

        String ets = String.valueOf(eventTimeStamp.toEpochMilli());

        DynamoDBSaveExpression saveExpression = new DynamoDBSaveExpression()
                .withExpected(Map.of(
                        // if not existing yet
                        "pVIDgK", new ExpectedAttributeValue(false),
                        // OR new time stamp is newer
                        "ts", new ExpectedAttributeValue(
                                // type of attribute value should be "number" (although it is given as String)
                                new AttributeValue().withN(ets)).withComparisonOperator(ComparisonOperator.LT)
                ))
                .withConditionalOperator(ConditionalOperator.OR);

        for (Vendor vendor : vendorsToModify) {

            try {
                vendor.setRpsId(rVID);
                vendor.setDeleted(deleted);
                vendor.setTimestamp(eventTimeStamp);

                // if not existing, insert. if existing, update.
                mapper.save(vendor, saveExpression);

                System.out.println(Thread.currentThread().getName() + " : successful add/delete");

            } catch (ConditionalCheckFailedException e) {
                // queue message outdated, skip
//                System.out.println(Thread.currentThread().getName()
//                        + " : add/delete failed, latest vendor older than existing one");
            }
        }
    }
}

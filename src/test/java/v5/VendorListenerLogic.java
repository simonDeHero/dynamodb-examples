package v5;

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

    /*
     * problem: how to find out, which ones in our table have been deleted (and thus are not existing in the given set)?
     * -> delete all in table and insert the set
     *
     * what about "duplicate" messages? same payload, but only millis apart. maybe even different payload?
     *
     *
     * go through the set 1 by 1 and use optimistic locking with @DynamoDBVersioned
     * load all from table by rVID (strong consistency)
     * - for each one, which is in table, but not in set: if set-timestamp > table-timestamp, delete. (use conditional expression, as timestamp could have been updated in meantime)
     *      - maybe already deleted -> skip
     *      - ?
     * - for each one, which is not in table, but in set: if set-timestamp > all timestamps of rVID restos, or no rVID resto existing, add.
     *      - maybe already existing -> reload and maybe update timestamp to set-timestamp, if newer
     *      - versioning helps?
     * - for each one, which is in both: if set-timestamp > table-timestamp, update timestamp
     *      - maybe already deleted?
     *      - then add?
     *
     * in TX
     * - only write (PUT, UPDATE, DELETE) or read (GET) - no mix
     */
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

        // we could leave deleted rows in there, then we have the timestamp.
        /*
         but then this could happen:
         - there exists a deleted row for rps id "a" with ("1", GK). a message comes in for rps id "b" where this is added.
           no versioning, so we just save ("last is winner") the row ("1", GK) with rps-id b as not deleted.
         - scenario: it gets "deleted", although it isn't
           it exists for rps id "a", message comes in without it. in the meantime it gets concurrently deleted and added to rps id "b".
           the algorithm deletes it, as it only uses the ("1", GK) -> we can add the condition to it, that rps vendor id is the same as expected
         */
//        handleDeletions_versioning(existingVendorsToDelete, latestTimeStamp);
//        handleAdditions_versioning(queueMessageVendorsToAdd, latestTimeStamp);
//        handleUpdates_versioning(existingVendorsToUpdate, latestTimeStamp);

        // or without versioning (remove the version annotation from Vendor)
        // those do not really work
//        handleDeletions_conditional(existingVendorsToDelete, latestTimeStamp);
//        handleAdditions_conditional(queueMessageVendorsToAdd, latestTimeStamp);
//        handleUpdates_conditional(existingVendorsToUpdate, latestTimeStamp);

        // no versioning used, only with conditional expression based on latestTimeStamp
        handleDeletions(existingVendorsToDelete, latestTimeStamp, rVID);
        handleAdditions(queueMessageVendorsToAdd, latestTimeStamp);
        handleUpdates(existingVendorsToUpdate, latestTimeStamp);
    }

    private void handleDeletions(Set<Vendor> existingVendorsToDelete, Instant latestTimeStamp, String rVID) {

        // TODO actually in order to compare, this should be a int, i think
        String latestTs = latestTimeStamp.toString();

        DynamoDBSaveExpression saveExpression = new DynamoDBSaveExpression()
                .withExpected(Map.of(
                        // if older than latestTimeStamp
                        "ts", new ExpectedAttributeValue(new AttributeValue(latestTs)).withComparisonOperator(ComparisonOperator.LT),
                        // AND still attached to rVID
                        "rVID", new ExpectedAttributeValue(new AttributeValue(rVID)).withComparisonOperator(ComparisonOperator.EQ)))
                .withConditionalOperator(ConditionalOperator.AND);

        for (Vendor vendorToDelete : existingVendorsToDelete) {

            boolean shouldTryAgain = true;
            while (shouldTryAgain) {
                try {
                    vendorToDelete.setDeleted("true");
                    vendorToDelete.setTimestamp(latestTimeStamp);
                    vendorToDelete.setRpsId(rVID);
                    mapper.save(vendorToDelete, saveExpression);
                    shouldTryAgain = false;

                } catch (ConditionalCheckFailedException e) {
                    // saveExpression not met. test!
                    vendorToDelete = mapper.load(vendorToDelete, mapperConfigConsistent);
                    // should not be null, as we don't really delete
                    if (vendorToDelete.getTimestamp().isAfter(latestTimeStamp)) {
                        shouldTryAgain = false;
                    }
                }
            }
        }
    }

    private void handleAdditions(Set<Vendor> queueMessageVendorsToAdd, Instant latestTimeStamp) {

        // TODO actually in order to compare, this should be a int, i think
        String latestTs = latestTimeStamp.toString();

        DynamoDBSaveExpression saveExpression = new DynamoDBSaveExpression()
                .withExpected(Map.of(
                        // "ts < :latestTs"
                        "ts", new ExpectedAttributeValue(new AttributeValue(latestTs)).withComparisonOperator(ComparisonOperator.LT)
                ))
                .withConditionalOperator(ConditionalOperator.OR);

        // those vendors will actually come from the queue and so are not versioned yet.
        // - if such a platform vendor doesn't exist, add it.
        // - if it exists for an other rVID, overwrite, only if the latestTimeStamp is newer. versioning is not interfering, as we don't have a loaded existing vendor.
        //   due to the overwriting, it will be set to "not deleted" if it currently is.
        for (Vendor vendorToAdd : queueMessageVendorsToAdd) {

            boolean shouldTryAgain = true;
            while (shouldTryAgain) {
                try {
                    vendorToAdd.setTimestamp(latestTimeStamp);
                    mapper.save(vendorToAdd, saveExpression);
                    shouldTryAgain = false;

                } catch (ConditionalCheckFailedException e) {
                    // saveExpression not met? test!
                    Vendor existingVendor = mapper.load(vendorToAdd, mapperConfigConsistent);
                    // should not be null, as we don't really delete

                    // TODO not finished
                    if (existingVendor.getTimestamp().isAfter(latestTimeStamp)) { // existing vendor is newer, so addition is outdated
                        shouldTryAgain = false;
                    } else {
                        vendorToAdd = existingVendor; // to have current version and be able to "save" without version conflict
                        vendorToAdd.setTimestamp(latestTimeStamp);
                    }
                }
            }
        }
    }

    private void handleUpdates_conditional(Set<Vendor> existingVendorsToUpdate, Instant latestTimeStamp) {
        // update if timestamp of existing vendor is older. add if not existing
        // TODO ??? but what if it was actually deleted due to a more up-to-date queue message? then it should not be added
    }

    private void handleDeletions_versioning(Set<Vendor> existingVendorsToDelete, Instant latestTimestamp) {

        for (Vendor vendorToDelete : existingVendorsToDelete) {

            // queue message is outdated
            if (vendorToDelete.getTimestamp().isAfter(latestTimestamp)) {
                continue;
            }

            boolean shouldTryAgain = true;
            while (shouldTryAgain) {
                try {
                    mapper.delete(vendorToDelete);
                    shouldTryAgain = false;
                } catch (ConditionalCheckFailedException e) {
                    // optimistic locking case, stale vendor
                    vendorToDelete = mapper.load(vendorToDelete, mapperConfigConsistent);

                    if (vendorToDelete == null // vendor was already deleted somehow, nothing to do
                            || vendorToDelete.getTimestamp().isAfter(latestTimestamp) // existing vendor is newer, so deletion is outdated
                    ) {
                        shouldTryAgain = false;
                    }
                }
            }
        }
    }

    private void handleAdditions_versioning(Set<Vendor> queueMessageVendorsToAdd, Instant latestTimestamp) {

        for (Vendor vendorToAdd : queueMessageVendorsToAdd) {

            vendorToAdd.setTimestamp(latestTimestamp);

            boolean shouldTryAgain = true;
            while (shouldTryAgain) {
                try {
                    mapper.save(vendorToAdd);
                    shouldTryAgain = false;
                } catch (ConditionalCheckFailedException e) {
                    // optimistic locking case, vendor already exists
                    Vendor existingVendor = mapper.load(vendorToAdd, mapperConfigConsistent);

                    if (existingVendor == null) {
                        // has been deleted before the "load". hm, what is more recent? the deletion or the add?
                        // cannot say, as i don't have timestamp of deletion
                        // TODO what to do?

                    } else {
                        if (existingVendor.getTimestamp().isAfter(latestTimestamp)) { // existing vendor is newer, so addition is outdated
                            shouldTryAgain = false;
                        } else {
                            vendorToAdd = existingVendor; // to have current version and be able to "save" without version conflict
                            vendorToAdd.setTimestamp(latestTimestamp);
                        }
                    }
                }
            }
        }
    }

    private void handleUpdates(Set<Vendor> existingVendorsToUpdate, Instant latestTimestamp) {

        for (Vendor vendorToUpdate : existingVendorsToUpdate) {

            vendorToUpdate.setTimestamp(latestTimestamp);

            boolean shouldTryAgain = true;
            while (shouldTryAgain) {
                try {
                    mapper.save(vendorToUpdate);
                    shouldTryAgain = false;
                } catch (ConditionalCheckFailedException e) {
                    // optimistic locking case, vendor was updated in meantime
                    vendorToUpdate = mapper.load(vendorToUpdate, mapperConfigConsistent);

                    if (vendorToUpdate == null) {
                        // has been deleted before the "load". hm, what is more recent? the deletion or the update?
                        // cannot say, as i don't have timestamp of deletion
                        // TODO what to do?

                    } else {
                        if (vendorToUpdate.getTimestamp().isAfter(latestTimestamp)) { // existing vendor is newer, so udpate is outdated
                            shouldTryAgain = false;
                        }
                    }
                }
            }
        }
    }
}

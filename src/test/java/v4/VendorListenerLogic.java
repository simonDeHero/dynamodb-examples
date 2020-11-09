package v4;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;

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
    public void update(Set<Vendor> latestVendorsForRvid, String rVID, Instant timestamp) {

        DynamoDBQueryExpression<Vendor> gsiQuery = new DynamoDBQueryExpression<Vendor>()
                .withIndexName("rVIDGsi")
                .withConsistentRead(false)
                .withKeyConditionExpression("rVID = :rVID")
                .withExpressionAttributeValues(Map.of(":rVID", new AttributeValue(rVID)));
        PaginatedQueryList<Vendor> existingVendors = mapper.query(Vendor.class, gsiQuery, mapperConfigConsistent);
        existingVendors.loadAllResults();

        // all which are existing in table, but not in set
        Set<Vendor> vendorsToDelete = existingVendors.stream()
                .filter(existingVendor -> latestVendorsForRvid.stream()
                        .noneMatch(latestVendor -> latestVendor.getHashKey().equals(existingVendor.getHashKey())))
                .collect(Collectors.toSet());

        // all which are existing in set, but not in table
        Set<Vendor> vendorsToAdd = latestVendorsForRvid.stream()
                .filter(latestVendor -> existingVendors.stream()
                        .noneMatch(existingVendor -> latestVendor.getHashKey().equals(existingVendor.getHashKey())))
                .collect(Collectors.toSet());

        // all which are existing in both
        Set<Vendor> vendorsToUpdate = existingVendors.stream()
                .filter(existingVendor -> latestVendorsForRvid.stream()
                        .anyMatch(latestVendor -> latestVendor.getHashKey().equals(existingVendor.getHashKey())))
                .collect(Collectors.toSet());

        handleDeletions(vendorsToDelete, timestamp);
        handleUpdates(vendorsToUpdate, timestamp);
        handleAdditions(vendorsToAdd, timestamp);
    }

    private void handleAdditions(Set<Vendor> vendorsToAdd, Instant latestTimestamp) {

    }

    private void handleUpdates(Set<Vendor> vendorsToUpdate, Instant latestTimestamp) {

    }

    private void handleDeletions(Set<Vendor> vendorsToDelete, Instant latestTimestamp) {

        for (Vendor vendor : vendorsToDelete) {

            boolean shouldTryAgain = true;
            while (shouldTryAgain) {
                try {
                    mapper.delete(vendor);
                    shouldTryAgain = false;
                } catch (ConditionalCheckFailedException e) {
                    // optimistic locking case, stale vendor
                    vendor = mapper.load(vendor, mapperConfigConsistent);
                    if (vendor == null // vendor was already deleted somehow, nothing to do
                            || vendor.getTimestamp().isAfter(latestTimestamp) // existing vendor is newer, so deletion is outdated
                    ) {
                        shouldTryAgain = false;
                    }
                }
            }
        }
    }
}

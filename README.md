# Some DynamoDB examples

* v1
  * start the localstack docker image with DynamoDB before the test
  * first try: create table, use global secondary index, annotate the order entity
  * crud and read by GSI
  * no error on creating duplicate orders
* v2
  * in-memory DynamoDB (run mvn test-compile once to create the `native-libs` folder containing the so and dll files 
  for sqlite used by DynamoDB)
  * error on creating duplicate orders
* v3
  * primary key of GSI does not need to be unique, so we have to assert uniqueness of (pOID, global-key) differently
* v4
  * use 2 order tables to assert uniqueness of (pOID, gEID) and (pOID, gK)
  * use insert with TX to throw error on duplicates
  * started with vendor example: use optimistic locking with versioning when applying changes via listener
    * handle "duplicate" orders for same rps vendor id at the same time!
    * don't use big TX for "read all vendors for rpsId, delete them and add again according to message" but handle each
    entry on its own (using optimistic locking and the existing and provided timestamps)
    * not finished yet (don't even know, if the approach works)
* v5
  * played around with different approaches how to update the vendors in the listener via queue message
  * versioning, conditional expressions
  * but each platform vendor updated singularly
  * does not look promising
* v6
  * TX approach for updating the vendors
  * all-or-nothing with retries
  * TX can fail due to concurrent modification or platform vendor already exists (due to duplicate message or for other 
  rps vendor id) or queue message timestamp outdated
  * looks more promising, but still thinking to do
* v_final
  * the final solutions, after talk to colleagues with experience in DynamoDB
  * orders in 2 tables: not nice and a little more expensive, but ok, as we don't have to adapt OPA business logic and
  we can just throw away 1 table without further DB adaptions, when we drop the global-key
  * vendors
    * don't use a TX, as it supports only 25 items. use some kind of versioning. also keep deleted vendors in
      order to know their latest change timestamp. if a queue message has a newer "version", just apply (insert or update)
    * (concurrent) tests with conditional expressions based on timestamps, as a kind of versioning/optimistic locking
* in general
  * no distinction between eventual and strong consistency reads IMO, as we only have 1 DynamoDB node (i "think") and so all reads are
  always strong consistent (eventual consistent would be, if we would read from slave, which may not have the latest data yet)

# Costs (eu01-prd01-opa)
* https://calculator.s3.amazonaws.com/index.html
* 370K orders per day -> 11 millions per month
* ~300 relation changes in last 2 days -> 150 per day -> 4500 per month
* db.r5.large, 30GB storage
## Aurora
**~ 1 instance ~240$ -> reader+writer ~480$**
## DynamoDB
* let's say 10GB storage
* on-demand/pay-per-request
* per order
  * 2 item writes in TX (each item <= 1KB)
  * 1 vendor reader (eventually consistent)
  * ~ 22 mio writes in TX + 11 mio reads (EC)
  * 120$
* vendor relation
  * calculator only allows millions, so let's say 1 million writes per month (NOT in TX)
  * **125$**

# Costs (as01-prd01-tw-opa)
* https://calculator.s3.amazonaws.com/index.html
* 600K orders per day -> 18 millions per month
* ~20K relation changes in last 2 days -> 10K per day -> 300K per month
* db.r5.large, 30GB storage
## Aurora
**~ reader+writer ~1000$**
## DynamoDB
* let's say 20GB storage
* on-demand/pay-per-request
* per order
  * 2 item writes in TX (each item <= 1KB)
  * 1 vendor reader (eventually consistent)
  * ~ 36 mio writes in TX + 18 mio reads (EC)
  * 290$
* vendor relation
  * calculator only allows millions, so let's say 1 million writes per month (NOT in TX)
  * **295$**
  
# Costs for OMA (eu01)

## Aurora
- db.r5.xlarge, 30GB, reader+writer, 100 mio requests -> ~1000$

## Dynamo
(for 1 resto OD order)
1. accept order from opa queue (write)
2. create in hurrier (read + write)
3. rider assigned (read + write)
4. ack from vendor (read + write)
5. accept from vendor (read + write)
6. picked up (r + w)
7. delivered (r + w)
additionally: 1 prepared, 1 near vendor, 1 courier left vendor, 1 near customer, 1 delayed ? (r+w)

+ for each change: 
	- 1 read for platform notification
	- 1 read for fridge notification

------------ (without retries due to opt lock)

12 reads, 13 writes

------------ (11 mio orders per month)

~130 mio reads, ~140 mio writes

reads must happen in TX. listener triggered by action dispatcher expect, that the data has been persisted!

+ polling from clients
	- newrelic: 6800 RPM * 43800 = 293760000 (300 mio)

+ DB debugging calls
	- number does not matter
	- which fields are queried?

+ item size ~ 5KB ?

====> 2500 $
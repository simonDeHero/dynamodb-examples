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

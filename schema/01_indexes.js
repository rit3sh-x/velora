const dbName = "velora";
const collName = "raw_tweets";

const target = db.getSiblingDB(dbName);

const existing = target.getCollectionNames();
if (!existing.includes(collName)) {
    target.createCollection(collName);
}

const coll = target.getCollection(collName);

coll.createIndex({ tweet_id: 1, source: 1 }, { unique: true, name: "uniq_tweet_source" });

coll.createIndex({ coin: 1, ts: -1 }, { name: "coin_ts_desc" });

coll.createIndex({ coin: 1, source: 1, ts: -1 }, { name: "coin_source_ts_desc" });

coll.createIndex({ ts: 1 }, { name: "ts_ttl", expireAfterSeconds: 2592000 });

print("velora.raw_tweets indexes ready: " + coll.getIndexes().map(i => i.name).join(", "));

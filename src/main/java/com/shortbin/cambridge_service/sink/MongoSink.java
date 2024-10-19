package com.shortbin.cambridge_service.sink;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.shortbin.cambridge_service.model.ClickAggregate;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MongoSink extends RichSinkFunction<ClickAggregate> {
    private final String uri;
    private final String databaseName;
    private final String collectionName;
    private transient MongoClient mongoClient;
    private transient MongoCollection<Document> collection;

    public MongoSink(String uri, String databaseName, String collectionName) {
        this.uri = uri;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        mongoClient = MongoClients.create(uri);
        collection = mongoClient.getDatabase(databaseName).getCollection(collectionName);
    }

    @Override
    public void invoke(ClickAggregate clickAggregate, Context context) {

        List<Bson> updates = new ArrayList<>();

        updates.add(Updates.inc("clicks", clickAggregate.getCount()));

        for (Map.Entry<String, Integer> entry : clickAggregate.getCountryCounter().entrySet()) {
            updates.add(Updates.inc("countries." + entry.getKey(), entry.getValue()));
        }

        for (Map.Entry<String, Integer> entry : clickAggregate.getRefererCounter().entrySet()) {
            updates.add(Updates.inc("referers." + entry.getKey(), entry.getValue()));
        }

        UpdateOneModel<Document> updateModel = new UpdateOneModel<>(
                Filters.eq("_id", clickAggregate.getShortId()),
                Updates.combine(updates),
                new UpdateOptions().upsert(true)
        );

        collection.bulkWrite(List.of(updateModel));
    }

    @Override
    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}

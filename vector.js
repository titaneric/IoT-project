conn = new Mongo();
db = conn.getDB("log");
// Define stage to add convertedDate field with the converted order_date value

dateConversionStage = {
    $addFields: {
        convertedDate: { $toDate: "$window" }
    }
};

// Match window at certain range
var after = new ISODate()
var before = new Date(after.getTime() - 1000 * 60 * 2)

matchStage = {
    $match: {
        convertedDate: { $gte: before, $lt: after }
    }
}

groupByAddressStage = {
    $group: {
        _id: "$nctu_address",
        "agg": { $push: "$appearance" }
    }
}

// Define stage to sort documents by the converted date


cursor = db.traffic_windowed_appearance.aggregate([
    dateConversionStage,
    matchStage,
    groupByAddressStage,
])

// cursor = db.traffic_windowed_appearance.find();
while (cursor.hasNext()) {
    printjson(cursor.next());
}
conn = new Mongo();
db = conn.getDB("log");
// Define stage to add convertedDate field with the converted order_date value

dateConversionStage = {
    $addFields: {
        convertedDate: { $toDate: "$window" }
    }
};

// Match window at certain range
var delta = 10;
var end = new ISODate()
var begin = new Date(end.getTime() - 1000 * 60 * delta)

matchStage = {
    $match: {
        convertedDate: { $gte: begin, $lt: end }
    }
}

groupByAddressStage = {
    $group: {
        _id: {
            addr: "$nctu_address",
            date: "$convertedDate",
        },
        "agg": {
            $push: {
                appr: "$appearance",
                bytes_received: "$bytes_received",
                bytes_sent: "$bytes_sent",
            }
        },
    }
}

maximumProjectStage = {
    $project: {
        appr: { $max: "$agg.appr" }
    }
}

featureListStage = {
    $group: {
        _id: "$_id.addr",
        list: {
            $push: {
                appr: "$appr",
                date: "$_id.date",
            }
        }
    }
}

// source from https://stackoverflow.com/questions/39351245/sort-data-in-mongodb-on-the-basis-of-date-embedded-in-array
sortStage = [
    {
        $unwind: { path: "$list" }
    },
    {
        $sort: {
            "list.date": 1
        }
    },
    {
        $group: {
            _id: "$_id",
            list: {
                $push: {
                    appr: "$list.appr",
                    date: "$list.date",
                }
            }
        }
    }
]

cursor = db.traffic_windowed_appearance.aggregate([
    dateConversionStage,
    matchStage,
    groupByAddressStage,
    maximumProjectStage,
    featureListStage,
    ...sortStage
])

// cursor = cursor.forEach(
//     function(log){
//         var it = start;
//         while (it < end) {

//         }

//     }
// )


while (cursor.hasNext()) {
    printjson(cursor.next());
}
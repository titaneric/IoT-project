conn = new Mongo();
db = conn.getDB("log");
printjson(db.threat_severity_vector.findOne({_id: ObjectId("5efa646c262fb33219f55162")}))
const mongo = require('/mongo.js')

async function start () {
  let db = mongo.getDB()

  let bulk = db.channel_info.initializeOrderedBulkOp()
  let counter = 0
  db.channel_info.find().forEach(data => {
    bulk.find({
      "_id": data._id
    }).update({"$set": {
      "info.statistics.viewCount": parseInt(data.info.statistics.viewCount),
      "info.statistics.subscriberCount": parseInt(data.info.statistics.subscriberCount),
      "info.statistics.videoCount": parseInt(data.info.statistics.videoCount)
    }})

    counter++
    if (counter % 1000 == 0) {
      bulk.execute()
      bulk = db.channel_info.initializeOrderedBulkOp()
    }
  })
  // Add the rest in the queue
  if (counter % 1000 != 0) bulk.execute()
}

start()

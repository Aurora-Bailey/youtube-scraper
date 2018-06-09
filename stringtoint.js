const mongo = require('./mongo.js')

async function start () {
  let db = await mongo.getDB()

  let bulk = db.collection('channel_info').initializeOrderedBulkOp()
  let counter = 0
  db.collection('channel_info').find().forEach(data => {
    bulk.find({
      "_id": data._id
    }).update({"$set": {
      "info.statistics.viewCount": parseInt(data.info.statistics.viewCount),
      "info.statistics.subscriberCount": parseInt(data.info.statistics.subscriberCount),
      "info.statistics.videoCount": parseInt(data.info.statistics.videoCount)
    }})

    counter++
    if (counter % 1000 == 0) {
      console.log('execute ', counter)
      bulk.execute()
      bulk = db.collection('channel_info').initializeOrderedBulkOp()
    }
  })
  // Add the rest in the queue
  if (counter % 1000 != 0) bulk.execute()
}

start()

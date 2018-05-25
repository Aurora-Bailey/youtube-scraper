const {MongoClient} = require('mongodb')

class Mongo {
  constructor() {
    this._url = 'mongodb://localhost:27017'
    this._dbName = 'youtube-scraper'
    this._db = false
  }

  getDB () {
    return new Promise((resolve, reject) => {
      if (this._db) {
        resolve(this._db)
      } else {
        MongoClient.connect(this._url, { useNewUrlParser: true }).then(client => {
          this._db = client.db(this._dbName)
          resolve(this._db)
        }).catch(reject)
      }
    })
  }
}

module.exports = new Mongo()

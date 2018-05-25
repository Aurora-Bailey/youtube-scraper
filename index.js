const {google} = require('googleapis')
const credentials = require('./credentials.json')
const mongo = require('./mongo')
const youtube = google.youtube({
  version: 'v3',
  auth: credentials.youtube
})

class Crawler {
  constructor (options) {
    // options
    this.youtube = google.youtube({ version: 'v3', auth: options.apiKey })

    // setup
    this.start = Date.now()
    this.usedQueries = 0
    this.dailyQuota = 2e5 // 200k

    // internal config
    this.capSubsAt = 500
  }

  run () {
    this.next()
  }

  async mongoGetNextChannel () {
    let db = await mongo.getDB()
    let response = await db.collection('crawled').findOneAndUpdate({crawled: false}, {$set: {crawled: true}})
    let channelId = response.value.channelId
    if (typeof channelId === 'undefined') throw "channelId empty"
    return channelId
  }

  async mongoSetNewChannels (channelIdList) {
    let db = await mongo.getDB()
    channelIdList.forEach(channelId => {
      db.collection('crawled').insertOne({channelId, crawled: false}).catch(console.error) // duplicates will throw an error
    })
  }

  async mongoSetChannelSubscriptions (channelId, subscriptions, error) {
    let db = await mongo.getDB()
    db.collection('channel_subscriptions').insertOne({channelId, subscriptions, error})
  }

  async next () {
    let channelId = await this.mongoGetNextChannel().catch(err => { console.error(err); process.exit(0) }) // fatal errror
    try {
      let subscriptions = await this.getChannelSubscriptions(channelId)
      this.mongoSetNewChannels(subscriptions).catch(err => { console.error(err)})
      this.mongoSetChannelSubscriptions(channelId, subscriptions, false).catch(err => { console.error(err)})
      console.log(channelId)
    } catch (err) {
      this.mongoSetChannelSubscriptions(channelId, [], err.code).catch(err => { console.error(err)})
      console.log(err.code)
    } finally {
      setTimeout(() => { this.next() }, this.pace())
    }
  }

  pace () {
    let millisecondQuota = (((this.dailyQuota / 24) / 60) / 60) / 1000
    let timeRunning = Date.now() - this.start
    let expectedQueries = Math.floor(timeRunning * millisecondQuota)
    let overusedQueries = this.usedQueries - expectedQueries
    let waitMilliseconds = overusedQueries / millisecondQuota
    if (waitMilliseconds < 0) waitMilliseconds = 0
    return waitMilliseconds
  }

  async getChannelSubscriptions (channelId) {
    this.usedQueries++
    let res = await this.youtube.subscriptions.list({
      part: 'snippet',
      maxResults: 50,
      fields: 'items/snippet/resourceId/channelId,pageInfo,nextPageToken',
      channelId: channelId
    })
    let subscriptions = res.data.items.map(i => { return i.snippet.resourceId.channelId })
    let nextPage = res.data.nextPageToken || false
    let numSubscriptions = res.data.pageInfo.totalResults

    while (nextPage && subscriptions.length < this.capSubsAt) {
      this.usedQueries++
      let sub_res = await this.youtube.subscriptions.list({
        part: 'snippet',
        maxResults: 50,
        fields: 'items/snippet/resourceId/channelId,pageInfo,nextPageToken',
        pageToken: nextPage,
        channelId: channelId
      })
      subscriptions = subscriptions.concat(sub_res.data.items.map(i => { return i.snippet.resourceId.channelId }))
      nextPage = sub_res.data.nextPageToken || false
    }
    return subscriptions
  }

}

var crawler1 = new Crawler({
  apiKey: credentials.youtube
})
crawler1.run()

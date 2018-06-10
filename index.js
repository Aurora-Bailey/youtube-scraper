const {google} = require('googleapis')
const credentials = require('./credentials.json')
const mongo = require('./mongo')
const chalk = require('chalk')

class Crawler {
  constructor (options) {
    // options
    this.youtube = google.youtube({ version: 'v3', auth: options.apiKey })
    this.crawlerId = options.crawlerId

    // setup
    this.start = Date.now()
    this.queryCost = 3
    this.usedQueries = 0
    this.dailyQuota = 5e5 // 500k

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
      db.collection('crawled').insertOne({channelId, crawled: false}).catch(() => {}) // duplicates will throw an error
    })
  }

  async mongoSetChannelSubscriptions (channelId, subscriptions, error) {
    let db = await mongo.getDB()
    db.collection('channel_subscriptions').insertOne({channelId, subscriptions, error})
  }

  async next () {
    let pullStartTime = Date.now()
    let channelId = await this.mongoGetNextChannel().catch(err => { console.error(err); process.exit(0) }) // fatal errror
    let pullChannelTime = Date.now()
    try {
      let subscriptions = await this.getChannelSubscriptions(channelId)
      this.mongoSetNewChannels(subscriptions).catch(err => { console.error(err)})
      this.mongoSetChannelSubscriptions(channelId, subscriptions, false).catch(err => { console.error(err)})
      let pullDataTime = Date.now()
      let channelTime = '' + (pullChannelTime - pullStartTime)
      let dataTime = '' + (pullDataTime - pullChannelTime)
      console.log(`${this.crawlerId} db:${channelTime} yt:${dataTime}`)
      // console.log(`${chalk.bgGreen(this.crawlerId + ' ' + channelId)} mongodb:${channelTime > 1000 ? chalk.red(channelTime) : channelTime}ms youtube-aip: ${dataTime > 1000 ? chalk.cyan(dataTime) : dataTime}ms`)
    } catch (err) {
      this.mongoSetChannelSubscriptions(channelId, [], err.code).catch(err => { console.error(err)})
      let queryTime = '' + (Date.now() - pullStartTime)
      console.log(chalk.red(`${this.crawlerId} err:${err.code} t:${queryTime}`))
      // console.log(`${chalk.bgRedBright(this.crawlerId + ' ' + err.code)} query-time: ${queryTime > 1000 ? chalk.cyan(queryTime) : queryTime}ms`)
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
    this.usedQueries += this.queryCost
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
      this.usedQueries += this.queryCost
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

setTimeout(() => { let crawler = new Crawler({ crawlerId: 1, apiKey: credentials["youtube-scraper-1"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 2, apiKey: credentials["youtube-scraper-2"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 3, apiKey: credentials["youtube-scraper-3"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 4, apiKey: credentials["youtube-scraper-4"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 5, apiKey: credentials["youtube-scraper-5"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 6, apiKey: credentials["youtube-scraper-6"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 7, apiKey: credentials["youtube-scraper-7"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 8, apiKey: credentials["youtube-scraper-8"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 9, apiKey: credentials["youtube-scraper-9"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 10, apiKey: credentials["youtube-scraper-10"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 11, apiKey: credentials["youtube-scraper-11"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 12, apiKey: credentials["youtube-scraper-12"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 13, apiKey: credentials["youtube-scraper-13"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 14, apiKey: credentials["youtube-scraper-14"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 15, apiKey: credentials["youtube-scraper-15"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 16, apiKey: credentials["youtube-scraper-16"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 17, apiKey: credentials["youtube-scraper-17"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 18, apiKey: credentials["youtube-scraper-18"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 19, apiKey: credentials["youtube-scraper-19"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 20, apiKey: credentials["youtube-scraper-20"] }); crawler.run() }, Math.random() * 5000)

setTimeout(() => { let crawler = new Crawler({ crawlerId: 101, apiKey: credentials["youtube-scraper-1"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 102, apiKey: credentials["youtube-scraper-2"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 103, apiKey: credentials["youtube-scraper-3"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 104, apiKey: credentials["youtube-scraper-4"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 105, apiKey: credentials["youtube-scraper-5"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 106, apiKey: credentials["youtube-scraper-6"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 107, apiKey: credentials["youtube-scraper-7"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 108, apiKey: credentials["youtube-scraper-8"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 109, apiKey: credentials["youtube-scraper-9"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 1010, apiKey: credentials["youtube-scraper-10"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 1011, apiKey: credentials["youtube-scraper-11"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 1012, apiKey: credentials["youtube-scraper-12"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 1013, apiKey: credentials["youtube-scraper-13"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 1014, apiKey: credentials["youtube-scraper-14"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 1015, apiKey: credentials["youtube-scraper-15"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 1016, apiKey: credentials["youtube-scraper-16"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 1017, apiKey: credentials["youtube-scraper-17"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 1018, apiKey: credentials["youtube-scraper-18"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 1019, apiKey: credentials["youtube-scraper-19"] }); crawler.run() }, Math.random() * 5000)
setTimeout(() => { let crawler = new Crawler({ crawlerId: 1020, apiKey: credentials["youtube-scraper-20"] }); crawler.run() }, Math.random() * 5000)

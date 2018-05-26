### Youtube scraper

``` bash
mongodb
$ sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv EA312927; echo "deb http://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.2 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-3.2.list; sudo apt-get update; sudo apt-get install -y mongodb-org; sudo systemctl start mongod; sudo systemctl enable mongod;

node
$ curl -sL https://deb.nodesource.com/setup_10.x | sudo -E bash -; sudo apt-get install -y nodejs; sudo apt-get install -y build-essential;

git
$ sudo apt-get install git; git clone https://github.com/gbradthompson/youtube-scraper.git; cd "youtube-scraper";
```

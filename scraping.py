import sqlite3
import tweepy
from datetime import datetime

twitter_api = {
    'consumer_key':"JoPmGvLluEyLqyASsz49lE0fh",
    'consumer_secret':"sOcYXKBtnWYtIkzjiYS2tw9y7OdeX2qcP3SnZk8FYe5kzLz2ME",
    'access_token':"1287763964466610176-K64Z6DDuzE8zLWvH0vn5Q8Nz6d13b1",
    'access_token_secret':"ZLA10vZWo27ervw0DgCGLKpWsZ35GtEcmC9ploOqMbKdM"
}

query = 'korupsi'
database = 'database.db'
jumlah_tweet = 200

class DataHandler :
    
    scraping_id = 0
    # date_since = datetime.today().strftime('%Y-%m-%d')
    date_since = ""
    
    def __init__(self, database):
        self.tweet_container = []
        self.user_container = []
        self.database = database

    def save_sql(self):
        
        # cek status dan ganti status
        active = self.get_active()
        #print(active)
        if active != None and active[1] != DataHandler.date_since :
            self.swap_active()
        
        connection = sqlite3.connect(self.database)
        cursor = connection.cursor()
  
        # insert lastscrapping
        query = """INSERT INTO lastscraping VALUES (?, ?, ?);"""
        lastscrap = (DataHandler.scraping_id, DataHandler.date_since, 1)
        cursor.execute(query, lastscrap)
        connection.commit()
          
        # insert user
        query = """INSERT INTO user VALUES  (?, ?, ?, ?, ?, ?, ?, ?);"""

        for user in self.user_container:
            if(self.get_userid(user[0])) :
                cursor.execute(query,  user)
                connection.commit()
       
        # insert tweet
        query = """INSERT INTO tweet ("tweetid", "userid", "createddate", "tweet", "cleantweet", "scraping_id") VALUES (?, ?, ?, ?, ?, ?);"""
        for tweet in self.tweet_container:
            print(tweet)
            if(self.get_tweetid(tweet[0])) :
                cursor.execute(query,  tweet)
                connection.commit()
        
        print("\n\n")
        self.tweet_container = []
        self.user_container = []
        cursor.close()
        connection.close()

    def get_data(self, twitter_api, query, jumlah_tweet):
        consumer_key        = twitter_api['consumer_key']
        consumer_secret     = twitter_api['consumer_secret']
        access_token        = twitter_api['access_token']
        access_token_secret = twitter_api['access_token_secret']
        
        # autentikasi
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        api = tweepy.API(auth)
        
        # (since:2020-11-20  until:2020-11-21) (-filter:retweets)
        date = DataHandler.date_since.split("-")
        year = int(date[0])
        month = int(date[1])
        day = int(date[2])
        before = datetime(year, month, day, 00, 00, 00).strftime('%Y-%m-%d')
        after = datetime(year, month, day+1, 23, 59, 00).strftime('%Y-%m-%d')

        retweet_filter = '-filter:retweets'
        since_date = "since:" + before
        until_date = "until:" + after
        new_query = "(" + query + ") " + "(" + retweet_filter + ") " + "(" + since_date + " "+ until_date + ")"

        # mengambil tweet
        
        tweets = api.search(q=new_query, lang="id", count=jumlah_tweet, tweet_mode="extended")
        #print(tweets)
        # ambil data
        
        active = self.get_active()
        #print(active)
        if active != None :
            if active[1] == DataHandler.date_since :
                DataHandler.scraping_id = active[0]
            else :
                DataHandler.scraping_id = active[0] + 1
        else :
            DataHandler.scraping_id += 1
  
        for tweet in tweets:
            
            # tweet
            tweet_id = tweet.id
            user_id = tweet.user.id
            createddate = tweet.created_at
            tweet_text = tweet.full_text
            clean_data = self.cleaning(tweet_text)
            self.tweet_container.append((tweet_id, user_id, createddate, tweet_text, clean_data, DataHandler.scraping_id))
            
            #user
            user_name = tweet.user.name
            screenname = tweet.user.screen_name
            location = tweet.user.location
            acccreated = tweet.user.created_at
            follower = tweet.user.followers_count
            friend = tweet.user.friends_count
            verified = tweet.user.verified
            self.user_container.append((user_id, user_name, screenname, location, acccreated, follower, friend, verified))
        
    def cleaning(self, tw):
        import re
        import string
        from nltk.corpus import stopwords
        from nltk.tokenize import word_tokenize
        from Sastrawi.Stemmer.StemmerFactory import StemmerFactory as sf

        #mengubah ke huruf kecil
        res = tw.lower()
        #hapus angka
        res = re.sub(r"\d+", "", res)
        res = re.sub(r'\\u\w\w\w\w', '', res)
        res = re.sub(r'http\S+', '', res)
        #hapus @username
        res = re.sub('@[^\s]+', '', res)
        #hapus #tagger
        res = re.sub(r'#([^\s]+)', r'\1', res)
        #hapus tanda baca
        res = res.translate(str.maketrans("","",string.punctuation))
        #hapus whitespace
        res = res.strip()
        #menghilangkan stopword, melakukan tokenize, dan menggabungkan string
        sw = set(stopwords.words('indonesian'))
        wt = word_tokenize(res)
        res = " ".join([w for w in wt if not w in sw])
        #melakukan stemming dengan sastrawi
        factory = sf()
        st = factory.create_stemmer()
        output = st.stem(res)
        return output

    def get_scrapid(self):
        connection = sqlite3.connect(self.database)
        cursor = connection.cursor()
        
        # ambil data
        query = """SELECT scraping_id FROM lastscraping WHERE status = 1;"""
        cursor.execute(query)
        connection.commit()
        data = cursor.fetchone()
        cursor.close()
        connection.close()
        
        return data

    def get_active (self):
        connection = sqlite3.connect(self.database)
        cursor = connection.cursor()
        
        # ambil data
        query = """SELECT * FROM lastscraping WHERE status = 1;"""
        cursor.execute(query)
        connection.commit()
        data = cursor.fetchone()
        cursor.close()
        connection.close()
        
        return data

    def swap_active(self):
        connection = sqlite3.connect(self.database)
        cursor = connection.cursor()
        
        # ambil data
        query = """UPDATE lastscraping SET status = 0 WHERE status = 1;"""
        cursor.execute(query)
        connection.commit()
        cursor.close()
        connection.close()

    def get_userid(self, userid):
        connection = sqlite3.connect(self.database)
        cursor = connection.cursor()
        
        # ambil data
        query = "SELECT userid FROM user WHERE userid = " + str(userid)
        cursor.execute(query)
        connection.commit()
        data = cursor.fetchone()
        cursor.close()
        connection.close()
        
        if data == None :
            return True
        else:
            return False

    def get_tweetid(self, tweetid):
        connection = sqlite3.connect(self.database)
        cursor = connection.cursor()
        
        # ambil data
        query = "SELECT tweetid FROM tweet WHERE tweetid = " + str(tweetid)
        cursor.execute(query)
        connection.commit()
        data = cursor.fetchone()
        cursor.close()
        connection.close()
        
        if data == None :
            return True
        else:
            return False

    def delete_all_table(self):
        connection = sqlite3.connect(self.database)
        cursor = connection.cursor()
        
        # ambil data
        query = """DELETE FROM tweet;"""
        cursor.execute(query)
        connection.commit()
        query = """DELETE FROM user;"""
        cursor.execute(query)
        connection.commit()
        query = """DELETE FROM lastscraping;"""
        cursor.execute(query)
        connection.commit()
        cursor.close()
        connection.close()
    
        
data_handler = DataHandler(database)

DataHandler.date_since = "2020-11-25" #Hari Rabu
data_handler.get_data(twitter_api, query, jumlah_tweet)
data_handler.save_sql()
DataHandler.date_since = "2020-11-26" #Hari Kamis
data_handler.get_data(twitter_api, query, jumlah_tweet)
data_handler.save_sql()
DataHandler.date_since = "2020-11-27" #Hari Jumat
data_handler.get_data(twitter_api, query, jumlah_tweet)
data_handler.save_sql()

# data_handler.delete_all_table()
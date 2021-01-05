import sqlite3

class DataHandler:
    def __init__(self, db):
        self.database = db

    def get_rows(self):
        connection = sqlite3.connect(self.database)
        connection.row_factory = lambda cursor, row: row[0]
        c = connection.cursor()
            
        # ambil data
        query = """SELECT cleantweet FROM tweet;"""
        c.execute(query)
        connection.commit()
        data = c.fetchall()
        c.close()
        connection.close()

        return data

    def top_words(self, data):
        from nltk.tokenize import word_tokenize
        from nltk.probability import FreqDist
        import itertools
        import matplotlib.pyplot as plt
        from wordcloud import WordCloud

        res = [word_tokenize(p) for p in data]
        res = list(itertools.chain(*res))
        fqdist = FreqDist(res) 
        print(fqdist.most_common(20)) #mengetahui 10 kata terbanyak

        stopwords = ['yg', 'gak', 'nya', 'aja', 'ga', 'ya', 'dgn', 'sih', 'gitu', 'klo', 'utk', 'krn', 'dg', 'jd', 'dn', 'dlm', 'gk', 'dr', 'tdk', 'dg', 'tuh', 'amp', 'kalo', 'tau']
        res = ' '.join(data)
        wordcloud = WordCloud(width=1600,
                        height=800,
                        min_font_size=30,
                        stopwords=stopwords).generate(res)
        plt.figure(figsize=(12, 10))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis('off')
        plt.show()
        

database = 'database.db'
dt = DataHandler(database)
dt.top_words(dt.get_rows())
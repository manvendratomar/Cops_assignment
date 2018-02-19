
                        ############# Required Packages ##################
import tweepy,sys,json,datetime
from tweepy import OAuthHandler,Stream
from tweepy.streaming import StreamListener
from elasticsearch import Elasticsearch




class Tweet_retriever(StreamListener):
    """docstr for retrivieng the tweets as lead from twitter and index them into the elastic search """
    def __init__(self,es):
        self.es = es                                                            #initializing elasticsearch instance
# makeing index if not present and inserting lead into elastic server
    def make_index(self,tweet):
        if not self.es.indices.exists(index="twitter_index"):
            request_body = {
            "mappings": {
            "tweets": {
            "properties": {
              "created_at":{"type":"date", "format":"date_optional_time" },
              "location" :{"type":"geo_point"},
              "country":{"type":"text","fielddata":"true","fields": {"raw": {"type": "string","index": "not_analyzed"}},  "boost":2},
              "lead":{"type": "text",  "analyzer": "english",
                  "search_analyzer": "standard","boost":1},
              "author":{"type": "text","fielddata":"true","fields": {"raw": {"type": "string","index": "not_analyzed"}}, "boost":2},

              "categories":{"type":"text","fielddata":"true","fields": {"raw": {"type": "string","index": "not_analyzed"}}, "boost":2},
              "entities":{"type":"text","fielddata":"true","fields": {"raw": {"type": "string","index": "not_analyzed"}}, "boost":3}
                        }
                    }
                }
            }
            try:
                res = self.es.indices.create(index = "twitter_index", body = request_body)
            except Exception as e:
                print "error in making Index  "+str(e)
        try:
            dat = tweet["created_at"].split()
            dt = dat[-1]+"-"+dat[1]+"-"+dat[2]
            dt = datetime.datetime.strptime(dt, "%Y-%b-%d")
            dt = datetime.datetime.strftime(dt,"%Y-%m-%d")
            created = dt+"T"+dat[3]
            country = tweet["place"]
            lead = tweet["text"]
            author = tweet["user"]["name"]
            hash_t = []
            u_m=[]
            for en in tweet["entities"]["hashtags"]:
                 hash_t.append(en["text"])
            for en in tweet["entities"]["user_mentions"]:
                u_m.append(en["name"])
            request_body = {"created_at":created, "country":country, "lead":lead, "author":author, "categories":hash_t, "entities":u_m}
            self.es.index(index="twitter_index", doc_type="tweets",id=tweet["id"], body=request_body)
            print "Making index for tweets"

        except BaseException as e:
            print "ERROR in making index "+str(e)



#on getting tweet make_index call
    def on_data(self, data):
        try:
            # print data
            self.make_index(json.loads(data));
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

#catching errors
    def on_error(self, status):
        print(status)
        return True



class Query_Lead:
    """docstring for searching the query of holmes in leads stored in elastic search"""
    def __init__(self, es):
        self.es=es                                                  #initializing elasticsearch instance

#processing the result of search query
    def process_result(self, leads):
        count = 0
        for lead in leads['hits']['hits']:
            print "lead--->>   "+ str(lead["_source"]["lead"].encode("utf-8"))
            print "reporter--->>   "+ str((lead["_source"]["author"]).encode("utf-8"))
            print "categories--->>   "+ str(lead["_source"]["categories"])
            print "entities--->>   "+ str(lead["_source"]["entities"])
            print "created--->>   "+ str(lead["_source"]["created_at"])
            # print "entities--->>   "+ str(lead[""])
            count+=1;
            print "\n\n ############################################################\n\n"
            if(count>5):
                break
        if(count==0):
            print"Nothing Found"

#making query to the elastic server
    def make_query(self,query):
        leads = self.es.search(index = 'twitter_index', body = {"query":{"bool":{"should":[{"match":{'lead':{"query":query[0],"operator": "or","fuzziness":0 }}},
        {"match":{"author":{"query":query[0],"operator": "and","fuzziness":0 }}},
        {"match":{"categories":{"query":query[0],"operator": "or","fuzziness":0 }}}
        ,{"match":{"country":{"query":query[0],"operator": "or","fuzziness":0 }}},
        {"match":{"entities":{"query":query[0],"operator": "or","fuzziness":0 }}},
        {"geo_distance":{"distance":"12km","location":query[3] if(query[3]) else "40, -70" }}],
        "must":[{"range": { "created_at": {"gte" :(query[1] if(query[1]) else "now/d") ,"lte":(query[2] if(query[2]) else "now/d")}}}]}
        }})
        # print leads
        processed_data = self.process_result(leads)




class Analytics:
    """docstring for lead analytics  of holmes network"""


    def __init__(self,es):
        self.es = es                                                    #initializing elasticsearch instance

#printing average numer analytics
    def process_avg_analytics(self, data):
        for item in data["aggregations"]["leads"]["buckets"]:
            print "Average number of leads corresponding to key "+str(item["key"].encode("utf-8"))+"  is :::::::::------>>>>>"+str(item["aggs"]["value"])+"\n\n"


#processing the other analytics
    def process_analytics(self, data):
        for item in data["aggregations"]["top_tags"]["buckets"]:
            print "leads corresponding to key:::::::  "+str(item["key"].encode("utf-8"))
            print "doc_count corresponding to key:::::::  "+str(item["key"].encode("utf-8"))+":::::"+str(item["doc_count"])+"  \n"

            for lead in item["top_lead_hits"]["hits"]["hits"]:
                print str(lead["_source"]["lead"].encode("utf-8"))+"\n"
            print "\n\n###############\n\n"

#leads analysis by categories,entities,reporter respectively
    def get_category_analytics(self):
        leads_by_category = self.es.search(index = 'twitter_index', doc_type = "tweets",size=0, body = {"aggs": {"top_tags": {"terms": {"field": "categories.raw","size": 10},
            "aggs": {"top_lead_hits": {"top_hits":{"_source": {"includes": [ "lead", "categories.raw" ]},"size" : 10}}}}}})
        print "::::::::::::Analytics Lead by categories:::::::::\n\n"
        # print len(leads_by_category["aggregations"]["top_tags"]["buckets"])
        self.process_analytics(leads_by_category)

    def get_entities_analytics(self):
        leads_by_entities = self.es.search(index = 'twitter_index', doc_type = "tweets",size=0, body = {"aggs": {"top_tags": {"terms": {"field": "entities.raw","size": 10},
            "aggs": {"top_lead_hits": {"top_hits":{"_source": {"includes": [ "lead", "entities.raw" ]},"size" : 10}}}}}})
        print "::::::::::::Analytics Lead by entities:::::::::\n\n"
        self.process_analytics(leads_by_entities)

    def get_reporter_analytics(self):
        leads_by_reporter = self.es.search(index = 'twitter_index', doc_type = "tweets",size=0, body = {"aggs": {"top_tags": {"terms": {"field": "author.raw","size": 10},
            "aggs": {"top_lead_hits": {"top_hits":{"_source": {"includes": [ "lead", "author.raw" ]},"size" : 10}}}}}})
        print "::::::::::::Analytics Lead by reporter:::::::::\n\n"
        self.process_analytics(leads_by_reporter)

# Average lead analytics
    def get_avg_category_analytics(self):
        result = self.es.search(index = 'twitter_index', doc_type = "tweets",size=0, body = {"aggs":{"leads": {"terms": {"field": "categories.raw"},
            "aggs": { "leads_per_minute": {"date_histogram": {"field": "created_at","interval": "minute"}},"aggs": { "avg_bucket": {"buckets_path": "leads_per_minute._count"
            }}}}}})
        print "::::::::::::Analytics  avg number of Lead per category :::::::::\n\n"
        self.process_avg_analytics(result)

    def get_avg_entity_analytics(self):
        result = self.es.search(index = 'twitter_index', doc_type = "tweets",size=0, body = {"aggs":{"leads": {"terms": {"field": "entities.raw"},
            "aggs": { "leads_per_minute": {"date_histogram": {"field": "created_at","interval": "minute"}},"aggs": { "avg_bucket": {"buckets_path": "leads_per_minute._count"
            }}}}}})
        print "::::::::::::Analytics  avg number of Lead per entity :::::::::\n\n"
        self.process_avg_analytics(result)


#main method where the decisions are taken
def main(args):
    es_instance =  Elasticsearch(hosts=[{"host":"127.0.0.1", "port":9200}])
    if(args[0]=="fetch_tweets"):
        consumer_key = 'rDNahiYsc9xnuPT1dHIurYhwN'
        consumer_secret = '9JivLmJ5kn9iSYCR9zxcChgOKgf1MIAE7Stpcz28mk0daN7RiF'
        access_token = '3900430219-vhusv7KC1iaf9eefqzF3nBYIOJ0bpDGKNADj4vi'
        access_secret = 'dfU0Zp46mNucjZd4xcgbFnsVGt3GX6ffvcohzMnqHXAZh'
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_secret)
        api = tweepy.API(auth)
        twitter_stream = Stream(auth, Tweet_retriever(es_instance))
        twitter_stream.filter(languages=['en'], track=["a","b", "c","d","e","f","g","h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"])
    elif(args[0]=="fetch_lead"):
        query_object = Query_Lead(es_instance)
        query_object.make_query(args[1:])

    elif(args[0]=="fetch_analytics"):
        analytic_object = Analytics(es_instance)
        if(len(args)>1):
            if(args[1]=="category"):
                analytic_object.get_category_analytics()
            elif(args[1]=="entities"):
                analytic_object.get_entities_analytics()
            elif(args[1]=="reporter"):
                analytic_object.get_reporter_analytics()
            elif(args[1]=="avg_leads_cat"):
                analytic_object.get_avg_category_analytics()
            elif(args[1]=="avg_leads_ent"):
                analytic_object.get_avg_entity_analytics()
            else:
                print "Invalid Choice"

        else:
            print "Insufficient Params"
    else:
        print "Invalid Choice"


if __name__ == "__main__":
    if(len(sys.argv)>1):
        main(sys.argv[1:])
    else:
        print "Insufficient arguments\n"

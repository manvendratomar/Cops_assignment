####### BAKER_STREET execution description #############


Requirements::

1)Elastic Server is up on the system.
2)tweepy
3)elasticsearch

Script has three section which are identified through command line args.


        ######### Section 1 (class Tweet_retriever)##################
Section is about fetching tweets from twitter and index them into elastic server as leads.
If index is not present this script will make index as per the requirement.
to run this section type command on console "python Baker_Street.py 'fetch_tweets'"


        ############ Section  2 (class Query_Lead)#################
Section 2 is to making queries into elastic server.
it has four params to execute first will be query of text which can be any entity, category, country or random tweet text.
Second and third param for temporal query execution where second param is smaller date and third one is larger date in format "2018-03-29T07:05:45" the portion from t is optional.
Fourth parameter is for spatial query you can give lat long as a string parram.
if You are not willing to give any of above param simply put "" in place of that param.
Boosting Relevance is done on the basis of mentioned points in challenge, and it is shown in the mapping of index.

Sample query execution command for tunning section 2

"python Baker_Street.py 'fetch_lead' 'America Mark' '2018-02-12T07:05:45' '2018-02-29T07:05:45' '40, -70'"


       ################# Section 3 (class Analytics) ###############
Section 3 is about lead analytics.
it has one param which is for identifying, about whom we need to send Analytics.

that param will be:

for lead by category: "category"

Explaination:(Sherlock will be needing leads by category to analyze the kind of information we have like,
 categorizing negative leads about suspect or possitive leads about suspect.)


for lead by entities: "entities"
Explaination: (Sherlock will be needing leads by entities to know the entities about which we have information)


for lead by author: "reporter"
Explaination: (Sherlock will be needing leads by author to know the which member of Holmes network was active at network and sent the leads,
 so the when it's necessary Sherlock can contact that particular member about that particular case)


for avg number of  lead per category over a minute/second: "avg_leads_cat"
Explaination: (Sherlock will be needing avg_no. of leads per categogry to analyze how fast a particular case is progressing and in which direction,
 like getting complicated or getting solved/simplified)



for avg number of  lead per entity over a minute/second: "avg_leads_ent"
Explaination: (Sherlock will be needing avg_no. of leads per entity to analyze which suscpect is easily accessible
how frequent information we can have about a particular suspect)


Sample query execution command for tunning section 3

"python Baker_Street.py 'fetch_analytics' 'category'"

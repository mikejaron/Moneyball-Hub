import pandas as pd
import numpy as np
import csv
import time
import smtplib
import json, requests
import pprint

"""
    The api call should be a script or importable function.
    When in your terminal
    $ python api_call.py  'your query title goes here'
    the OPTIONAL params should be
        date_range=
        save_location=
        ...
        every_other_one=


"""








start_time = time.time()
# #### 5 things to do before every time
# ### 1.
threemo = 'no'
project = "google"   
category = "google"    ## this goes into one of the colums maybe it is diff then what you want to save it as
date_range = "all" #+ "_text"
date_range_html = "3 Month"   ## this goes into the title of the graphs
# project1 = project+"_"+time.strftime("%m_%d_%Y")
# ### set date range (ex: 2013-10-25t00:00:00)
less_than = "2015-07-30t00:00:00"
greater_than = "2012-01-01t00:00:00"

# ### 2. 2 part,  do topics and keywords of each one
doctag = str(111)    #### what are you searching for, in tag form

## this is how it is saved
project1 = project+"_"+doctag+"_"+date_range+"_"+time.strftime("%m_%d_%Y")  

directory = "/Users/mjaron/Desktop/MoneyBall/New_code_test/"+project1+".csv"



#################################################################################
#################################################################################
######### Everything below this does not really need to be changed ###############
#################################################################################
#################################################################################

### Main stuff that rarely changes 0= not doing it, 1 = doing it
children_query = 1
clustering = 1
#### additions that may change 1 = doing it, 5 not doing it (any number besides 1 and 0 will imply not doing it)
keyword = 0
keytextquery = 1 
textquery = 0     ### doing a text query vs a tag query
tagquery = 1
source_query = 0
    #### decide if you want to search sentences or documents, normally it is done through sentences
doc_search = 0
sent_search = 1

misc = 0      ### do this if you want to do an unusual query not listed, edit below

if misc == 1:
    query = r'{"query": { "query_string": { "default_field": "text", "query": "google AND privacy" } }}'
 
 #### what are you searching for, in text form
if textquery == 1:
    doctag = ["google AND app"] 
        
source = "obama_speeches"   ####no capitol letters or dashes so split it 
                                ###with a comma and in seperate quotes
    

# ### 4. page size   (i have gone up to 10000 succesfully)
limit = 3000

# ### set date range (ex: 2013-10-25t00:00:00)
less_than = "2015-07-30t00:00:00"
greater_than = "2012-01-01t00:00:00"


# ### 4a. divide by number
if limit > 2000:
    divide = 2000
    if limit >= 5000:
        divide = 3000
else:
    divide = 2000

divide1 = 4000


	
###############################################################
   
# ########################################################################################################################################################3
########################################################################################################################################################3
##########################################################################################################################################################
# #####################################################################################################################################################
data1 = []
data2 = []
data3 = []
data5 = []
doc1 = []
doc11 = []
childrenlist_ch = []
textlist = []
polaritylist_ch = []
subjectivitylist_ch = []
timestamplist_ch = []
docid_all = []
stars = []
type = []
sent_id = []
sent_id1 = []
main_source = []

text_all = []
docid_list = []
polaritylist = []
subjectivitylist = []
timestamplist = []
doctemp2 = []
type2 = []

cat = []
top = []
cat1 = []
top1 = []



writefile = directory
fieldnames = ['topic', 'category', 'timestamp', 'sent_id', 'source', 'type', 'polarity', 'subjectivity', 'text']
with open( writefile, 'w' ) as f:
    writer = csv.writer(f)
    writer.writerow(fieldnames)



AUTH = ('mjaron','canudigit')
# api_url = "http://moneyball.datasco.pe/api/v1/"
api_url = "http://104.197.14.37/api/v1/"
if doc_search == 1:
    search_url = api_url + "documents/dd/search"
elif sent_search == 1:
    search_url = api_url + "sentences/dd/search"
count_search_url = api_url + "sentences/dd/count/"
doc_search_url = api_url + "documents/dd/search"
 
    

def scroll_paginated_query(url, query, page_size=limit, params=None, retry_count_on_fail=3):


    # initialize
    url = url + "?scroll=10m&size=%s" % page_size
    scroll_id = None
    num_hits = 0
    total_hits = 0
    keep_getting_pages = True
    requery = True

    while keep_getting_pages:


        # check if valid response
        num_tries = 0

        while num_tries == 0 or requery:

            # keep track of how many time you requery in case of failure
            num_tries += 1

        #### Try Catch block HERE
            # make the query
            try:
                response = requests.post(url,
                                         params=params,
                                         data=query,
                                         auth=AUTH,
                                         headers={"content-type":"application/json"},
                                         timeout = 25)
            except:
                time.sleep(60)
                response = requests.post(url,
                                         params=params,
                                         data=query,
                                         auth=AUTH,
                                         headers={"content-type":"application/json"},
                                         timeout = 25)


            results_page = json.loads(response.text)

            # get the scroll_id from pagination (which url to send an empty query for the next page)
            # if this was the first query
            if not scroll_id:

                if "_scroll_id" in results_page:
                    scroll_id = results_page["_scroll_id"]
                    url = url+"&scroll_id=%s" % scroll_id
                    query = '{}'
                else:
                    msg = "Failed query on first page"
                    print "The query that failed: %s" % query
                    print "Failure response :"
                    pprint.pprint(results_page)
                    raise ValueError(msg)


            if 'hits' in results_page:
                # expected results_page

                # report on number of hits
                num_hits = len(results_page['hits']['hits'])
                print "%i hits on current page" % num_hits
                total_hits += num_hits
                print '%i hits encountered so far' % total_hits
                #escape
                requery = False

                # if there are no hits left, we'll stop querying, pagination finished
                if num_hits == 0:
                    keep_getting_pages = False


            elif num_tries <= retry_count_on_fail:

                # query failed. retry again and again until the retry limit
                requery = True


            elif num_tries > retry_count_on_fail:

                msg = "Query failed."
                print "The query that failed: %s" % query
                print "Failed response:"
                pprint.pprint(results_page)
                raise ValueError(msg)

        # end of while (retrying loop)
        yield results_page



# EXAMPLE 1
# SCROLLED QUERY: ALL SENTENCES WITH TAG 99
def get_info_on_sentences_with_a_tag(query, search_url):
    print search_url
    for page in scroll_paginated_query(search_url, query):
    #     for page in scroll_paginated_query(doc_search_url, query):
        main_source1 = [hit['_source']["source"] for hit in page['hits']['hits']]
        id = [hit['_source']["_id"] for hit in page['hits']['hits']]
        if sent_search == 1:
            documents = [hit['_source']["doc_id"] for hit in page['hits']['hits']]
        texts = [hit['_source']["text"]for hit in page['hits']['hits']]
        polarities = [hit['_source']["sentiment"]["polarity"]for hit in page['hits']['hits']]
        subjectivities = [hit['_source']["sentiment"]["subjectivity"]for hit in page['hits']['hits']]
        timestamps = [hit['_source']["timestamp"]for hit in page['hits']['hits']]

        ####### For Yelp
        # star = [hit['_source']["parent"]for hit in page['hits']['hits']]
        # 		for temp in star:
        # 			documentstring = str(temp)
        # 			stars.append(documentstring[:])
        main_source = []
        for temp in main_source1:
            string = str(temp)
            url = string.find("https://")
            url1 = string.find("http://")
            url2 = string.find("www.")
            end1 = string.find(".net")
            end = string.find(".com")
            if url >= 0:
                if url2 >= 0:
                    if end >= 0:
                        main_source.append(string[12:end])
                    elif end1 >= 0:
                        main_source.append(string[12:end1])
                else:
                    if end >= 0:
                        main_source.append(string[8:end])
                    elif end1 >= 0:
                        main_source.append(string[8:end1])
            elif url1 >= 0:
                if url2 >= 0:
                    if end >= 0:
                        main_source.append(string[11:end])
                    elif end1 >= 0:
                        main_source.append(string[11:end1])
                else:
                    if end >= 0:
                        main_source.append(string[7:end])
                    elif end1 >= 0:
                        main_source.append(string[7:end1])   
            else:
                main_source.append(string[:]) 


        data1 = []
        for temp in timestamps:
            documentstring = str(temp)
            data1.append(documentstring[:])
        ### this was to get only the date  data1.append(documentstring[:10])
        data2 = []
        for temp in polarities:
            documentstring = str(temp)
            data2.append(documentstring[:])
        data3 = []
        for temp in subjectivities:
            documentstring = str(temp)
            data3.append(documentstring[:])
        textlist = []
        for temp in texts:
            documentstring = temp.encode('utf-8')
            textlist.append(documentstring[:])
        cat = []
        top = []
        type = []
        data5 = []
        if sent_search == 1:
            for temp in documents:
                cat.append(category)
                top.append(category)
                documentstring = str(temp)
                doc1.append(documentstring[:])
                dash = documentstring.find("-")
                string = documentstring[:dash]
                split = string.find("tweet")
                if split == 0:
                    data5.append("twitter")
                    type.append(string[:])
                else:
                    if split == -1:
                        split = string.find("Article")
                        split1 = split
                    if split == -1:
                        split = string.find("sDiscussion")
                        split1 = split + 1
                    if split == -1:
                        split = string.find("Discussion")
                        split1 = split
                    if split == -1:
                        split = string.find("Forum")
                        split1 = split
                    if split == -1:
                        split = string.find("Blog")
                        split1 = split
                    if split == -1:
                        split = string.find("Comment")
                        split1 = split
                    if split == -1:
                        split = string.find("NewsItem")
                        split1 = split
                    if split == -1:
                        split = string.find("Review")
                        split1 = split
                    if split == -1:
                        split = string.find("Feature")
                        split1 = split
                    if split == -1:
                        split = string.find("ForumsMessage")
                        split1 = split
                    if split == -1:
                        split = string.find("ForumPost")
                        split1 = split

                    data5.append(string[:split])
                    type.append(string[split1:])

            send_id = []
            for temp in id:
                documentstring = str(temp)
                sent_id.append(documentstring[:])
        elif doc_search == 1:
            for temp in id:
                cat.append(category)
                top.append(category)
                documentstring = str(temp)
                doc1.append(documentstring[:])
                dash = documentstring.find("-")
                string = documentstring[:dash]
                split = string.find("tweet")
                if split == 0:
                    data5.append("twitter")
                    type.append(string[:])
                else:
                    if split == -1:
                        split = string.find("Article")
                        split1 = split
                    if split == -1:
                        split = string.find("sDiscussion")
                        split1 = split + 1
                    if split == -1:
                        split = string.find("Discussion")
                        split1 = split
                    if split == -1:
                        split = string.find("Forum")
                        split1 = split
                    if split == -1:
                        split = string.find("Blog")
                        split1 = split
                    if split == -1:
                        split = string.find("Comment")
                        split1 = split
                    if split == -1:
                        split = string.find("NewsItem")
                        split1 = split
                    if split == -1:
                        split = string.find("Review")
                        split1 = split
                    if split == -1:
                        split = string.find("Feature")
                        split1 = split
                    if split == -1:
                        split = string.find("ForumsMessage")
                        split1 = split
                    if split == -1:
                        split = string.find("ForumPost")
                        split1 = split

                    data5.append(string[:split])
                    type.append(string[split1:])
                    sent_id.append(string[:split])

        writefile = directory
        with open( writefile, 'a' ) as f:
            writer = csv.writer(f)
#                 writer.writerows(zip(top, cat, data1, sent_id, data5, type, data2, data3, textlist))
            writer.writerows(zip(top, cat, data1, sent_id, main_source, type, data2, data3, textlist))


# ### Children
parent_list = []
parent_doc1 = []
parent_doc2 = []
def children(u_doc1, doc_search_url):
###### the doc_id must be tokenized, take out the dash and split into 2 strings, make a list of lists
    f = []
#     for i in u_doc1:
#         o = i.replace(" ","-")
#         a = o.find("-")
#         a1 = o[a+1:].find("-")
#         if a1 == -1:
#             c = o[:a]
#             d = o[a+1:]
#             f.append('["'+c+'","'+d+'"]')
#             parent_doc1.append(c)
#             parent_doc2.append(d)
#     document_id_list_str = ('[' + ', '.join(f) + ']') 
    document_id_orig = '['+ ', '.join(['"%s"' % _id for _id in u_doc1]) + ']'
    
#     children_query = '{"query": {"filtered": {"filter": { "terms": { "parent": %s }} }}}' % document_id_list_str
    children_query = '{"query": {"filtered": {"filter": { "terms": { "parent.original": %s }} }}}' % document_id_orig
####  To search for 1 document
#     children_query = '{"query": {"match": {"parent": %s  }}}' % document_id_list_str
#     print children_query
    for page in scroll_paginated_query(doc_search_url, children_query):
        # Example task: collect documents, text, polarity, etc. info
        parent = [hit['_source']["parent"] for hit in page['hits']['hits']]
        main_source1 = [hit['_source']["source"] for hit in page['hits']['hits']]
        id2 = [hit['_source']["sentences"] for hit in page['hits']['hits']]
        children = [hit['_source']["_id"] for hit in page['hits']['hits']]
        timestamp_child = [hit['_source']["timestamp"]for hit in page['hits']['hits']]
        text_child = [hit['_source']["text"]for hit in page['hits']['hits']]
        polarity_child = [hit['_source']["sentiment"]["polarity"]for hit in page['hits']['hits']]
        subjectivity_child = [hit['_source']["sentiment"]["subjectivity"]for hit in page['hits']['hits']]
        

        for temp in parent:
            parent_list.append(str(temp))
        main_source = []
        for temp in main_source1:
            string = str(temp)
            url = string.find("https://")
            url1 = string.find("http://")
            url2 = string.find("www.")
            end = string.find(".com")
            if url >= 0:
                if url2 >=0:
                    main_source.append(string[12:end])
                else:
                    main_source.append(string[8:end])
            elif url1 >= 0:
                if url2 >=0:
                    main_source.append(string[11:end])
                else:
                    main_source.append(string[7:end])   
            else:
                main_source.append(string[:]) 
        data1 = []
        for temp in timestamp_child:
            documentstring = str(temp)
            data1.append(documentstring[:])
        data2 = [] 
        for temp in polarity_child:
            documentstring = str(temp)
            data2.append(documentstring[:])
        data3 = []
        for temp in subjectivity_child:
            documentstring = str(temp)
            data3.append(documentstring[:])
        textlist = []
        for temp in text_child:
            documentstring = temp.encode('utf-8')
            textlist.append(documentstring[:])
        cat = []
        top = []
        type = []
        data5 = []
        for temp in children:
            cat.append(category+"_children")
            top.append(category)
            documentstring = str(temp)
            doc1.append(documentstring[:])
            dash = documentstring.find("-")
            string = documentstring[:dash]
            split = string.find("tweet")	
            if split == 0:
                data5.append("twitter")
                type.append(string[:])
            else:
                if split == -1:
                    split = string.find("Article")
                    split1 = split
                if split == -1:
                    split = string.find("sDiscussion")
                    split1 = split + 1
                if split == -1:
                    split = string.find("Discussion")
                    split1 = split
                if split == -1:
                    split = string.find("Forum")
                    split1 = split
                if split == -1:
                    split = string.find("Blog")
                    split1 = split
                if split == -1:
                    split = string.find("Comment")
                    split1 = split
                if split == -1:
                    split = string.find("NewsItem")
                    split1 = split
                if split == -1:
                    split = string.find("Review")
                    split1 = split
                if split == -1:
                    split = string.find("Feature")
                    split1 = split
                if split == -1:
                    split = string.find("ForumsMessage")
                    split1 = split
                if split == -1:
                    split = string.find("ForumPost")
                    split1 = split

                data5.append(string[:split])
                type.append(string[split1:])
        sent_id = []
        for temp in id2:
            documentstring = str(temp)
            sent_id.append(documentstring[:])

        writefile = directory
        with open( writefile, 'a' ) as f:
            writer = csv.writer(f)
#             writer.writerows(zip(top, cat, data1, sent_id, data5, type, data2, data3, textlist))
            writer.writerows(zip(top, cat, data1, sent_id, main_source, type, data2, data3, textlist))


# ## Keywords
def keywords(u_docs, topics, i, k):
#     w = 0
    document_id_list_str = '['+ ', '.join(['"%s"' % _id for _id in u_docs]) + ']'
#     for tem123 in topic:
#         for temp12 in tem123:
    if keytextquery == 1:
        query = '{"query":{"filtered":{"filter": {"and": [{"terms": {"doc_id.original": %s }},{"query": {"query_string":{"query": "%s","default_field":"text"}}} ] }}}}' % (document_id_list_str, i)
    else:
        query = '{"query":{"filtered":{"filter": {"and": [{"terms": {"doc_id.original": %s }},{"query": {"filtered": {"filter": { "term": { "tags": "%s" }}}}} ] }}}}' % (document_id_list_str, i)
    ##        query = '{"query":{"filtered":{"filter": {"and": [{"terms": {"doc_id.original": %s }},{"query": {"query_string":{"query": ["\"johnny\""],"default_field":"text"}}} ] }}}}' % document_id_list_str
    for page in scroll_paginated_query(search_url, query):
        # Example task: collect documents, text, polarity, etc. info
        main_source1 = [hit['_source']["source"] for hit in page['hits']['hits']]
        id3 = [hit['_source']["_id"] for hit in page['hits']['hits']]
        documents2 = [hit['_source']["doc_id"] for hit in page['hits']['hits']]
        text2 = [hit['_source']["text"]for hit in page['hits']['hits']]
        polarity2 = [hit['_source']["sentiment"]["polarity"]for hit in page['hits']['hits']]
        subjectivity2 = [hit['_source']["sentiment"]["subjectivity"]for hit in page['hits']['hits']]
        timestamp2 = [hit['_source']["timestamp"]for hit in page['hits']['hits']]

        if(text2):
            for temp in main_source1:
                string = str(temp)
                url = string.find("https://")
                url1 = string.find("http://")
                url2 = string.find("www.")
                end = string.find(".com")
                if url >= 0:
                    if url2 >=0:
                        main_source.append(string[12:end])
                    else:
                        main_source.append(string[8:end])
                elif url1 >= 0:
                    if url2 >=0:
                        main_source.append(string[11:end])
                    else:
                        main_source.append(string[7:end])   
                else:
                    main_source.append(string[:]) 
            data1 = []
            for temp in timestamp2:
                documentstring = str(temp)
                data1.append(documentstring[:])
# 						timestamplist.append(documentstring[:])
            data2 = []
            for temp in polarity2:
                documentstring = str(temp)
                data2.append(documentstring[:])
# 						polaritylist.append(documentstring[:])
            data3 = []
            for temp in subjectivity2:
                documentstring = str(temp)
                data3.append(documentstring[:])
# 						subjectivitylist.append(documentstring[:])
            textlist = []
            for temp in text2:
                documentstring = temp.encode('utf-8')
                textlist.append(documentstring[:])
# 						text_all.append(documentstring[:])
            cat = []
            top = []
            type = []
            data5 = []                    
            for temp in documents2:
                documentstring = str(temp)
                cat.append(topics[k])
#                         cat.append(temp12)
                top.append(topics[k])
#                         top.append(topics[w])
                doc1.append(documentstring[:])
# 						cat1.append(temp12)
# 						top1.append(topics[w])
                dash = documentstring.find("-")
                string = documentstring[:dash]
                split = string.find("tweet")

                if split == 0:
                    data5.append("twitter")
                    type.append(string[:])
                # 							docid_list.append("twitter")
                # 							type1.append(string[:])
                else:
                    if split == -1:
                        split = string.find("Article")
                        split1 = split
                    if split == -1:
                        split = string.find("sDiscussion")
                        split1 = split + 1
                    if split == -1:
                        split = string.find("Discussion")
                        split1 = split
                    if split == -1:
                        split = string.find("Forum")
                        split1 = split
                    if split == -1:
                        split = string.find("Blog")
                        split1 = split
                    if split == -1:
                        split = string.find("Comment")
                        split1 = split
                    if split == -1:
                        split = string.find("NewsItem")
                        split1 = split
                    if split == -1:
                        split = string.find("Review")
                        split1 = split
                    if split == -1:
                        split = string.find("Feature")
                        split1 = split
                    if split == -1:
                        split = string.find("ForumsMessage")
                        split1 = split
                    if split == -1:
                        split = string.find("ForumPost")
                        split1 = split
                    data5.append(string[:split])
                    type.append(string[split1:])
                # 							docid_list.append(string[:split])
                # 							type1.append(string[split:])
            sent_id = []
            for temp in id3:
                documentstring = str(temp)
                sent_id.append(documentstring[:])
# 						sent_id1.append(documentstring[:])

            writefile = directory
            with open( writefile, 'a' ) as f:
                writer = csv.writer(f)
#               %%!writer.writerows(zip(top, cat, data1, sent_id, data5, type, data2, data3, textlist))
                writer.writerows(zip(top, cat, data1, sent_id, main_source, type, data2, data3, textlist))
            time.sleep(1)

# w += 1			



# ###################################################################################################################################################################
################################################################################################################################################################
############################################################################################################################################################
# ########################################################################################################################################

if source_query == 1:
    query = r'{"query": {"filtered": { "filter": {"term": {"source": '+'"'+source+'"'+'} }}}}'

elif tagquery == 1:
    query = r'{"query": {"filtered": {"filter":{"range":{"timestamp":{"gt":'+'"'+greater_than+'"'+', "lt":'+'"'+less_than+'"'+'}}}, "query": {"filtered": {"filter": { "term": { "tags": '+doctag+' }}}} }}}'
##text query
# elif textquery == 1:   
#     query = r'{"query": {"filtered": {"filter":{"range":{"timestamp":{"gt":'+'"'+greater_than+'"'+', "lt":'+'"'+less_than+'"'+'}}}, "query": {"query_string":{ "default_field": "text", "query":'+'"'+i+'"'+' }} }}}'
#     query = r'{"query": {"filtered": {"filter":{"term":{"source":"yelp"}}}}}'
elif misc == 1:
     query = query

if textquery == 1:  
    for i in doctag:
#         category = i
        print i
        query = r'{"query": {"filtered": {"filter":{"range":{"timestamp":{"gt":'+'"'+greater_than+'"'+', "lt":'+'"'+less_than+'"'+'}}}, "query": {"query_string":{ "default_field": "text", "query":'+'"'+i+'"'+' }} }}}'
        print query
        get_info_on_sentences_with_a_tag(query, search_url)
else:
    print query
    get_info_on_sentences_with_a_tag(query, search_url)

u_doc1 = list(set(doc1))
if children_query == 1:
    print "*** start children ****"
    print len(doc1), "length all doc ids"
    print len(u_doc1), "length unique doc ids"
    cycles = float(len(u_doc1)/divide)
    print cycles, "cycles"
    print "****STARTING******"
    x = 0
    for temp in range(0,(len(u_doc1)+divide)/divide):
        print "***", x, "out of", cycles, "***"
        children(u_doc1[temp*divide:temp*divide+(divide-1)], doc_search_url)
        x+=1
    u_docs = list(set(doc1))
    print len(u_docs)

### keyword  -> 0=not doing it, 1=doing it 
if keyword == 1:
    print "**** start keywords ****"
    print float(len(u_docs)/divide1)
    k = 0
    for i in keytag:
        print i
        for temp in range(0,(len(u_docs)+divide1)/divide1):
            print "*** start temp =", temp
    #         keywords(u_docs[temp*divide1:temp*divide1+(divide1-1)], topic)
            keywords(u_docs[temp*divide1:temp*divide1+(divide1-1)], topics, i, k)
        k += 1


end_time = time.time()
print "***Finished***"
print "time (seconds)=", end_time-start_time
# done_text(start_time, end_time
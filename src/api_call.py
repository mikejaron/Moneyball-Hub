import csv
import json
import requests
import pprint
import time

def scroll_paginated_query(query, url, page_size=3000, params=None, AUTH = ('mjaron','canudigit') ,retry_count_on_fail=3):
    """
    INPUT: url=string of apit to query, query=elasticsearch string, page_size=integer
    OUTPUT: This is the main funtion to query the api, and split it into pages to make things
    easier.
    """ 
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


def get_info_on_sentences_with_a_tag(tag='130', begin_date='2012-01-01t00:00:00', end_date='2015-07-30t00:00:00', search_url="http://104.197.14.37/api/v1/sentences/dd/search", save_directory=''):
    """
    INPUT: begin_date & end_date= date in 'y-m-dth:m:s' format, search_url=url of api string, 
    save_directory=string
    OUTPUT: This calls previous function and gets the json from that, parses it and saves it
    to a csv
    """
    ### initialize csv
    writefile = save_directory+tag+".csv"
    fieldnames = ['timestamp','source', 'polarity', 'subjectivity', 'text']
    with open( writefile, 'w' ) as f:
        writer = csv.writer(f)
        writer.writerow(fieldnames)

    query = r'{"query": {"filtered": {"filter":{"range":{"timestamp":{"gt":'+'"'+begin_date+'"'+', "lt":'+'"'+end_date+'"'+'}}}, "query": {"filtered": {"filter": { "term": { "tags": '+tag+' }}}} }}}'
    doc1 = []
    print search_url
    print query
    for page in scroll_paginated_query(query=query, url=search_url):
        main_source1 = [hit['_source']["source"] for hit in page['hits']['hits']]
        documents = [hit['_source']["doc_id"] for hit in page['hits']['hits']]
        texts = [hit['_source']["text"]for hit in page['hits']['hits']]
        polarities = [hit['_source']["sentiment"]["polarity"]for hit in page['hits']['hits']]
        subjectivities = [hit['_source']["sentiment"]["subjectivity"]for hit in page['hits']['hits']]
        timestamps = [hit['_source']["timestamp"]for hit in page['hits']['hits']]

        main_source = []
        for temp in main_source1:
            string = str(temp)
            main_source.append(string[:]) 


        date_list = []
        for temp in timestamps:
            documentstring = str(temp)
            date_list.append(documentstring[:]) ### this was to get only the date  date_list.append(documentstring[:10])
        
        pol_list = []
        for temp in polarities:
            documentstring = str(temp)
            pol_list.append(documentstring[:])
        sub_list = []
        for temp in subjectivities:
            documentstring = str(temp)
            sub_list.append(documentstring[:])
        textlist = []
        for temp in texts:
            documentstring = temp.encode('utf-8')
            textlist.append(documentstring[:])
        #### this is to save to a main list of all doc ids to use later for children
        for temp in documents:
            documentstring = str(temp)
            doc1.append(documentstring[:])

        writefile = save_directory+tag+".csv"
        with open( writefile, 'a' ) as f:
            writer = csv.writer(f)
            writer.writerows(zip(date_list, main_source, pol_list, sub_list, textlist))
def scroll_paginated_query(url, query, page_size=limit, params=None, retry_count_on_fail=3):
    """
    INPUT: url=string of apit to query, query=ealsticsearch string, page_size=integer
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


def get_info_on_sentences_with_a_tag(query, search_url, save_directory):
    """
    INPUT: query=elasticsearch string, search_url=url of api string, save_directory=string
    OUTPUT: This calls previous function and gets the json from that, parses it and saves it
    to a csv
    """
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
        #       for temp in star:
        #           documentstring = str(temp)
        #           stars.append(documentstring[:])
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

        writefile = save_directory
        with open( writefile, 'a' ) as f:
            writer = csv.writer(f)
#                 writer.writerows(zip(top, cat, data1, sent_id, data5, type, data2, data3, textlist))
            writer.writerows(zip(top, cat, data1, sent_id, main_source, type, data2, data3, textlist))
import requests, os, time, json, io
import argparse, sys
from datetime import datetime


from multiprocessing import Pool

postListGlobal = []


##########################################################################################################
def getRequests(url):

    requests_result = requests.get(url, headers={'Connection':'close'}).json()
    time.sleep(0.01)
    return requests_result

##########################################################################################################
def getFeedIds(feeds, feed_list):

    feeds = feeds['feed'] if 'feed' in feeds else feeds

    for feed in feeds['data']:
        feed_list.append(feed['id'])
        if not stream:
            print('Feed found: ' + feed['id'] + '\n')
            #log.write('Feed found: ' + feed['id'] + '\n')
    
    if 'paging' in feeds and 'next' in feeds['paging']:
        feeds_url = feeds['paging']['next']
        feed_list = getFeedIds(getRequests(feeds_url), feed_list)
    return feed_list

##########################################################################################################
def getComments(comments, comments_count):

    # If comments exist.
    comments = comments['comments'] if 'comments' in comments else comments
    if 'data' in comments:

        if not stream:
            comments_dir = 'comments/'
            if not os.path.exists(comments_dir):
                os.makedirs(comments_dir)

        for comment in comments['data']:

            comment_content = {
                'id': comment['id'],
                'user_id': comment['from']['id'],
                'user_name': comment['from']['name'] if 'name' in comment['from'] else None,
                'message': comment['message'],
                'like_count': comment['like_count'] if 'like_count' in comment else None,
                'created_time': comment['created_time']
            }

            comments_count+= 1

            if stream:
                print(comment_content)
            else:
                #print('Processing comment: ' + comment['id'] + '\n')
                comment_file = open(comments_dir + comment['id'] + '.json', 'w')
                comment_file.write(json.dumps(comment_content, indent = 4, ensure_ascii = False))
                comment_file.close()
                #log.write('Processing comment: ' + feed_id + '/' + comment['id'] + '\n')

        # Check comments has next or not.
        if 'next' in comments['paging']:
            comments_url = comments['paging']['next']
            comments_count = getComments(getRequests(comments_url), comments_count)

    return comments_count

##########################################################################################################
def getReactions(reactions, reactions_count_dict):
    # If reactions exist.
    reactions = reactions['reactions'] if 'reactions' in reactions else reactions
    if 'data' in reactions:

        if not stream:
            reactions_dir = 'reactions/'
            if not os.path.exists(reactions_dir):
                os.makedirs(reactions_dir)

        for reaction in reactions['data']:

            if reaction['type'] == 'LIKE':
                reactions_count_dict['like']+= 1
            elif reaction['type'] == 'LOVE':
                reactions_count_dict['love']+= 1
            elif reaction['type'] == 'HAHA':
                reactions_count_dict['haha']+= 1
            elif reaction['type'] == 'WOW':
                reactions_count_dict['wow']+= 1
            elif reaction['type'] == 'SAD':
                reactions_count_dict['sad']+= 1
            elif reaction['type'] == 'ANGRY':
                reactions_count_dict['angry']+= 1

            if stream:
                print(reaction)
            else:
               #bleh print('Processing reaction: ' + reaction['id'] + '\n')
                reaction_file = open(reactions_dir + reaction['id'] + '.json', 'w')
                reaction_file.write(json.dumps(reaction, indent = 4, ensure_ascii = False))
                reaction_file.close()
                #log.write('Processing reaction: ' + feed_id + '/' + reaction['id'] + '\n')

        # Check reactions has next or not.
        if 'next' in reactions['paging']:
            reactions_url = reactions['paging']['next']
           # print("REAC CHECK: " + str(getRequests(reactions_url), reactions_count_dict))
            reactions_count_dict = getReactions(getRequests(reactions_url), reactions_count_dict)
            
    return reactions_count_dict

##########################################################################################################
def getFollowerCount(followerCount_req):
    followerCount_req = followerCount_req['followerCount_req'] if 'followerCount_req' in followerCount_req else followerCount_req
    #print(followerCount_req)
    return followerCount_req['fan_count']


##########################################################################################################
def getAttachments(attachments, attachments_content):

    # If attachments exist.
    attachments = attachments['attachments'] if 'attachments' in attachments else attachments
    if 'data' in attachments:
        attachments_content['title'] = attachments['data'][0]['title'] if 'title' in attachments['data'][0] else ''
        attachments_content['description'] = attachments['data'][0]['description'] if 'description' in attachments['data'][0] else ''
        attachments_content['target'] = attachments['data'][0]['target']['url'] if 'target' in attachments['data'][0] and 'url' in attachments['data'][0]['target'] else ''

    return attachments_content

##########################################################################################################
def getFeedType(feedType_req):
        feedType_req = feedType_req['feedType_req'] if 'feedType_req' in feedType_req else feedType_req
        return feedType_req['type']

##########################################################################################################
def getMessage(message_req):
        message_req = message_req['message_req'] if 'message_req' in message_req else message_req
        return message_req['message']

##########################################################################################################
def getOptimizedReactions(opt_reactions):
        opt_reactions = opt_reactions['opt_reactions'] if 'opt_reactions' in opt_reactions else opt_reactions
        #print("USER THIS: ", opt_reactions)
        like = ((opt_reactions['LIKE'])['summary'])['total_count']
        #print("LIKE: ", like)
        love = ((opt_reactions['LOVE'])['summary'])['total_count']
        haha = ((opt_reactions['HAHA'])['summary'])['total_count']
        wow = ((opt_reactions['WOW'])['summary'])['total_count']
        sad = ((opt_reactions['SAD'])['summary'])['total_count']
        angry =((opt_reactions['ANGRY'])['summary'])['total_count']

        reactions_count_dict1 = {
            'like': like,
            'love': love,
            'haha': haha,
            'wow': wow,
            'sad': sad,
            'angry': angry
        }


        return reactions_count_dict1

##########################################################################################################
def getFeed(feed_id):

    global postListGlobal

    feed_url = 'https://graph.facebook.com/v2.7/' + feed_id
    accessable_feed_url = feed_url + '?' + tokenGlobal
    #print("accessable " + accessable_feed_url)
    message_feed_url = feed_url +'?fields=message&' + tokenGlobal
    #print("message: " + message_feed_url)

    post = dict()
    feed_type_url = feed_url + '?fields=type&' + tokenGlobal
    feed_type = getFeedType(getRequests(feed_type_url))
    #print("type" + feed_type)
    post['type'] = feed_type
    #print("Feed type: " + feed_type)

    message = getMessage(getRequests(message_feed_url))
    #print(type(message))
    post['message'] = message
    # message.decode('utf-8')



    if not stream:
        feed_dir = feed_id + '/'
        if not os.path.exists(feed_dir):
            os.makedirs(feed_dir)

        os.chdir(feed_dir)

        print('\nProcessing feed: ' + feed_id + '\nAt: ' + datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S') + '\n')
        log = open('../log', 'a')
        log.write('\nProcessing feed: ' + feed_id + '\nAt: ' + datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S') + '\n')
        log.close()

    # For comments.
    comments_url = feed_url + '?fields=comments.limit(100)&' + token
    
    comments_count = getComments(getRequests(comments_url), 0)
    post['comment count'] = comments_count


    reactions_summary_url = feed_url + '?fields=reactions.type(LIKE).limit(0).summary(true).as(LIKE),reactions.type(LOVE).limit(0).summary(true).as(LOVE),\
    reactions.type(HAHA).limit(0).summary(true).as(HAHA),reactions.type(WOW).limit(0).summary(true).as(WOW),\
    reactions.type(SAD).limit(0).summary(true).as(SAD),reactions.type(ANGRY).limit(0).summary(true).as(ANGRY)&' + token

    opt = getOptimizedReactions(getRequests(reactions_summary_url))
    #print("DIC 1: " , opt)

    #print("YAAAA: ", reactions_summary_url)

    # For reactions.
    # if get_reactions:
    #     reactions_count_dict = {
    #         'like': 0,
    #         'love': 0,
    #         'haha': 0,
    #         'wow': 0,
    #         'sad': 0,
    #         'angry': 0
    #     }
    #     reactions_url = feed_url + '?fields=reactions.limit(100)&' + token
    #     test = feed_url + 'fields=reactions.type(like)&' + token
    #     #print('TESTTTT', test)
    #     reactions_count_dict = getReactions(getRequests(reactions_url), reactions_count_dict)
      #  post['reactions'] = reactions_count_dict
    post['reactions'] = opt
    

       # print("DIC: " , reactions_count_dict)
    #postListGlobal.append(post)

    

    #print(data)


    # For attachments.
    attachments_content = {
        'title': '',
        'description': '',
        'target': ''
    }
    attachments_url = feed_url + '?fields=attachments&' + token
    attachments_content = getAttachments(getRequests(attachments_url), attachments_content)

    # For feed content.
    feed = getRequests(feed_url + '?' + token)

    if 'message' in feed:
        feed_content = {
            'id': feed['id'],
            'message': feed['message'],
            'link': feed['link'] if 'link' in feed else None,
            'created_time': feed['created_time'],
            'comments_count': comments_count
        }

        feed_content.update(attachments_content)

        if get_reactions:
            feed_content.update(opt)

        if stream:
            print(feed_content)
        else:
            feed_file = open(feed_id + '.json', 'w')
            feed_file.write(json.dumps(feed_content, indent = 4, ensure_ascii = False))
            feed_file.close()

    if not stream:
        os.chdir('../')

    return post

##########################################################################################################
def getTarget(target):
    
    if not stream:
        target_dir = target + '/'
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)
        os.chdir(target_dir)

        log = open('log', 'w')
        start_time = datetime.now()
        execution_start_time = time.time()
        print('Task start at:' + datetime.strftime(start_time, '%Y-%m-%d %H:%M:%S') + '\nTaget: ' + target + '\nSince: ' + since + '\nUntil: ' + until + '\n')
        log.write('Task start at:' + datetime.strftime(start_time, '%Y-%m-%d %H:%M:%S') + '\nTaget: ' + target + '\nSince: ' + since + '\nUntil: ' + until + '\n')
        log.close()

    #Get list of feed id from target.
    feeds_url = 'https://graph.facebook.com/v2.7/' + target + '/?fields=feed.limit(100).since(' + since + ').until(' + until + '){id}&' + token
    feed_list = getFeedIds(getRequests(feeds_url), [])

    if not stream:
        feed_list_file = open('feed_ids', 'w')
        for id in feed_list:
            feed_list_file.write(id + '\n')
        feed_list_file.close()

    
   

    feed_list = [str(i) for i in feed_list] 
   # print("Feed LIST: " + feed_list)
    

    #Get message, comments and reactions from feed.
    target_pool = Pool()
    postList = target_pool.map(getFeed, feed_list)
    #print(postList)    


    followerCount_url = 'https://graph.facebook.com/'+target+'/?fields=fan_count&'+token
    followerCount = getFollowerCount(getRequests(followerCount_url))

    #print(postListGlobal)
    data = {
        'Artist Name' : 'x',
        'Artist Login' : target ,
        'File create date/time' : str(datetime.now()), 
        'Follower Count' : followerCount,
        'Posts': postList
    }

    #print(data)

    with io.open (target +'.json', 'w', encoding = "utf-8") as fp:
        json.dump(data, fp, indent = 4, ensure_ascii=False)


    target_pool.close()

    if not stream:
        end_time = datetime.now()
        cost_time = time.time() - execution_start_time
        print('\nTask end Time: ' + datetime.strftime(end_time, '%Y-%m-%d %H:%M:%S') + '\nTime Cost: ' + str(cost_time))
        log = open('log', 'a')
        log.write('\nTask end Time: ' + datetime.strftime(end_time, '%Y-%m-%d %H:%M:%S') + '\nTime Cost: ' + str(cost_time))
        log.close()
        os.chdir('../')


##########################################################################################################
if __name__ == '__main__':


    # Set crawler target and parameters.
    parser = argparse.ArgumentParser()

    parser.add_argument("target", help="Set the target fans page(at least one) you want to crawling. Ex: 'appledaily.tw' or 'appledaily.tw, ETtoday'")
    parser.add_argument("since", help="Set the start date you want to crawling. Format: 'yyyy-mm-dd HH:MM:SS'")
    parser.add_argument("until", help="Set the end date you want to crawling. Format: 'yyyy-mm-dd HH:MM:SS'")

    parser.add_argument("-r", "--reactions", help="Collect reactions or not. Default is no.")
    parser.add_argument("-s", "--stream", help="If yes, this crawler will turn to streaming mode.")

    args = parser.parse_args()

    target = str(args.target) #artist fb name
    global targGlobal 
    targGlobal = target


    since = str(args.since)
    until = str(args.until)

    if args.reactions == 'yes':
        get_reactions = True
    else:
        get_reactions = False

    if args.stream == 'yes':
        stream = True
    else:
        stream = False

    app_id = '157949204752963'
    app_secret = '2aa1ebdf5aa5cb1e190ffdc21b032c99'

    token = 'access_token=' + app_id + '|' + app_secret

    global tokenGlobal
    tokenGlobal = token

    #Create a directory to restore the result if not in stream mode.
    if not stream:
        result_dir = 'Result/'
        if not os.path.exists(result_dir):
            os.makedirs(result_dir)
        os.chdir(result_dir)

    if target.find(',') == -1:
        #followerCount_url = 'https://graph.facebook.com/'+target+'/?fields=fan_count&'+token
        #print("FOLLOWWWWW:" + followerCount_url)
        #followerCount = getFollowerCount(getRequests(followerCount_url))
        #print("COUNT " + str(followerCount))
        getTarget(target)
        

         #For follower Count
        
    else:
        #followerCount_url = 'https://graph.facebook.com/'+target+'/?fields=fan_count&'+token
        #followerCount = getFollowerCount(getRequests(followerCount_url))

        #print("FOLLOWWWWW: " + str(followerCount_url))
        target = target.split(',')
        for t in target :
            getTarget(t)

    #print(postListGlobal)
        



"""Publishes multiple messages to a Pub/Sub topic with an error handler."""
import time
import os
import google
from google.cloud import pubsub_v1

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'pubsubtest.json'


def createpub(project_id, topic_id):
    from google.cloud import pubsub_v1

    # TODO(developer)
    # project_id = "your-project-id"
    # topic_id = "your-topic-id"

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    topic = publisher.create_topic(topic_path)

    print("Topic created: {}".format(topic))


def createsubscription(topicname, project_id, subscriptionname):
    subscriber = pubsub_v1.SubscriberClient()
    topic_name = 'projects/{project_id}/topics/{topic}'.format(
        project_id=project_id,
        topic=topicname,  # Set this to something appropriate.
        )
    subscription_name = 'projects/{project_id}/subscriptions/{sub}'.format(
        project_id=project_id,
        sub=subscriptionname,  # Set this to something appropriate.
        )
    subscriber.create_subscription(
        name=subscription_name, topic=topic_name)
    print("Subscription created: {}".format(subscription_name))


def checktopicavailable(project_id, topicid):
    print('Verifying the topic')
    client = pubsub_v1.PublisherClient()
    try:
        topic = client.topic_path(project_id, topicid)
        response = client.get_topic(topic)
        return response
    except google.api_core.exceptions.NotFound as er:
        print('Topic doesnot exist. Creating New Topic :{}'.format(topicid))
        createpub(project_id, topicid)


def checksubscriptionavailable(project_id, subscription_name, topicname):
    print('Verifying the subsciption ')
    client = pubsub_v1.SubscriberClient()
    try:
        subscription = client.subscription_path(project_id, subscription_name)
        response = client.get_subscription(subscription)
        return response
    except google.api_core.exceptions.NotFound as er:
        print('Subscription doesnot exist. Creating New Subscription :{}'.format(subscription))
        createsubscription(topicname=topicname, project_id=project_id, subscriptionname=subscription_name)


def publishmessage(project_id, topic_id, message):
    # TODO(developer)
    checktopicavailable(project_id, topic_id)

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    futures = dict()

    def get_callback(f, data):
        def callback(f):
            try:
                print(f.result())
                futures.pop(data)
            except:  # noqa
                print("Please handle {} for {}.".format(f.exception(), data))

        return callback

    jsondata = message
    #print(jsondata)
    futures.update({jsondata: None})
    # When you publish a message, the client returns a future.
    future = publisher.publish(
        topic_path, data=jsondata.encode("utf-8")  # data must be a bytestring.
        )
    futures[jsondata] = future
    # Publish failures shall be handled in the callback function.
    future.add_done_callback(get_callback(future, jsondata))

    # Wait for all the publish futures to resolve before exiting.
    while futures:
        time.sleep(5)

    print("Published message with error handler.")




# topic_id='Mytopic'
# message = "{" + '"url"' + ":" + '"Sarath@gmail' + str(88) + '"' + "," + '"review"' + ":" + '"negative"' + "}"
# print(checktopicavailable('peaceful-branch-279707',topic_id))
# print(checksubscriptionavailable(project_id='peaceful-branch-279707', subscription_name='sub123', topicname=topic_id))
#
# publishmessage(project_id='peaceful-branch-279707', topic_id=topic_id, message=message)

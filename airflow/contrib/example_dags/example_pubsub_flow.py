# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
This example DAG demonstrates how the PubSub*Operators and PubSubPullSensor
can be used to trigger dependant tasks upon receipt of a Pub/Sub message.

NOTE: project_id must be updated to a GCP project ID accessible with the
      Google Default Credentials on the machine running the workflow
"""
from __future__ import unicode_literals
from base64 import b64encode

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.pubsub_operator import (
    PubSubTopicCreateOperator, PubSubSubscriptionCreateOperator,
    PubSubPublishOperator, PubSubTopicDeleteOperator,
    PubSubSubscriptionDeleteOperator
)
from airflow.contrib.sensors.pubsub_sensor import PubSubPullSensor
from airflow.utils import dates

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dates.days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

project_id = 'your-project-id'  # Change this to your own GCP project_id
topic = 'example-topic'
subscription = 'subscription-to-example-topic'
messages = [
    {'data': b64encode('Hello World')},
    {'data': b64encode('Another message')},
    {'data': b64encode('A final message')}
]
echo_template = '''
{% for m in task_instance.xcom_pull(task_ids='pull-messages') %}
    echo "AckID: {{ m.get('ackId') }}, Base64-Encoded: {{ m.get('message') }}"
{% endfor %}
'''

dag = DAG('pubsub-end-to-end', default_args=default_args,
          schedule_interval=None)
t1 = PubSubTopicCreateOperator(
    task_id='create-topic', project=project_id, topic=topic, dag=dag)
t2 = PubSubSubscriptionCreateOperator(
    task_id='create-subscription', topic_project=project_id, topic=topic,
    subscription=subscription, dag=dag)
t3 = PubSubPublishOperator(
    task_id='publish-messages', project=project_id, topic=topic,
    messages=messages, dag=dag)
t4 = PubSubPullSensor(
    task_id='pull-messages', project=project_id, subscription=subscription,
    ack_messages=True, dag=dag
)
t5 = BashOperator(task_id='echo-pulled-messages', bash_command=echo_template,
                  dag=dag)
t6 = PubSubSubscriptionDeleteOperator(
    task_id='delete-subscription', project=project_id,
    subscription=subscription, dag=dag
)
t7 = PubSubTopicDeleteOperator(
    task_id='delete-topic', project=project_id, topic=topic, dag=dag
)

t1 >> t2 >> t3
t2 >> t4 >> t5 >> t6 >> t7

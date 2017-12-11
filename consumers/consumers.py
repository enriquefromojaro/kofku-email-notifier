import json

import datetime
import os

from kafka import KafkaConsumer, TopicPartition

from emailers import GMailer


class EmailConsumer(object):
    def __init__(self, username, password, kafka_servers, **kwargs):
        self.emailer = GMailer(username, password)
        args = {
            'bootstrap_servers': kafka_servers
        }
        partition = kwargs.get('topic_partition')
        topics = kwargs.get('topics')
        if kwargs.get('value_deserializer'):
            args['value_deserializer'] = kwargs['value_deserializer']

        if topics and not partition:
            self.consumer = KafkaConsumer(*topics, **args)
        elif partition:
            self.consumer = KafkaConsumer(**args)
            self.consumer.assign([partition])
        else:
            raise ValueError('partition or topics need to be provided. None of them did')
    def run(self):
        for item in self.consumer:
            to, subject, text = self.get_email_info(item)
            print(to, subject, text)
            self.emailer.send_mail(to, subject, text)
        self.emailer.close()

    def get_email_info(self, item):
        raise NotImplementedError()


class HardProcessEmailConsumer(EmailConsumer):

    def __init__(self, username, password, kafka_servers, **kwargs):
        super(HardProcessEmailConsumer, self).__init__(username, password, kafka_servers, topic_partition=TopicPartition('processes', 1),
                         value_deserializer= lambda x: json.loads(x.decode()), **kwargs)

    def get_email_info(self, item):
        values = item.value
        email = values['email']
        if isinstance(email, str):
            email = [email]
        date = datetime.datetime.fromtimestamp(values['timestamp']).strftime('%A %d/%m/%Y a las %H:%M')
        subject= 'MSDE SisDist: {hard_state} del proceso {display_name} en el host {host_name}'.format(**values)
        text = ('El proceso {display_name} ha pasado a estado {state} el {date}.\n'
                'Nombre del proceso: {process}').format(date=date, **values)
        return email, subject, text

if __name__ == '__main__':
    em_con = HardProcessEmailConsumer('<email_here>', '<password here>', kafka_servers='172.17.0.1:9092')
    em_con.run()

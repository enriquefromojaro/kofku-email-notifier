import os
import smtplib
EMAIL_TEMPLATE= \
'''From: {from_}
To: {to}
Subject: {subject}

{text}
'''

class Emailer(object):

    def __init__(self, email, password, smtp_server, smtp_port):
        try:
            self.from_email = email
            self.server = smtplib.SMTP_SSL(smtp_server, smtp_port)
            self.server.ehlo()
            self.server.login(email, password)
        except:
            raise Exception('Unable to connect and login to email server')

    def send_mail(self, to, subject, text):
        if isinstance(to, str):
            to = [to]
        msg = EMAIL_TEMPLATE.format(from_=self.from_email, to=', '.join(to), subject=subject, text=text)

        self.server.sendmail(self.from_email, to, msg)

    def close(self):
        self.server.close()

class GMailer(Emailer):

    def __init__(self, email, password):
        super().__init__(email, password, 'smtp.gmail.com', 465)


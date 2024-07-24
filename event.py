#! /usr/bin/env python
# -*- coding: utf-8 -*-


from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email.utils import COMMASPACE, formatdate
from email.mime.base import MIMEBase
from email import encoders
import socket
import datetime
import sys
import os
from datetime import datetime, timedelta


def send_event(server_ip, server_port, from_addr, to_addr, subject, content, encoding='utf-8', priority='3', image=[],
               username=None, password=None,image_names = []):



	socket.setdefaulttimeout(1)
	#try:
	smtp = SMTP()
	try:
	    smtp.connect(server_ip, server_port)
	except:
	    return 1
	
	if username and password:
		smtp.login(username, password)

	event = ''
	#	for i in content.keys():
	#	    event = event + i + '=' + content[i] + '\n'
	#try:
	for i in content:
		try:
			event = event + i[0] + '=' + i[1] + '\n'
		except:
			event = event +i[0].decode(encoding) + '=' + i[1].decode(encoding) + '\n'

	text_subtype = 'plain'
	#print event
	try:
		event = event.decode('utf-8')
	except:
		pass
	subject = subject

	msg = MIMEMultipart()
	msg['Date'] = formatdate(localtime=True)
	msg['From'] = from_addr
	msg['To'] = to_addr
	msg['X-Priority'] = str(priority)
	msg['Sender'] = from_addr
	msg['Source-Name'] = 'Название сервера'
	msg['CharSet'] = 'cyrillic'

	if encoding:
		event = event
		text = MIMEText(event, text_subtype, _charset=encoding)
		text.add_header('Content-Disposition', 'inline')
		msg.attach(text)

		if image:
			for im in image:
				index = image.index(im)
				frame = MIMEBase('image', "jpeg")
				frame.set_payload(im)
				encoders.encode_base64(frame)
				if image_names:
					try:
						name = image_names[index]
						frame.add_header('Content-Disposition', 'attachment; filename="%s"' % name)
					except:
						frame.add_header('Content-Disposition', 'attachment; filename="cadr%i.jpg"' % index)
				else:
					frame.add_header('Content-Disposition', 'attachment; filename="cadr%i.jpg"' % index)

				msg.attach(frame)

		msg['Subject'] = Header(subject, encoding)

		try:
			smtp.sendmail(from_addr, to_addr, msg.as_string())
		except:
			return 1

		finally:
			smtp.close()
		return 0




if __name__ == '__main__':
	#image0 = open('/root/sample.jpg','r').read()
	#image1 = open('/root/sample1.jpg','r').read()
	begin_time = datetime.strftime(datetime.now() - timedelta(hours=4), '%y%m%d_%H_%M%S' )
	print(send_event('192.168.10.152', 31000, 'stein@localhost', 'xviewsion@Win7', 'export_period', [('job','in_office'),(
		'begin_time',begin_time)]))
	#print datetime.datetime.now()

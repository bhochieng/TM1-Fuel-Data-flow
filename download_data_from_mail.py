#!/usr/bin/python
import email
import getpass, imaplib
import os
import sys
import datetime
import time

detach_dir = '.'
if 'attach' not in os.listdir(detach_dir):
    os.mkdir('attach')
userName = ('yourusername@mail.com')
passwd = ('your_password')

senders_emails = ['sender@mail.com']
imapSession = imaplib.IMAP4_SSL('outlook.office365.com')
typ, accountDetails = imapSession.login(userName, passwd)
print('Starting to download files............')


try:
    #DECLARE FOR LOOP TO LOOP THROUGH ALL THE EMAILS.
    # for i in senders_emails:
    #     print(i)
    print('Checking new files')
    if typ != 'OK':
        print ('Not able to sign in!')
        raise
    
    imapSession.select("INBOX")
    for i in senders_emails:
        print('Checking for emails from',str(i).strip())#01-05-2022-11:41:10
        typ, data = imapSession.search(None, '(SEEN SINCE "08-May-2022" BEFORE "30-May-2022" FROM "' + str(i).strip() + '")')
        #typ, data = imapSession.search(None, 'ALL')
        if typ != 'OK':
            print ('Error searching Inbox.')
            raise
    
        # Iterating over all emails
        for msgId in data[0].split():
            typ, messageParts = imapSession.fetch(msgId, '(RFC822)')
            if typ != 'OK':
                print ('Error fetching mail.')
                raise

            emailBody = messageParts[0][1].decode('utf-8')
            #print (emailBody)
            message = email.message_from_string(emailBody)
            date_tuple = email.utils.parsedate_tz(message['Date'])
            local_date = datetime.datetime.fromtimestamp(email.utils.mktime_tz(date_tuple))
            formatted_date = local_date.strftime("%d-%m-%Y-%H:%M:%S")
            print(formatted_date)

            mail = email.message_from_string(emailBody)
            for part in mail.walk():
                if part.get_content_maintype() == 'multipart':
                    # print part.as_string()
                    continue
                if part.get('Content-Disposition') is None:
                    # print part.as_string()
                    continue
                fileName = part.get_filename() #gettiing the file name.
                print("This is the file name@@@@@@@@@@@@@@@@@@",fileName)
                dt = datetime.datetime.now()
                newdt = dt.strftime("%Y-%m-%d-%H-%M-%S%f")


                if bool(fileName):
                    #check the file name and decide the folder to dump the file.
                    #dynamically create a directory using python.
                    folder_name = fileName.replace('.txt','')+'_files'
                    # folder_name = folder_n+'_files'
                    parent_path= "./attachments/"
                    combo_path = os.path.join(parent_path, folder_name)
                    if not os.path.isdir(combo_path):
                        os.mkdir(combo_path)
                    filePath = os.path.join(combo_path, formatted_date+'-'+fileName)
                    # filePath = os.path.join(detach_dir, 'attachments', formatted_date+'-'+fileName)
                    print("###this is thef ile path",filePath)

                    #this code runs if its creating a new file
                    if not os.path.isfile(filePath):
                        print (fileName,"############")
                        fp = open(filePath, 'wb')
                        fp.write(part.get_payload(decode=True))
                        fp.close()
    imapSession.close()
    imapSession.logout()
    raise Exception(senders_emails,'def')
except Exception as e:
    print ('Not able to download all attachments.', str(e))


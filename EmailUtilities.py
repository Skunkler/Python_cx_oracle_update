import smtplib, datetime, time, os, ssl
import subprocess


from datetime import datetime, date, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
#from email.MIMEBase import MIMEBase
#from email.MIMEText import MIMEText
#from email import Encoders



class EmailUtil:
    __email_IP = "Smtp-relay.rcc.org"
    
    def __init__(self, email_From, email_To):
        self._from = email_From
        self._to = email_To

    def sendMessage(self, subject, Msg, email_Username=False, email_Password = False):
        
        #msg = MIMEText("This is a test message for the email utilities class")
        if type(Msg) == str:
            msg = MIMEText(Msg)
        else:
            msg = Msg
        msg["Subject"]=subject
        msg["From"] = self._from
        msg["To"] = ", ".join(self._to)
        print(msg.as_string())
                
        server = smtplib.SMTP(EmailUtil.__email_IP)

      
        if email_Username == False:
            
            server.sendmail(self._from, self._to, msg.as_string())
            server.quit()
            
        #    except Exception, e:
               
         #       print(str(e))
        else:
            try:
                server.login(username, password)
                server.sendmail(self._from, self._to, msg)
                server.quit()
                
            except:
                print(" Authentication may have failed for this SMTP server.\n"
                  " Please verify the username and password.\n If username"
                  " and password are correct consider contacting your system"
                  " administrator to verify the IP address and PORT numbers"
                  " are correct.\n Also ensure that authentication is required,"
                  " try leaving username and password blank and try again.")

        


    

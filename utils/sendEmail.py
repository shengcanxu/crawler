from email.mime.text import MIMEText
from email.header import Header
from email.mime.multipart import MIMEMultipart
import smtplib
import datetime

# https://www.cnblogs.com/potato-find/p/13290419.html
class Email(object):
    def __init__(self):
        self.smtp = 'smtp.qq.com'
        self.username = '147148940@qq.com'
        self.password = 'qnymumhmtfizbjcf'
        self.subject = 'Test/测试邮件'
        self.sender = '147148940@qq.com'
        self.receiver = []
        self.cc = []

    def message_init(self, html):
        message = MIMEMultipart()
        message['subject'] = Header(self.subject, 'utf-8')
        message['From'] = self.sender
        message['To'] = ', '.join(self.receiver)
        message['Cc'] = ', '.join(self.cc)

        # 解决乱码, html是html格式的str
        context = MIMEText(html, _subtype='html', _charset='utf-8')
        # 邮件正文内容
        message.attach(context)
        return message

    def sendEmail(self, html):
        message = self.message_init(html)
        smtpObj = smtplib.SMTP_SSL(host=self.smtp, port=465)
        smtpObj.login(self.username, self.password)
        smtpObj.sendmail(self.sender, self.receiver + self.cc, message.as_string())
        smtpObj.quit()


def sendRoutineEmail(content):
    email = Email()
    email.subject = '数据更新结果' + datetime.datetime.now().strftime("%Y%m%d %H:%M:%S")
    email.receiver.append('cano1984@163.com')
    email.sendEmail(content)


if __name__ == '__main__':
    strs = "This is a test email / 这是一封测试邮件"
    sendRoutineEmail(strs)

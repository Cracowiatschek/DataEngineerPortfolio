from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from datetime import datetime

def send_message(ready_template, sender, receiver, subject, sg_api_key):
    #config message content to api
    message = Mail(
        from_email = sender,
        to_emails = receiver,
        subject = subject,
        html_content = ready_template
    )
    message.add_cc(sender)
    #materialize email client
    sg = SendGridAPIClient(sg_api_key)
    #send message
    response = sg.send(message)

    return response.status_code
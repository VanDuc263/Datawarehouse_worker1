import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_error_mail(subject, message, to_email):
    sender_email = "ducvan26324@gmail.com"
    sender_password = "bacc aejj zqey ujhn"  # dùng app password

    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(message, "plain", "utf-8"))

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, to_email, msg.as_string())
        server.quit()
        print("Email sent!")
    except Exception as e:
        print(f"Error sending email: {e}")


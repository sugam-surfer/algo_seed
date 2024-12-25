import boto3
import argparse
from botocore.exceptions import ClientError

# Set up the SES client
ses_client = boto3.client('ses', region_name='ap-south-1')

def send_email(recipients):
    # Define the email parameters
    SENDER = "sugamkuchhal2023@gmail.com"  # Verified email address
    SUBJECT = "Test Email from SES"
    BODY_TEXT = "This is a test email sent through Amazon SES using Python."
    BODY_HTML = """<html>
      <head></head>
      <body>
        <h1>This is a test email sent through Amazon SES using Python.</h1>
      </body>
    </html>"""
    CHARSET = "UTF-8"

    try:
        # Send the email
        response = ses_client.send_email(
            Source=SENDER,
            Destination={
                'ToAddresses': recipients,  # Pass recipients as a list
            },
            Message={
                'Subject': {
                    'Data': SUBJECT,
                    'Charset': CHARSET
                },
                'Body': {
                    'Text': {
                        'Data': BODY_TEXT,
                        'Charset': CHARSET
                    },
                    'Html': {
                        'Data': BODY_HTML,
                        'Charset': CHARSET
                    },
                },
            },
        )
        print("Email sent! Message ID:"),
        print(response['MessageId'])
    except ClientError as e:
        print("Error sending email:", e.response['Error']['Message'])

# Main function to handle command-line arguments
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Send an email using AWS SES.")
    parser.add_argument(
        "--recipients",
        nargs="+",  # Accept multiple recipients
        required=True,
        help="List of recipient email addresses (space-separated)",
    )
    args = parser.parse_args()
    
    # Pass recipients to the send_email function
    send_email(args.recipients)

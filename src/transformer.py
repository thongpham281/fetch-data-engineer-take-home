from datetime import datetime


def transform(message):
    # Change from timestamp to readable format
    message['timestamp'] = datetime.fromtimestamp(message['timestamp']).strftime('%Y-%m-%d %H:%M:%S')

    # Add more operations if needed

    return message

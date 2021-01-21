import json
import time
from datetime import datetime

import requests


class Request:

    def __init__(self, endpoint, content_type):
        self.endpoint = endpoint
        self.content_type = content_type

    @staticmethod
    def send_request(input_msg, endpoint, content_type, print_msg=False, headers=None):
        msg = input_msg.encode('utf-8')
        session = requests.session()
        session.headers = {"Content-Type": f"{content_type}; charset=utf-8"}
        session.headers.update({"Content-Length": str(len(msg))})
        if headers:
            session.headers.update(headers)
        response = session.post(url=endpoint, data=msg, verify=False)
        print("\n", datetime.now(), f'Message sent to {endpoint}')
        if print_msg:
            print(input_msg)
        return response.text

    def send(self, input_msg, print_msg=False, oko_url=None):
        msg = input_msg.encode('utf-8')
        session = requests.session()
        session.headers = {"Content-Type": f"{self.content_type}; charset=utf-8"}
        session.headers.update({"Content-Length": str(len(msg))})
        if oko_url:
            session.headers.update({'OkoSystemUrl': oko_url})
        response = session.post(url=self.endpoint, data=msg)
        print("\n", datetime.now(), f'Message sent to {self.endpoint}')
        if print_msg:
            try:
                input_msg = json.loads(input_msg)
            except json.decoder.JSONDecodeError:
                input_msg = input_msg
            print(json.dumps(input_msg, indent=4, ensure_ascii=False))
        if response.status_code != 200:
            print(response.status_code)
        if response.content:
            print(response.content.decode('utf-8'))

    def send_requests_with_delay(self, msgs, delay=30, print_msg=False):
        for msg in msgs:
            self.send(msg, print_msg=print_msg)
            if msgs.index(msg) != len(msgs) - 1:
                time.sleep(delay)

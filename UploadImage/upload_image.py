import requests
import os
import json


class Uploader(object):

    def __init__(self, image_path, userid, tags):
        self.path = image_path
        self.userid = userid
        self.tags = tags

    def get_image_name(self):
        extension = self.path.split('.')[1]
        image_filename = os.path.basename(self.path)
        multipart_form_data = {
            "image": (image_filename, open(self.path, 'rb'), "image/" + extension),
            "authtoken": "eyJhbGciOiJIUzI1NiJ9.eyJ1c2VySWQiOjYzLCJsb2dpbklkIjoxNDYzLCJwbGF0Zm9ybSI6IkFQUCIsImlhdCI6MTU0MDE5MDUzNCwiZXhwIjoxNTQ1Mzc0NTM0fQ.kqBz35L71uqEndAg3g6Ity5jMUu9sf_o0UywzAKUjS8"
        }
        response = requests.post(os.environ['IMAGE_URL'], {'userId': self.userid}, files=multipart_form_data)
        print(response.status_code)
        data = json.loads(response.content.decode())
        image_name = data['data']['imageName']
        return image_name

    def send_image(self):
        image_name = self.get_image_name()
        multipart_form_data = {
            "authToken": "eyJhbGciOiJIUzI1NiJ9.eyJ1c2VySWQiOjIyNCwibG9naW5JZCI6ODk5LCJwbGF0Zm9ybSI6IkFQUCIsImlhdCI6MTUzNDg1NzYzNSwiZXhwIjoxNTQwMDQxNjM1fQ.TFEYJ8sOhUjC9t-gfE4Q4A8pKXvmZw-lQ57OaVG0nC4",
            "userId": self.userid,
            "contentLink": [str(image_name)],
            "language": 1,
            "languageName": "HINDI",
            "published": 'true',
            "tags": self.tags,
            "thumbnail1": image_name,
            "title": "",
            "type": "IMAGE"}

        response = requests.post(os.environ['API_URL'], data=json.dumps(multipart_form_data),
                                 headers={'Content-Type': 'application/json'})
        print(response.status_code)
        data = json.loads(response.content.decode())
        status = data['data']['success']
        return image_name, status

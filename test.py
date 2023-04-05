import requests
from requests import Session
from functools  import partial
from six.moves.urllib.parse import urlparse
from bs4 import BeautifulSoup
session = Session()


class URLFetch:

    def __init__(self, url, method='get', json=False, session=None,
                 headers = None, proxy = None):
        self.url = url
        self.method = method
        self.json = json

        if not session:
            self.session = requests.Session()
        else:
            self.session = session

        if headers:
            self.session.headers.update(headers)
        if proxy:
            self.update_proxy(proxy)
        else:
            self.update_proxy('')

    def set_session(self, session):
        self.session = session
        return self

    def get_session(self, session):
        self.session = session
        return self

    def __call__(self, *args, **kwargs):
        u = urlparse(self.url)
        self.session.headers.update({'Host': u.hostname})
        url = self.url%(args)
        if self.method == 'get':
            return self.session.get(url, params=kwargs, proxies = self.proxy )
        elif self.method == 'post':
            if self.json:
                return self.session.post(url, json=kwargs, proxies = self.proxy )
            else:
                return self.session.post(url, data=kwargs, proxies = self.proxy )

    def update_proxy(self, proxy):
        self.proxy = proxy
        self.session.proxies.update(self.proxy)

    def update_headers(self, headers):
        self.session.headers.update(headers)

url = "https://www.nseindia.com/api/historical/fo/derivatives/meta?&from=30-03-2023&to=02-04-2023&expiryDate=27-Apr-2023&instrumentType=FUTSTK&symbol=BATAINDIA"

payload={}
# headers = {"Referrer Policy": "strict-origin-when-cross-origin",
#     "referer":"https://www.nseindia.com",
#   'sec-ch-ua': '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
#   'sec-fetch-user': '?1',
#   'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
#   'Cookie': 'ak_bmsc=4257D20D58572B8206EEF766991A2103~000000000000000000000000000000~YAAQlidzaOrPdxiHAQAAyCkFQhMTFvfhCId7I22Aw33a7tkbDAVTrP6HhgV72joyzmkdtc5/4ujtSABSnahEFKkl+dVGs8Wl4XCIbrLHUYXg6sXtRnX5lSSpyDQmxiGvUxoyq+yMBkgD0KzI46OHWEvgjuFMXdPC31MZ4EOvo73/SaAuZKIynQSQ+B52uWuObcsZLT9SAZoJLQEFLQ267uNEZSv/yxR+if3P34ZZlo4Fep1lrbpfAWqs2HqkN3ydlJFSUWOu1EMiw3L8X6kyUdOC9DaoKri6eLVnWQ7eZoOG8iPOGxwmdxBYXE2FQUTsMxZqyqIEoXFIjL3aqmelhIKF/ccO/Y8ocrEvZTOh7odRIy5Y0i7QWH4jJqgvS5FIARWss15qdBaIn0oPht2xNw==; bm_sv=2224DE5E2BC9F4B934C8CE2F96ACC080~YAAQlidzaDcbeBiHAQAAgvgVQhND110cacsEv/wVlUaJ+tM7UKc3mwV+PLfQjkb5EoD+9eHjUDDDDfYAoaZ3yqDX0R7j1eTSeehDhhcdBNuqOyZk+1YsedCqlJSez9OfDiRHGNrZ/uWQ4LkQ/MLnmUz4J5rAJu8nKDPlO3+4Bnvn+dY+UaKQjo/H8r5UQmQGji0eqANddOTobOSTyBXaxsR4mVIQNyNxwDG8/wf5r9BPRhc4LCMBR1DXffVzl5Ikdhya~1'
# }
headers = {'Accept': '*/*',
           'Accept-Encoding': 'gzip, deflate, sdch, br',
           'Accept-Language': 'en-GB,en-US;q=0.8,en;q=0.6',
           'Connection': 'keep-alive',
           'Host': 'www.nseindia.com',
           'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
           'X-Requested-With': 'XMLHttpRequest',
           'Referer': 'https://www.nseindia.com/get-quotes/derivatives?symbol=BATAINDIA'
           }
URLFetchSession=partial(URLFetch, session=session,headers=headers)
derivative_history_url = partial(
    URLFetchSession(
        url='https://www.nseindia.com/api/historical/fo/derivatives/meta?&from=30-03-2023&to=02-04-2023&expiryDate=27-Apr-2023&instrumentType=FUTSTK&symbol=BATAINDIA',
        headers = {**headers, **{'Referer': 'https://www.nseindia.com/get-quotes/derivatives?symbol=BATAINDIA'}}
        ),
    segmentLink=9,
    symbolCount='')
try:
    resp=derivative_history_url()
    # bs = BeautifulSoup(resp.text, 'lxml')
    # response = requests.request("GET", url, headers=headers, data=payload)

    # print(response.text)
    print(resp.text)
except Exception as e:
    print("error",e)

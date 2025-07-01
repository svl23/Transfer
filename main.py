from urllib.parse import urlencode
from requests import post, get
from json import loads, dumps
from time import sleep
from io import StringIO
import os
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta
from gspread import service_account

def download_query(host, counter_id, token, request_id, part_list):
    header_dict = {
        "Authorization": f"OAuth {token}",
        "Content-Type": "application/x-yametrika+json"
    }
    tmp_df_list = []

    for part_num in map(lambda x: x["part_number"], part_list):
        url = f"{host}/management/v1/counter/{counter_id}/logrequest/{request_id}/part/{part_num}/download"

        r = get(url, headers=header_dict)
        assert r.status_code == 200, f"Загрузка не удалась, {r.text}"

        tmp_df = pd.read_csv(StringIO(r.text), sep="\t")
        tmp_df_list.append(tmp_df)

    return pd.concat(tmp_df_list)

def wait_query(host, counter_id, token, request_id):
    header_dict = {
        "Authorization": f"OAuth {token}",
        "Content-Type": "application/x-yametrika+json"
    }
    url = f"{host}/management/v1/counter/{counter_id}/logrequest/{request_id}"
    status = "created"

    while status == "created":
        sleep(60)
        print("trying")

        r = get(url, headers=header_dict)
        assert r.status_code == 200, f"Ожидание не удалось, {r.text}"

        status = loads(r.text)["log_request"]["status"]
        print(dumps(loads(r.text)["log_request"], indent=4))

    return loads(r.text)["log_request"]["parts"]

def create_query(host, counter_id, token, source, start_date, end_date, api_field_list):
    header_dict = {
        "Authorization": f"OAuth {token}",
        "Content-Type": "application/x-yametrika+json"
    }
    url_params = urlencode([
        ("date1", start_date),
        ("date2", end_date),
        ("source", source),
        ("fields", ",".join(sorted(api_field_list, key=lambda s: s.lower())))
    ])
    url = f"{host}/management/v1/counter/{counter_id}/logrequests?{url_params}"

    r = post(url, headers=header_dict)
    assert r.status_code == 200, f"Запрос не создан, {r.text}"

    return loads(r.text)["log_request"]["request_id"]

host = "https://api-metrika.yandex.ru"
counter_id = "48656855"
token = "y0__xCZoJ1qGOCtFiCf2veDE1S1MRrGv5kfpIedBGoYXKD5R6v1"
source = "visits"
start_date = "2025-06-26"
yesterday = (date.today() - relativedelta(days=1)).strftime("%Y-%m-%d")
end_date = os.getenv("END_DATE", default=yesterday)
api_field_list = [
    "ym:s:date",
    "ym:s:dateTime",
    "ym:s:pageViews",
    "ym:s:visitDuration",
    "ym:s:startURL",
    "ym:s:deviceCategory",
    "ym:s:operatingSystemRoot",
    "ym:s:clientID",
    "ym:s:browser",
    "ym:s:browserEngine",
    "ym:s:screenOrientation",
    "ym:s:screenWidth",
    "ym:s:screenHeight",
    "ym:s:physicalScreenWidth",
    "ym:s:physicalScreenHeight",
    "ym:s:lastTrafficSource",
    "ym:s:purchaseRevenue",
    "ym:s:purchaseID",
    "ym:s:bounce",
    "ym:s:isNewUser",
    "ym:s:goalsID"
]

request_id = create_query(host, counter_id, token, source, start_date, end_date, api_field_list)
part_list = wait_query(host, counter_id, token, request_id)
print(part_list)
print("грузим")
data= download_query(host, counter_id, token, request_id, part_list)
import gspread

creadentials = {
  "type": "service_account",
  "project_id": "focused-bridge-464413-j8",
  "private_key_id": "405dcdba3bfba7707bb857a722333a22d9a030d3",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCkVJUq7B7U/qN3\nt02LK1piwMzQpsLTqjPgDsdIKemhPThiI+FOwKVZ/HTEjR9MCZ38hxKBY7Hn5Ddp\nITRegHU7fV+wgMgeEWDmHtU7wACz3OovaMxgQIcX3GnfyzahwKDt+bZHu7CEnL8x\nGrT88iSFfPtx2v/UIhaITMPFF8+kxYPN7xoK98uDZSwyNW+F4jseat+nSELb8GrU\nMRlsjaECWKOzKCKBanxXEAXUsHeU+j4cfdxLM2BiXEA59o0pSdFt+W6Dv00AnSz9\nvApoM+mdjENImw720HYa+kP098T2r8oYHxl4pP4ieQaFJ94MSg5tecHfA7G3vLMe\nixbzPgyZAgMBAAECggEAGiA7GaMx668S53pEYSHisYdG3iKvyt2+eEV2jWnhyPlm\nDhmiPSuySNzdkPosGvdUdMziC6GBVrAZLHpTocqj2MdqqocZnx4oMlh9541JD090\ntADiAqjU/DLWFPcb7RomURoS+X3vkcXR58lVM6R+W0gEOepqzfEgH/SvE0C2idLT\n0k7586AXKHz4h5+jF4pvAm2peagmD7yXwBpTIG5XT4a6eqt5w2rGPvOr3l/fUoji\nQ5mBwjmycs0L6/0++vg1Z+ej/E63XsIYloyV9xXd/avQPp+2fSJbAyh1DMq+bged\nqCqsJhbs8K5xY+isVe4js17VNim3iNNxopzfuODAuQKBgQDYceaih+hjx9uQHmpX\n+FBGzpgjuaqlZT3LgIOjII8HcQkK1bydUVBQagcfrhaZM62+OlMPPDE/ZBZtluD8\nmaXZ37pI1+AvOjlJqPu+0ciytLOJPRk47ZGnfdpJCrL5huUno4ze4/Qy6lgvUooy\nr0JjNTVSdJgwHW/68smFvoF+AwKBgQDCXJIEoCZRU8B9YiJDnlac+EoUaYnFCkOD\nrVKKU8FYgqI3fEGPmZMxogA69tw7EDrnheOUvCwxKg1Po39vsqoAfwlwwfeQGkBk\nhOUz5OHX5tH/KDBzQu4/YmIy1nyrP2u82TvAiFSIqU/AKLiPHBroZN9wfRpV2vWi\nDonVupSmMwKBgQCC7FLP2VmukYO0J1G4KJGWYv0QDwyzwwuf/vqhP96EZ9FKZoMS\nvc7Q9XGdrhYpUWM0/96iBozbhVUzsOIiqWXy5iuuSDZpGtTwQ3ETuZ2myzsWNoj9\nMGa8Y5oYOIqN+RS/52QcptJdwfbZmRnTEsQbcmHwoGo2IHhN2XIgnl2BtQKBgDWr\nfEK8ungAqYm1/IIxifdEdKhYUEvRvrzwsncyE5TOVd7+d9ggb03oTyn5Gu1QWQOv\nrB1ZLNbSpuRT4I/GhT0n10KEvUjUhu166RPAY0DMIzbTMTXSyJlGIJmrMrfTBnwa\nfG549ubZ4gTM+dHC0AdWO4EDeq80jFmYrCURpZSTAoGAbQqJ5xBuVUF56kUnpHxc\nTCUpZrPnLge6gBmuK/nDSPIGnvTXj7xVgaHvokE6BHzf8vHQ329qbxQJCt6ER9qD\nvjhcu6kR+qSEy8V7bWMQ2zKk1GagxT3t2QzLHxTMy8Q5ExDAQIJtz74TcbLlumhV\nMy5zI75vg0rGSj37LkX5N5w=\n-----END PRIVATE KEY-----\n",
  "client_email": "svl-863@focused-bridge-464413-j8.iam.gserviceaccount.com",
  "client_id": "105816078901843756063",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/svl-863%40focused-bridge-464413-j8.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}


# gc = gspread.api_key("AIzaSyBNez78ZhrYQ7QJ0z-cTY-ABKzRK-R9wJY")
gc = gspread.service_account_from_dict(creadentials)
# gc = gspread.oauth()
sh = gc.open("Visits") 
sh.sheet1.update([data.columns.values.tolist()]
                    + data.fillna("Unknown").values.tolist())

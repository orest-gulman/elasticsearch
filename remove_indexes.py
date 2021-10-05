import requests
from requests.auth import HTTPBasicAuth
import urllib3
import smtplib
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
import sys

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

admin = "admin"
password = "password"

url = "https://domain.local:9200"

retention_3_month = ((datetime.now() - relativedelta(months=3)).strftime('%Y.%m'))
retention_1_year = ((datetime.now() - relativedelta(years=1)).strftime('%Y.%m'))

def send_mail(msg):
    sender = 'sender@domain.com'
    receivers = 'receivers@domain.com'
    subject = "ElasticSearch cleanup report"
    message = 'From: {}\nTo: {}\nSubject: {}\n\n{}'.format(sender,receivers,subject,msg)
    s = smtplib.SMTP('192.168.0.100')
    s.sendmail(sender, receivers, message)

try:
    r_get = requests.get(url + '/_cat/indices?h=index,store.size&format=json', auth = HTTPBasicAuth(admin, password), verify=False, timeout=60, headers={'Connection':'close'})
    if r_get.status_code == 200:
        try:
            #indexes_all = (r.content.decode().split())
            indexes = json.loads(r_get.content.decode())
        except Exception as e:
            send_mail(str(e))
            sys.exit()
    else:
        send_mail(r_get.content.decode())
        sys.exit()
except requests.exceptions.ConnectionError:
    send_mail(f"{url} - ConnectionError")
    sys.exit()
except requests.exceptions.ReadTimeout:
    send_mail(f"{url} - Timeout")
    sys.exit()
except Exception as e:
    send_mail(str(e))
    sys.exit()

try:
    indexes_all = [x for x in indexes if not x["index"].startswith('.')]
    indexes_index_1 = [x for x in indexes if x["index"].startswith('index_1')]
    indexes_index_2 = [x for x in indexes if x["index"].startswith('index_2')]

except Exception as e:
    send_mail(str(e))
    sys.exit()

# Get old indexes
def get_indexes_to_delete(indexes, retention):
    indexes_to_delete = []
    for i in indexes:
        if i["index"][-7:] < retention:
            indexes_to_delete.append(i)
    return indexes_to_delete

indexes_index_1_to_delete = get_indexes_to_delete(indexes_index_1, retention_3_month)
indexes_index_2_to_delete = get_indexes_to_delete(indexes_index_2, retention_3_month)
indexes_all_to_delete = get_indexes_to_delete(indexes_all, retention_1_year)

# Deleting indexes
def delete_index(index):
    report = ""
    for i in index:
        try:
            r_del = requests.delete(url + i['index'], auth=HTTPBasicAuth(admin, password), verify=False, timeout=60, headers={'Connection':'close'})

            if r_del.status_code == 200:
                report += f"INDEX: {i['index']} with size: {i['store.size']} is DELETED.\n"
            else:
                report += f"INDEX: {i['index']} with size: {i['store.size']} is NOT DELETED! {r_del.content.decode()}.\n"
                send_mail(r_del.content.decode())
        except requests.exceptions.ConnectionError:
            send_mail(f"{url} + {i['index']} - ConnectionError")
        except requests.exceptions.ReadTimeout:
            send_mail(f"{url} + {i['index']} - ReadTimeout")
        except Exception as e:
            send_mail(str(e))
    if report:
        report += "\n"
        return report

report_full = ""
report_full += delete_index(indexes_index_1_to_delete)
report_full += delete_index(indexes_index_2_to_delete)
report_full += delete_index(indexes_all_to_delete)
if report_full:
    send_mail(report_full)

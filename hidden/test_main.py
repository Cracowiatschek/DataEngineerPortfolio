import requests
import json
import psycopg

connection = psycopg.connect(dbname = "", user="", password="", host="..", port=5432)
try:
    cursor = connection.cursor()
    cursor.execute("SELECT 1")
    c = cursor.fetchall()
    print(c)
except Exception:
    pass


data_url = {
    "pilots": "https://api.drony.gov.pl/api/public/statistics/pilots?year",
    "operators": "https://api.drony.gov.pl/api/public/statistics/operators?year=",
    "sts-declarations": "https://api.drony.gov.pl/api/public/statistics/sts-declarations?year=",
    "missions": "https://api.drony.gov.pl/api/public/statistics/missions?year="
}

req = requests.get("https://api.drony.gov.pl/api/public/statistics/operators?year=2025")
req = json.loads(req.content.decode('utf-8'))

t = 0
for i in req:
    if i == "monthlyBreakdown":
        for j, x in enumerate(req[i]):
            print(i, req[i][j]['month'], req[i][j]['registered'])
            t+= req[i][j]['registered']
    else:
        print(i, req[i])


print(t)
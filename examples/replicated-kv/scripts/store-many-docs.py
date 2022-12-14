import time
import random
import concurrent.futures
import requests
import string

session = requests.session()
def load_url(url, timeout):
    random_name = ''.join(random.sample(string.ascii_lowercase,16))
    r = session.post(url, data="{\"name\": " + f"{random_name}" + "\"}")
    r.raise_for_status()
    return r.status_code


out = []
CONNECTIONS = 10
TIMEOUT = 5

urls = []
doc_ids = []
keyspaces = []
endpoints = ["http://127.0.0.1:8000", "http://127.0.0.1:8002", "http://127.0.0.1:8004"]

for i in range(20_000):
    doc_ids.append(random.randint(1, 1000))

for i in range(100):
    keyspaces.append(''.join(random.sample(string.ascii_lowercase,16)))


for i in range(20_000):
    url_to_query = random.choice(endpoints)
    doc_id = random.choice(doc_ids)
    keyspace = random.choice(keyspaces)
    url = format(f"{url_to_query}/{keyspace}/{doc_id}")
    urls.append(url)

print("beginning executor")
with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
    future_to_url = (executor.submit(load_url, url, TIMEOUT) for url in urls)
    time1 = time.time()
    for future in concurrent.futures.as_completed(future_to_url):
        try:
            data = future.result()
        except Exception as exc:
            data = str(type(exc))
        finally:
            out.append(data)

            print(str(len(out)),end="\r")

    time2 = time.time()
print(f'Took {time2-time1:.2f} s')

#start = time.perf_counter()
#
#for i in range(100_000):
#    if (i / 1_000).is_integer():
#        print(f"Added {i} docs")
#
#    doc_id = random.randint(1, 12345678791231)
#    rand_data = random.randint(696969, 696969696969)
#    r = session.post(f"http://127.0.0.1:8000/my-keyspace/{doc_id}", data="{\"name\": " + f"{rand_data}" + "\"}")
#    r.raise_for_status()
#
#total_time = time.perf_counter() - start
#print(f"Took {total_time:.2f} seconds!")

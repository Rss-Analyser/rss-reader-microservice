import requests
import time

BASE_URL = "http://0.0.0.0:5002"

def test_start_rss_read():
    response = requests.post(f"{BASE_URL}/start-rss-read", json={
        "days_to_crawl": 1
    })

    assert response.status_code == 200
    assert response.json()["message"] == "RSS reading started."

    print("Test start-rss-read PASSED")

def test_status():
    while True:
        response = requests.get(f"{BASE_URL}/status")

        assert response.status_code == 200
        data = response.json()

        print(f"Status: {data['status']}")
        print(f"Entries crawled: {data['entries_crawled']}")
        print(f"rss_feeds crawled: {data['rss_feeds_crawled']}")
        
        # Print times:
        start_time = data['start_time']
        runtime = data['runtime']
        current_runtime = data['current_runtime']

        if start_time:
            print(f"Start Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")

        if runtime:
            print(f"Runtime: {runtime} seconds")

        if current_runtime:
            print(f"Current Runtime: {current_runtime} seconds")

        if data['status'] == "idle":
            break

        time.sleep(1)

    print("Test status PASSED")


if __name__ == "__main__":
    test_start_rss_read()
    test_status()

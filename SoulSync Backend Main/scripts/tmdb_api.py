from dotenv import load_dotenv
import os
import requests
import json
import pymongo
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter

load_dotenv()

pid = os.getpid()
print(f"The PID of this process is: {pid}")


def api_request(movie_id):
    tmdb_api_key = os.environ["TMDB_API_KEY"]
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={tmdb_api_key}"
    headers = {"accept": "application/json"}
    response = requests.get(url, headers=headers)
    if response.status_code == 429:
        print("Rate Limited")
        exit(1)
    return response.json()


def process_movie(movie):
    global movie_collection
    data = api_request(movie["id"])
    data = clean_data(data)
    res = movie_collection.update_one({"_id": data["_id"]}, {"$set": data}, upsert=True)
    return "update" if res.matched_count > 0 else "insert"


def main():
    global movie_collection
    db = get_mongo_db()
    movie_collection = db.get_collection("movies")
    results = Counter()
    total_processed = 0
    batch_size = 2000  # Adjust this value based on your available memory

    with ThreadPoolExecutor(max_workers=20) as executor:
        for batch in get_movies_batch(batch_size):
            future_to_movie = {
                executor.submit(process_movie, movie): movie for movie in batch
            }
            for future in as_completed(future_to_movie):
                try:
                    result = future.result()
                    results[result] += 1
                    total_processed += 1
                    if total_processed % 100 == 0:
                        print(
                            f"Processed: {total_processed}, Inserted: {results['insert']}, Updated: {results['update']}"
                        )
                except Exception as exc:
                    print(f"An exception occurred: {exc}")

    print(
        f"Final count - Processed: {sum(results.values())}, Inserted: {results['insert']}, Updated: {results['update']}"
    )


def get_mongo_db():
    mongo_connection = os.environ["MONGODB_CONNECTION"]
    client = pymongo.MongoClient(mongo_connection)
    return client["recommendation"]


def clean_data(data):
    data["poster_path"] = tmdb_image(data["poster_path"])
    data["backdrop_path"] = tmdb_image(data["backdrop_path"])
    for i in range(len(data["production_companies"])):
        data["production_companies"][i]["logo_path"] = tmdb_image(
            data["production_companies"][i]["logo_path"]
        )
    data["_id"] = data["id"]
    del data["id"]
    del data["belongs_to_collection"]
    return data


def get_movies_batch(batch_size):
    with open("data/tmdb_movie_ids.json", "r") as f:
        batch = []
        for line in f:
            movie = json.loads(line)
            if not movie["adult"]:
                batch.append(movie)
                if len(batch) == batch_size:
                    yield batch
                    batch = []
        if batch:  # Yield the last batch if it's not empty
            yield batch


def tmdb_image(img_path):
    return f"https://image.tmdb.org/t/p/original{img_path}"


if __name__ == "__main__":
    main()

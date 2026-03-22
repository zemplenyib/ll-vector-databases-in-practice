import time

BATCH_SIZE=100

def insert_in_batch(collection, objs, name: str):
    for i in range(0, len(objs), BATCH_SIZE):
        batch = objs[i:i+BATCH_SIZE]
        response = collection.data.insert_many(batch)

        print(f"Insertion complete with {len(response.all_responses)} objects for '{name}' collection.")
        print(f"Insertion errors: {len(response.errors)}.")

        time.sleep(60)
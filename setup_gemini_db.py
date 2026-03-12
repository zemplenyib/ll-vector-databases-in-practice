"""
Setup script: Creates all collections and imports all data from movies_data.csv
into your own Weaviate Cloud instance using Gemini (text2vec-google) as the vectorizer.

Prerequisites:
  - A Weaviate Cloud sandbox with Google module enabled
  - .env file with MY_WEAVIATE_URL, MY_WEAVIATE_KEY, and GEMINI_APIKEY

Usage:
  python setup_gemini_db.py
"""

import utils
import weaviate.classes as wvc
from weaviate.util import generate_uuid5
import pandas as pd
import time
import sys
import os

# ============================================================
# Connect
# ============================================================
client = utils.connect_to_demo_db_goog()

# ============================================================
# Delete existing collections (fresh start)
# Uncomment the lines below if you want to start from scratch
# ============================================================
# for name in ["Movie", "Review", "Synopsis"]:
#     client.collections.delete(name)
#     print(f"Deleted '{name}' (if it existed)")

# ============================================================
# Create collections with Gemini vectorizer (skips if they already exist)
# ============================================================

existing = [c.name for c in client.collections.list_all().values()]

if "Review" not in existing:
    client.collections.create(
        name="Review",
        vectorizer_config=wvc.config.Configure.Vectorizer.text2vec_google_aistudio(),
        generative_config=wvc.config.Configure.Generative.google_gemini(),
        properties=[
            wvc.config.Property(
                name="body",
                data_type=wvc.config.DataType.TEXT,
            ),
        ],
    )
    print("Created 'Review' collection")
else:
    print("'Review' collection already exists, skipping")

if "Movie" not in existing:
    client.collections.create(
        name="Movie",
        vectorizer_config=wvc.config.Configure.Vectorizer.text2vec_google_aistudio(),
        generative_config=wvc.config.Configure.Generative.google_gemini(),
        properties=[
            wvc.config.Property(
                name="title",
                data_type=wvc.config.DataType.TEXT,
            ),
            wvc.config.Property(
                name="description",
                data_type=wvc.config.DataType.TEXT,
            ),
            wvc.config.Property(
                name="movie_id",
                data_type=wvc.config.DataType.INT,
            ),
            wvc.config.Property(
                name="year",
                data_type=wvc.config.DataType.INT,
            ),
            wvc.config.Property(
                name="rating",
                data_type=wvc.config.DataType.NUMBER,
            ),
            wvc.config.Property(
                name="director",
                data_type=wvc.config.DataType.TEXT,
                skip_vectorization=True,
            ),
        ],
        references=[
            wvc.config.ReferenceProperty(
                name="hasReview",
                target_collection="Review",
            )
        ],
    )
    print("Created 'Movie' collection")

    # Add hasSynopsis reference after Synopsis is created
else:
    print("'Movie' collection already exists, skipping")

if "Synopsis" not in existing:
    client.collections.create(
        name="Synopsis",
        vectorizer_config=wvc.config.Configure.Vectorizer.text2vec_google_aistudio(),
        generative_config=wvc.config.Configure.Generative.google_gemini(),
        properties=[
            wvc.config.Property(
                name="body",
                data_type=wvc.config.DataType.TEXT,
            ),
        ],
        references=[
            wvc.config.ReferenceProperty(
                name="forMovie",
                target_collection="Movie",
            )
        ],
    )
    print("Created 'Synopsis' collection")
else:
    print("'Synopsis' collection already exists, skipping")

# Add hasSynopsis reference to Movie if not already present
movies = client.collections.get("Movie")
try:
    movies.config.add_reference(
        wvc.config.ReferenceProperty(
            name="hasSynopsis",
            target_collection="Synopsis"
        )
    )
    print("Added 'hasSynopsis' reference to Movie")
except Exception:
    print("'hasSynopsis' reference already exists, skipping")

reviews = client.collections.get("Review")
synopses = client.collections.get("Synopsis")

# ============================================================
# Import data from CSV
# ============================================================
movie_df = pd.read_csv("data/movies_data.csv")
print(f"\nLoaded {len(movie_df)} movies from CSV")

BATCH_SIZE = 40  # Stay under Gemini free tier limit of 100 req/min
SLEEP_BETWEEN_BATCHES = 30  # Seconds to wait between batches
MAX_RETRIES = 5


def insert_in_batches(collection, objs, label):
    """Insert objects in small batches with pauses to avoid rate limiting."""
    total_errors = 0
    total_inserted = 0
    for i in range(0, len(objs), BATCH_SIZE):
        batch = objs[i:i + BATCH_SIZE]
        for attempt in range(MAX_RETRIES):
            try:
                # Suppress Weaviate's verbose batch error output
                old_stdout = sys.stdout
                old_stderr = sys.stderr
                devnull = open(os.devnull, 'w')
                sys.stdout = devnull
                sys.stderr = devnull
                try:
                    response = collection.data.insert_many(batch)
                finally:
                    sys.stdout = old_stdout
                    sys.stderr = old_stderr
                    devnull.close()
                batch_errors = len(response.errors)
                batch_inserted = len(batch) - batch_errors
                total_errors += batch_errors
                total_inserted += batch_inserted
                print(f"  {label}: batch {i // BATCH_SIZE + 1} — {batch_inserted} inserted, {batch_errors} skipped/errors")
                break
            except Exception as e:
                if ("429" in str(e) or "503" in str(e) or "unavailable" in str(e).lower()) and attempt < MAX_RETRIES - 1:
                    wait = 60 * (attempt + 1)
                    print(f"  {label}: rate limited/unavailable, waiting {wait}s before retry (attempt {attempt + 2}/{MAX_RETRIES})...")
                    time.sleep(wait)
                else:
                    raise
        if i + BATCH_SIZE < len(objs):
            time.sleep(SLEEP_BETWEEN_BATCHES)
    print(f"Imported {total_inserted} new {label}, {total_errors} skipped (already exist or errors)")


def collection_count(collection):
    """Return the number of objects in a collection."""
    result = collection.aggregate.over_all(total_count=True)
    return result.total_count


# --- Import Reviews ---
expected_reviews = len(movie_df) * 3  # 3 reviews per movie
review_count = collection_count(reviews)
if review_count >= expected_reviews:
    print(f"Reviews already populated ({review_count} objects), skipping")
else:
    review_objs = list()
    for i, row in movie_df.iterrows():
        for c in [1, 2, 3]:
            col_name = f"Critic Review {c}"
            if len(str(row[col_name])) > 0:
                props = {"body": row[col_name]}
                review_uuid = generate_uuid5(row[col_name])
                data_obj = wvc.data.DataObject(properties=props, uuid=review_uuid)
                review_objs.append(data_obj)

    insert_in_batches(reviews, review_objs, "reviews")
    print("Waiting 60s before next collection...")
    time.sleep(60)

# --- Import Movies (with review references) ---
movie_count = collection_count(movies)
if movie_count >= len(movie_df):
    print(f"Movies already populated ({movie_count} objects), skipping")
else:
    movie_objs = list()
    for i, row in movie_df.iterrows():
        props = {
            "title": row["Movie Title"],
            "description": row["Description"],
            "rating": row["Star Rating"],
            "director": row["Director"],
            "movie_id": row["ID"],
            "year": row["Year"],
        }
        review_uuids = list()
        for c in [1, 2, 3]:
            col_name = f"Critic Review {c}"
            if len(str(row[col_name])) > 0:
                review_uuids.append(generate_uuid5(row[col_name]))

        movie_uuid = generate_uuid5(row["ID"])
        data_obj = wvc.data.DataObject(
            properties=props,
            uuid=movie_uuid,
            references={"hasReview": review_uuids},
        )
        movie_objs.append(data_obj)

    insert_in_batches(movies, movie_objs, "movies")
    print("Waiting 60s before next collection...")
    time.sleep(60)

# --- Import Synopses (with movie references) ---
synopsis_count = collection_count(synopses)
if synopsis_count >= len(movie_df):
    print(f"Synopses already populated ({synopsis_count} objects), skipping")
else:
    synopses_objs = list()
    for i, row in movie_df.iterrows():
        props = {"body": row["Synopsis"]}
        movie_uuid = generate_uuid5(row["ID"])
        data_obj = wvc.data.DataObject(
            properties=props,
            uuid=movie_uuid,
            references={"forMovie": movie_uuid},
        )
        synopses_objs.append(data_obj)

    insert_in_batches(synopses, synopses_objs, "synopses")

# --- Add hasSynopsis references from movies ---
synopses_refs = list()
for i, row in movie_df.iterrows():
    movie_uuid = generate_uuid5(row["ID"])
    ref_obj = wvc.data.DataReference(
        from_property="hasSynopsis", from_uuid=movie_uuid, to_uuid=movie_uuid
    )
    synopses_refs.append(ref_obj)

response = movies.data.reference_add_many(synopses_refs)
print(f"Added {len(synopses_refs)} hasSynopsis references ({len(response.errors)} errors)")

client.close()
print("\nDone! Your database is ready.")

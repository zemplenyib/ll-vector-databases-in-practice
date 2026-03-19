import utils
import weaviate.classes as wvc
from weaviate.util import generate_uuid5
import pandas as pd
from insert_in_batch import insert_in_batch
import time

BATCH_SIZE = 100

client = utils.connect_to_my_db()  # Connect to our own database

movies = client.collections.get("Movie")
reviews = client.collections.get("Review")
# Also load the synopses collection
synopses = client.collections.get("Synopsis")

movie_df = pd.read_csv("data/movies_data.csv")


# Import reviews first
review_objs = list()
for i, row in movie_df.iterrows():
    for c in [1, 2, 3]:  # There are three reviews per movie in this dataset
        col_name = f"Critic Review {c}"
        if len(row[col_name]) > 0:
            props = {
                "body": row[col_name],
            }
            review_uuid = generate_uuid5(row[col_name])
            data_obj = wvc.data.DataObject(properties=props, uuid=review_uuid)
            review_objs.append(data_obj)

insert_in_batch(reviews, review_objs, "review")
time.sleep(60)


# Import movies
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
    # Add references to reviews
    review_uuids = list()
    for c in [1, 2, 3]:
        col_name = f"Critic Review {c}"
        if len(row[col_name]) > 0:
            review_uuid = generate_uuid5(row[col_name])  # Identify the review IDs to refer to
            review_uuids.append(review_uuid)  # Collect the review IDs

    movie_uuid = generate_uuid5(row["ID"])
    data_obj = wvc.data.DataObject(
        properties=props,
        uuid=movie_uuid,
        references={"hasReview": review_uuids},
    )
    movie_objs.append(data_obj)

insert_in_batch(movies, movie_objs, "movie")
time.sleep(60)

# ====================================================================================================
# Add synopses data, similarly to how we added movies
# The synopsis data is in the same CSV file, but in the "Synopsis" column
# ====================================================================================================
# YOUR CODE GOES HERE
# Create a list of synopsis objects
synopses_objs = list()
# Iterate over the rows in the CSV file
for i, row in movie_df.iterrows():
    # Create a dictionary of properties
    props = {
        "body": row["Synopsis"]
    },
    
    # Generate a UUID for the synopsis (use the same UUID as the movie)
    synopsis_uuid = generate_uuid5(row["ID"])
    # Note: this is okay as the synopsis and movie are in different collections
    # Create a reference to the movie in the "forMovie" property
    synopsis_obj = wvc.data.DataObject(
        properties=props,
        uuid=synopsis_uuid,
        references={"forMovie": synopsis_uuid}
    )

    # Add the synopsis object to the list
    synopses_objs.append(synopsis_obj)

# Insert the synopsis objects into the collection
insert_in_batch(synopses, synopses_objs, "synopses")
time.sleep(60)

# ====================================================================================================
# Add references from movies to synopses
# Hints:
# - use the "wvc.data.DataReference" class to create the reference objects
# - use the "movies.data.reference_add_many" method to add the references to the collection
# ====================================================================================================
# YOUR CODE GOES HERE

# Create a list of references
synopsis_refs = list()
# Iterate over the rows in the CSV file
for i, row in movie_df.iterrows():
    # Generate a UUID for the movie
    movie_uuid = generate_uuid5(row["ID"])

    # Create a reference object with the "hasSynopsis" property
    # Hint: use the "wvc.data.DataReference" class
    # Add the reference object to the list
    synopsis_ref = wvc.data.DataReference(
        from_property="hasSynopsis",
        from_uuid=movie_uuid,
        to_uuid=movie_uuid
    )

# Add the references to the collection
# Hint: use the "movies.data.reference_add_many" method
for i in range(0, len(synopsis_refs), BATCH_SIZE):
    batch = synopsis_refs[i:i+BATCH_SIZE]
    response = synopses.data.reference_add_many(batch)

    print(f"Insertion complete with {len(response.uuids)} objects for 'synopsis' collection.")
    print(f"Insertion errors: {len(response.errors)}.")

    if i + BATCH_SIZE < len(synopsis_refs):
        time.sleep(60)

client.close()

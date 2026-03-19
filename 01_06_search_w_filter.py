import utils
import weaviate.classes as wvc

client = utils.connect_to_demo_db_goog()  # Connect to the demo database

movies = client.collections.get("Movie")

# Define the filter - look for the word "love" in the description
filter = (
    wvc.query.Filter.by_property("year").greater_or_equal(1990)
    & wvc.query.Filter.by_property("description").like("universes")
)

response = movies.query.near_text(  # Vector search
    query="science fiction",
    limit=3,
    filters=filter,  # With the filter
    return_metadata=wvc.query.MetadataQuery(distance=True),
)

for o in response.objects:
    movie_id = o.properties["movie_id"]
    movie_title = o.properties["title"]
    movie_year = o.properties["year"]

    print(f"ID: {movie_id}, {movie_title}, year: {movie_year}")     # Show which titles were found
    print(f"Distance: {o.metadata.distance:.3f}\n")
    print(o.properties["description"] + "...\n")                 # Show the description


client.close()

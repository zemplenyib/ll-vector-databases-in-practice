import utils
import weaviate.classes as wvc

client = utils.connect_to_my_db()  # Connect to our own database

existing = [c.name for c in client.collections.list_all().values()]
for name in existing:
     client.collections.delete(name)
     print(f"Deleted '{name}'")

client.collections.create(
    # Set the name of the collection
    name="Movie",

    # Set modules to be used
    vector_config=wvc.config.Configure.Vectors.text2vec_google_gemini(),    # Set the vectorizer module
    generative_config=wvc.config.Configure.Generative.google_gemini(model="gemini-3.1-flash-lite"),  # Set the generative module
    # Note: Could also explicitly set the model, e.g.:
    # generative_config=wvc.config.Configure.Generative.openai(model="gpt-4-1106-preview"),

    # Define the properties of the collection
    properties=[
        wvc.config.Property(
            # Set the name of the property
            name="title",
            # Set the data type of the property
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
)

client.close()

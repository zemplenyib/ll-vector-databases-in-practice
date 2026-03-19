import utils
import weaviate.classes as wvc
from weaviate.classes.config import Reconfigure
import argparse

# gemini-2.5-flash-lite
# gemini-3.1-flash-lite

def main():
    parser = argparse.ArgumentParser("A script to update generative config of the collections.")
    parser.add_argument("model_name", help="Name of the desired LLM model.")
    args = parser.parse_args()

    client = utils.connect_to_demo_db_goog()

    collections = client.collections.list_all()

    for name, config in collections.items():
        try:
            coll = client.collections.get(name)
            coll.config.update(generative_config=Reconfigure.Generative.google_gemini(model=args.model_name))
            print(f"Successfully updated {name} collection's generative_config to {args.model_name}.")
        except Exception as e:
            print(f"Failed to update {name} collection's generative_config to {args.model_name}.")
            print(f"Error: {e}")

    client.close()
    
if __name__ == "__main__":
    main()
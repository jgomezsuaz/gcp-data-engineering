import argparse
import requests
from fastavro import writer, parse_schema
from google.cloud import storage

schema = {
    "type": "record",
    "name": "Character",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "status", "type": "string"},
        {"name": "species", "type": "string"},
        {"name": "type", "type": ["null", "string"]},
        {"name": "gender", "type": "string"},
        {
            "name": "origin",
            "type": {
                "type": "record",
                "name": "Origin",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "url", "type": "string"}
                ]
            }
        },
        {
            "name": "location",
            "type": {
                "type": "record",
                "name": "Location",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "url", "type": "string"}
                ]
            }
        },
        {"name": "image", "type": "string"},
        {"name": "episode", "type": {"type": "array", "items": "string"}},
        {"name": "url", "type": "string"},
        {"name": "created", "type": "string"}
    ]
}
    

def run():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Get data from the Rick & Morty API to Avro')
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')

    opts = parser.parse_args()
    
    # Static input and output
    input = 'gs://{0}/characters.avro'.format(opts.project)
    file = 'characters.avro'
    
    base_url = 'https://rickandmortyapi.com/api/character?page='
    total_pages = 42

    results = [requests.get(base_url + str(page)).json()['results'] for page in range(1, total_pages + 1)]
    results = [item for sublist in results for item in sublist]

    parsed_schema = parse_schema(schema)

    with open('characters.avro', 'wb') as out:
        writer(out, parsed_schema, results)
        
    storage_client = storage.Client()
    bucket = storage_client.bucket(opts.project)
    blob = bucket.blob(file)
    blob.upload_from_filename(file)
        
    print("Data correctly stored in " + input)
        
        
if __name__ == '__main__':
    print('Getting the data...')
    run()

    
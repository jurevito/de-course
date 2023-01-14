
import json
from pprint import pprint

def read_large_json(file_path):
    with open(file_path, 'r') as json_file:
        for line in json_file:
            yield json.loads(line)


def save_chunks(n_chunks, n_records):

    iterator = read_large_json('./data/ingestion/publications.json')

    for i in range(n_chunks):
        with open(f'./data/ingestion/chunk_{i}.json', 'w+') as save_file:
            for count, json_object in enumerate(iterator):
                
                # Save json object into a file.
                json.dump(json_object, save_file)
                save_file.write('\n')

                if count+1 >= n_records:
                    break


if __name__ == '__main__':
    save_chunks(n_chunks=4, n_records=50_000)

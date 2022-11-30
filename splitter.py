import filter
import pickle
from firebase_admin import credentials, initialize_app, storage
import os

# Init firebase with your credentials
cred = credentials.Certificate("keystore.json")
initialize_app(cred, {'storageBucket': 'h-cluster-pool'})

def input_data_splitter(cluster_identifier, mapper_identifiers, input_file):
    number_of_mappers = len(mapper_identifiers)
    data_file = open(filter.filter_text(input_file), 'r')
    file_text = data_file.read()
    input_data = file_text.split()
    data_file.close()
    bucket = storage.bucket()

    # if not os.path.exists(cluster_identifier):
    #     os.mkdir(cluster_identifier)

    for i in range(number_of_mappers):
        output_file_name = str(cluster_identifier) + '/' + str(mapper_identifiers[i]) + '-mpinput.txt'
        output_file = open( output_file_name , "w+")
        data = input_data[round(i * (len(input_data)/number_of_mappers)):round((i+1) * (len(input_data)/number_of_mappers))]
        for word in data:
            output_file.write(word+'\n')
        output_file.close()
        blob = bucket.blob(output_file_name)   
        blob.upload_from_filename(output_file_name) 

# input_data_splitter("cls-f8ly", ["cls-f8ly-mapper-0"], "6527-0.txt")
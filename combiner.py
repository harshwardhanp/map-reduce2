import glob
import pickle
from firebase_admin import credentials, initialize_app, storage
from reduce import reduce_function
# Init firebase with your credentials
cred = credentials.Certificate("keystore.json")
initialize_app(cred, {'storageBucket': 'h-cluster-pool'})

def download_file(input_file_name, output_file_name):
    bucket = storage.bucket()
    blob = bucket.blob(input_file_name)   
    blob.download_to_filename(output_file_name)

def upload_file(source_file, destination_file):
    bucket = storage.bucket()
    blob = bucket.blob(destination_file)   
    blob.upload_from_filename(source_file)

def mapper_output_combiner(cluster_identifier, reducer_number, reducer_identifier, mapper_list):
    local_input_file_for_R = str(cluster_identifier) + '-r-' + str(reducer_identifier) + '-rdinput.txt'
    cloud_input_file_for_R = str(cluster_identifier) + '/r-' + str(reducer_identifier) + '-rdinput.txt'
    
    input_file = open(local_input_file_for_R, "w+")
    for mapper in mapper_list:
        mp_opfile_cloudname = cluster_identifier + '/m-' + mapper + '-' + str(reducer_number) + '.txt'
        mp_opfile_localname = cluster_identifier + '-m-' + mapper + '-' + str(reducer_number) + '.txt'
        download_file(mp_opfile_cloudname, mp_opfile_localname )

    # mp_opfile_uploadname = cluster_id + '/m-' + hostname + '-' + str(i + 1) + '.txt'
    for mapper in mapper_list:
        mp_opfile_localname = cluster_identifier + '-m-' + mapper + '-' + str(reducer_number) + '.txt'
        with open(mp_opfile_localname, "r") as mapper_output:
            input_file.write(mapper_output.read())

    input_file.close()
    upload_file(local_input_file_for_R, cloud_input_file_for_R)


def reducer_output_combiner(cluster_identifier, reducer_list, output_file_name):
    local_rdoutput_combiner = str(cluster_identifier) + '-r-rdoutput-combined.txt'
    # cloud_rdoutput_combiner = str(cluster_identifier) + '/r-rdoutput-combined.txt'
    


    for reducer in reducer_list:
        mp_opfile_cloudname = cluster_identifier + '/' + reducer + '-rdoutput.txt'
        mp_opfile_localname = cluster_identifier + '-' + reducer + '-rdoutput.txt'
        download_file(mp_opfile_cloudname, mp_opfile_localname )
    
    rdoutput_combiner = open(local_rdoutput_combiner, "w+")
    
    # mp_opfile_uploadname = cluster_id + '/m-' + hostname + '-' + str(i + 1) + '.txt'
    for reducer in reducer_list:
        reducer_r_output_file = cluster_identifier + '-' + reducer + '-rdoutput.txt'
        with open(reducer_r_output_file, "r") as reducer_output:
            rdoutput_combiner.write(reducer_output.read())

    rdoutput_combiner.close()

    data_file = open(local_rdoutput_combiner, "r")
    
    file_lines = data_file.readlines()

    reducer_output = reduce_function(file_lines)

    local_output_location = str(cluster_identifier) + '-fnfle-' + output_file_name
    cloud_output_location = str(cluster_identifier) + '/fnfle-' + output_file_name
    
    output_file = open(local_output_location, "w+")
    for key, value in reducer_output.items():
        output_file.write(str(key) + ' ' + str(value) + '\n')
    output_file.close()
    upload_file(local_output_location, cloud_output_location)
    return cloud_output_location


# mapper_output_combiner("cls-f8ly", "1", "cls-f8ly-reducer-0", ["cls-f8ly-mapper-0"])

reducer_output_combiner("cls-f8ly",  ["cls-f8ly-reducer-0"] , "output_file.txt")
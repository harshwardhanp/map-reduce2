#python3 master.py cluster_id
import socket
import multiprocessing
import time
import firebase_admin
import pickle
from firebase_admin import credentials, initialize_app, storage
import sys
import splitter
import os
import combiner


# Init firebase with your credentials
cred = credentials.Certificate("keystore.json")
initialize_app(cred, {'storageBucket': 'h-cluster-pool'})

def send(message, function_name, cluster_id, host_ip):
    host = host_ip
    port = 3278
    reply_msg=""

    try:
        clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clientSocket.connect((host, port))
    except socket.error as e:
        print("failed")

    try:
        response = clientSocket.recv(1024).decode()
        print("Response from client", response)
        if response != "Success":
            raise Exception("Error Occured in Mapredce phase")
        print(message)
        print(cluster_id)
        print(function_name)
        print(host_ip)
        command = message + " " + cluster_id +" "+ function_name
        print(command)
        clientSocket.send(command.encode())

        Response = clientSocket.recv(1024).decode("utf-8")
        if Response != "Completed":
            raise Exception("Error Occured in Mapredce phase")
        clientSocket.send(str.encode("exit"))
        clientSocket.close()

        return 0
    except Exception as ex:
        return 1



# print(send("127.0.1.1",1432,"get Harsh"))
# 1. get list of arguments - clusterid, mapper_function, reducer_function
# 2. get the cluster info from cluster id
# 3. separate mapper ips and reducer ips, keep them in a list
# 4. separate input file for m mappers call splitter function for that
# thus create input files for mappers and reducers using their host name
# 5. parallely iterate over the mappers indicating start function
# mention input_file for that mapper_________this will be mapper_hostname.txt , list of reducer_hostnames


 
# total arguments
# n = len(sys.argv)
# print("Total arguments passed:", n)
 
# Arguments passed
try:
    if len(sys.argv) != 4:
        raise Exception("Invalud argument number")

    cluster_id, mapper_func, reducer_func, input_file, output_file= sys.argv[1:]
    
    cluster_info_filename = cluster_id + "_cluster_info.txt"
    # uncomment
    bucket = storage.bucket()
    blob = bucket.blob(cluster_info_filename)   
    
    #Create a cluster_directory if cluster directory does not exist
    if not os.path.exists(cluster_id):
        os.mkdir(cluster_id)

    list_machines = []
    # uncomment
    with blob.open("r") as f:
        list_machines = f.readlines()

    # comment
    # with open(cluster_info_filename, "r") as f:
    #     list_machines = f.readlines()
    
    # print(list_machines)
    mapper = {}
    reducer = {}

    for line in list_machines:
        name_ip = line.split(":")
        if cluster_id + "-mapper-" in name_ip[0]: 
            mapper[name_ip[0]] = name_ip[1]
        if cluster_id + "-reducer-" in name_ip[0]: 
            reducer[name_ip[0]] = name_ip[1]

    mappers = len(mapper)

    blob = bucket.blob(input_file)   
    blob.download_to_filename(input_file)
    
    splitter.input_data_splitter(cluster_id, mapper.keys(), input_file)

    def mapper_init():
        while True:
            pool = multiprocessing.Pool(processes=mappers)
            outputs = pool.starmap(send , [("init_map", mapper_func, cluster_id, value) for value in mapper.values()])

            if sum(outputs) == 0:
                break
    
    mapper_init()

    for key, index in enumerate(reducer.keys()):
        combiner.mapper_output_combiner(cluster_id, index + 1, key, mapper.keys())

    def reducer_init():
        while True:
            reducers = len(reducer)
            pool = multiprocessing.Pool(processes=reducers)
            outputs = pool.starmap(send , [("init_reduce", reducer_func, cluster_id, value) for value in reducer.values()])
            
            if sum(outputs) == 0:
                break


    reducer_init()

    cloud_output_location = combiner.reducer_output_combiner(cluster_id, reducer.keys(), output_file)

    print(cloud_output_location)
except Exception as ex:
    print(ex)
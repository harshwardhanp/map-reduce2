# from google.cloud import storage
# import pickle

# from cluster import cluster

# project_id="harshwardhan-patil-fall2022"

# storage_client = storage.Client(project=project_id)
# buckets = storage_client.list_buckets()
# names_of_buckets = []
# for bucket in buckets:
#     names_of_buckets.append(bucket.name)

# bucket_name = "h-cluster-pool"
# blob_name = "h_cluster_pool.pkl"

# if bucket_name not in names_of_buckets:
#     storage_client.create_bucket(bucket_name)
#     # print("Bucket created.")

# bucket = storage_client.bucket(bucket_name)
# blob = bucket.blob(blob_name)

# cls_obj1 = cluster()
# cls_obj1._id = "dflhbfk-23432"
# cls_obj1.mappers = 3
# cls_obj1.reducers = 3
# cls_obj1.mapper_func = "WC"
# cls_obj1.reducer_func = "WC"

# cls_obj2 = cluster()
# cls_obj2._id = "dflhbfk-23432"
# cls_obj2.mappers = 3
# cls_obj2.reducers = 3
# cls_obj2.mapper_func = "WC"
# cls_obj2.reducer_func = "WC"

# cls_list = [cls_obj1, cls_obj2]
# with blob.open('wb') as outp:
#     pickle.dump(cls_list, outp, pickle.HIGHEST_PROTOCOL)

# # reading python objects from a pickel file
# def pickle_loader():
#     with blob.open("rb") as f:
#         while True:
#             try:
#                 yield pickle.load(f)
#             except EOFError:
#                 break

# print('objects in pickle file:')
# for cluster_list in pickle_loader():
#     for cluster_obj in cluster_list:
#         print('  name: {}, value: {}'.format(cluster_obj._id, cluster_obj.mappers))

#######################################Pernamant Code####################################
import pickle
from firebase_admin import credentials, initialize_app, storage

# Init firebase with your credentials
cred = credentials.Certificate("keystore.json")
initialize_app(cred, {'storageBucket': 'h-cluster-pool'})
#########################################################################################
# Put your local file path 

bucket = storage.bucket()

# upload a file to the firebase
# fileName = "temp.txt"
# blob = bucket.blob(fileName)
# blob.upload_from_filename(fileName)

# read names of files from firebase
# for blob in bucket.list_blobs():
#     name = str(blob.name)
#     print(name)
   
# blob_name = "h_cluster_pool.pkl"

# blob = bucket.blob(blob_name)

# # reading python objects from a pickel file
# def pickle_loader():
#     with blob.open("rb") as f:
#         while True:
#             try:
#                 yield pickle.load(f)
#             except EOFError:
#                 break

# print('objects in pickle file:')
# for cluster_list in pickle_loader():
#     for cluster_obj in cluster_list:
#         print('  name: {}, value: {}'.format(cluster_obj._id, cluster_obj.mappers))


#using firestore

blob_name = "cls-f8ly" + "_cluster_info.txt"
# bucket = storage.bucket()
# blob = bucket.blob(blob_name)   
# blob.upload_from_filename(blob_name)

list_machines = []
print('data in pickle file:')
# with blob.open("r") as f:
#     list_machines = f.readlines()
with open(blob_name, "r") as f:
    list_machines = f.readlines()
print(list_machines)
mapper = {}
reducer = {}

for line in list_machines:
    name_ip = line.split(":")
    if "-mapper-" in name_ip[0]: 
        mapper[name_ip[0]] = name_ip[1]
    if "-reducer-" in name_ip[0]: 
        reducer[name_ip[0]] = name_ip[1]

# print("Mapper")
# for key,val in mapper.items():
#     print(key, val)

# print("mappers are : ",len(mapper))

# print("Reducer")
# for key,val in reducer.items():
#     print(key, val)

# print("reducers are : ",len(mapper))


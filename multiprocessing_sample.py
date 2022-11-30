import multiprocessing
import time
   
ax_temp=3
def send(message,hostname, host_ip, reducer_dict):
    # print("x is sending the message ")
    print("The host :: ", hostname,host_ip, "value of axtemp", ax_temp)
    ax_temp += 1
    
    # return 0
    if  ax_temp % 2 ==0:
        # time.sleep(1)
        return 0
    else:
        # time.sleep(1)
        return 1
   

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
# print(list_machines)
mapper = {}
reducer = {}

for line in list_machines:
    name_ip = line.split(":")
    if "-mapper-" in name_ip[0]: 
        mapper[name_ip[0]] = name_ip[1]
    if "-reducer-" in name_ip[0]: 
        reducer[name_ip[0]] = name_ip[1]

mappers = len(mapper)
# pool = multiprocessing.Pool()
pool = multiprocessing.Pool(processes=mappers)
inputs = [0,1,2,3,4]

print(len(inputs))
# outputs = pool.map(send,[ mapper.keys(), mapper.values()])
while True:
    outputs = pool.starmap(send , [("init_map",key, value, reducer) for key,value in mapper.items()])
        
    print("Input: {}".format(inputs))
    print("Output: {}".format(outputs))
    if sum(outputs) == 0:
        print("success")
        break


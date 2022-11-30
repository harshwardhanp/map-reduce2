# class cluster2:
#     def __init__(self) -> None:
#         mappers = None
#         reducers = None
#         status = None
#         __id = None
    
#     def get_id(self):
#         self.__id = "fsd"
#         print(self.__id)
    
# cl = cluster2()
# cl.get_id()

# import subprocess
# temp_file = open("temp.txt",'w')
# count = 5
# cluster_id = "cls-vds4"
# spec = "mapper"
# c = 3
# machine_name = cluster_id +'-'+ spec+'-'+str(c)


# # subprocess.call(["bash","create_network.sh","cls-1-mapper-1", str(count)], stdout=temp_file)
# # subprocess.call(["bash","create_network.sh","default", str(count)], stdout=temp_file)
# # subprocess.call(["bash","delete_network.sh","default"], stdout=temp_file)
# subprocess.call(["bash","create_vm.sh",machine_name], stdout=temp_file)
# # subprocess.call(["bash","delete_vm.sh",machine_name], stdout=temp_file)

# with open("temp.txt",'r') as file:
#     output = file.read()
# print("The Ip is:")
# print(output.split()[-1])

f = open("temp.txt",'r')
print(f.readable())

# import glob, os
# try:
#     for file in glob.glob("\\*.txt"):
#         with open(file) as fp:
#             # do something with file
# except IOError:
#     print("could not read", file)
import socket
def send(message, function_name, cluster_id, host_ip):

    host = host_ip
    port = 3278
    reply_msg=""

    #print("Waiting for connection")
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
# print(send("init_map", "map_wc", "cls-f8ly", "192.168.1.18"))
# print(send("init_reduce", "red_wc", "cls-f8ly", "192.168.1.18"))

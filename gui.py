import streamlit as st
import socket
import os
import pickle
import sys
import math

MAX = 10000
#Connecting to the master_server

def list_files(directory):
    if(os.path.exists(directory)):
        files = os.listdir(directory)
        return [file for file in files if os.path.isfile(os.path.join(directory, file))]
    else:
        return []



def connect_to_master_server(getCommand, no_of_arg):
    try:
        s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        s.connect((socket.gethostbyname('localhost'),7082))
    except:
        try:
            s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            s.connect((socket.gethostbyname('localhost'),7083))
        except:
            print("Master server is not active...Try again later!!!")        
            sys.exit()    

    if no_of_arg==2:
        split_command = getCommand.split(' ')
        decision = split_command[0]
        filename = ' '.join(split_command[1:])
        # print(decision)
        # print(filename)
        if(decision=="upload"):
            size=str(os.path.getsize(filename))
            fileplussize="client"+":upload:"+filename+":"+size
            # print(fileplussize)
            s.send(bytes(fileplussize,"utf-8"))
            chunks=[]
            # status=s.recv(2048).decode("utf-8","ignore")
            status=s.recv(2048)
            status=pickle.loads(status)
            # print(status)
            if "Upload" in status:
                chunks=pickle.loads(s.recv(MAX))
            elif "Present" in status:
                chunks=''            
            return chunks
            
        if(decision=="download"):
            f_download="client"+":download:"+filename+":dummydata"
            s.send(bytes(f_download,"utf-8"))
            chunks=[]
            # getz1=s.recv(2048).decode('utf-8')
            # getz1=int(getz1)+1000
            # print(getz)
            chunks=pickle.loads(s.recv(MAX))
            return chunks
        
        if(decision=="lease"):
            f_lease="client"+":lease:"+filename+":dummydata"    
            s.send(bytes(f_lease,"utf-8"))
            status=s.recv(2048)
            status=pickle.loads(status)
            print(status)
            if "unavailable" in status:
                msg=s.recv(2048)
                msg=pickle.loads(msg)
                print(msg)

        if(decision=="unlease"):    
            f_lease="client"+":unlease:"+filename+":dummydata"    
            s.send(bytes(f_lease,"utf-8"))
            status=s.recv(2048)
            status=pickle.loads(status)
            print(status)   

        if decision == "update":
            size=str(os.path.getsize(filename))
            fileplussize="client"+":update:"+filename+":"+size
            #print(fileplussize)
            s.send(bytes(fileplussize,"utf-8"))
            chunks=[]
            # status=s.recv(2048).decode("utf-8","ignore")
            status=s.recv(2048)
            status=pickle.loads(status)
            # print(status)
            if "update" in status:
                chunks=pickle.loads(s.recv(MAX))
            # elif "Present" in status:
                # chunks=''
            #print(chunks)            
            return chunks
            

    if no_of_arg==1:
        f_list_files="client"+":listfiles:"+"dummy1"+":dummy2"             
        s.send(bytes(f_list_files,"utf-8"))
        status=s.recv(2048)
        status=pickle.loads(status)
        print("Files which are available:", end=" ")
        for i in status:
            print(i, end=" ")   
        print()    


#Connecting to the chunk_server
def connect_to_chunk_server(decision,chunks,filename):
    list1=[6467,6468,6469,6470]
    
    if(decision=="upload"):
        chunks_list=[]
        f=open(filename,'rb')
        data=f.read(2048)

        while data:
            chunks_list.append(data)
            data=f.read(2048)
        
        #Sending chunks to the appropriate chunkserver
        for chunk_id,chunk_server in chunks:
            s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            s.connect((socket.gethostbyname('localhost'),list1[chunk_server-1]))
            # filename="file2.txt"
            to_send="client:"+"upload:"+str(chunk_server)+":"+str(chunk_id)+":"+filename+":"
            #print(to_send)
            to_send=to_send.ljust(400,'~')
            # print(len(to_send.encode('utf-8')))
            #print(to_send)
            s.send(str(to_send).encode("utf-8"))
            s.send(chunks_list[chunk_id-1])

    data=""

    if(decision=="download"):

            filesystem = os.getcwd()+"/Client"

            if not os.access(filesystem, os.W_OK):
                    os.makedirs(filesystem)

            filesystem=filesystem+"/"+str(filename)        

            if os.path.exists(filesystem):
                    os.remove(filesystem)

            for chunk_id,chunk_server in chunks:
                s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                s.connect((socket.gethostbyname('localhost'),list1[chunk_server-1]))
                to_send="client:"+"download:"+str(chunk_server)+":"+str(chunk_id)+":"+filename+":"    
                # print(to_send)
                to_send=to_send.ljust(400,'~')
                s.send(str(to_send).encode("utf-8"))    

                with open(filesystem, 'ab') as f:
                    c_recv=s.recv(2048)
                    f.write(c_recv)



def connect_to_chunk_server_update(decision,chunks,filename,numChunks):
    list1=[6467,6468,6469,6470]
    
    if(decision=="update"):
        chunks_list=[]
        f=open(filename,'rb')
        data=f.read(2048)

        while data:
            chunks_list.append(data)
            data=f.read(2048)
        
        #Sending chunks to the appropriate chunkserver
        for chunk_id,chunk_server in chunks:
            s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            if chunk_id >= numChunks:
                s.connect((socket.gethostbyname('localhost'),list1[chunk_server-1]))
                # filename="file2.txt"
                to_send="client:"+"upload:"+str(chunk_server)+":"+str(chunk_id)+":"+filename+":"
                #print(to_send)
                to_send=to_send.ljust(400,'~')
                # print(len(to_send.encode('utf-8')))
                #print(to_send)
                s.send(str(to_send).encode("utf-8"))
                s.send(chunks_list[chunk_id-1])

    data=""



st.title("Distributed File System")

# Upload File Section
st.header("Upload File")
uploaded_file = st.file_uploader("Choose a file to upload")

if uploaded_file:
    filename = uploaded_file.name
    chunks = connect_to_master_server(f"upload {filename}", 2)
    if chunks:
        connect_to_chunk_server("upload", chunks, filename)
        st.success("File uploaded successfully!")
    else:
        st.error("File already exists!")

# Download File Section
st.header("Download File")
download_filename = st.text_input("Enter filename to download")
download_button = st.button("Download")

if download_button:
    chunks = connect_to_master_server(f"download {download_filename}", 2)
    if chunks:
        connect_to_chunk_server("download", chunks, download_filename)
        st.success("File downloaded successfully!")
    else:
        st.error("File not found!")

# Update File Section
st.header("Update File")
update_file1 = st.file_uploader("Choose the file to update")
update_file2 = st.file_uploader("Choose the file to append")

if update_file1 and update_file2:
    filename1 = update_file1.name
    filename2 = update_file2.name
    numChunks = math.ceil(os.path.getsize(filename1) / 2048)
    with open(filename1, "ab") as f:
        f.write(update_file2.read())
    chunks = connect_to_master_server(f"update {filename1}", 2)
    if chunks:
        connect_to_chunk_server_update("update", chunks, filename1, numChunks)
        st.success("File updated successfully!")
    else:
        st.error("File update unsuccessful!")

with st.sidebar:
    st.header("List Files in Client Directory")
    client_files = list_files("Client")

    if client_files:
        st.write("Downloaded Files:")
        for file in client_files:
            st.write(file)
    else:
        st.write("No files available for download.")

st.write("---")

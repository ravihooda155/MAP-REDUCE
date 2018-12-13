#include<bits/stdc++.h>
#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <openssl/sha.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include<dirent.h>
#include <atomic>
int calhash(std::string str)
{
    long long  hash = 1;
    for (int i = 0; i < str.length(); i++) 
    {
        hash += pow(37,i)*str[i];
        hash%=3;
    }
   
return hash;
}
void mapper_task_thread(int new_socket_fd,struct sockaddr_in client_addr,std::string job_id,std::string mapper_id)
{
    std::cout<<"mapper id:"<<mapper_id<<std::endl;
    std::cout<<"----------------------------------------------\n";
    std::string basepath=job_id+"/in/reducer";
    mkdir(basepath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    std::string red_path=job_id+"/in/reducer/"+mapper_id;
    mkdir(red_path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    
    struct sockaddr_in master_addr,self_addr;  
    char buffer[1024];
    

 
    std::ifstream in;
    in.open(job_id+"/in/mapper/"+mapper_id);
    std::string str;
  
    while(in>>str)
    {
        std::vector<std::string> tokens;
		tokens.clear();
		std::stringstream check1(str);
		std::string rawcode;
		while(getline(check1, rawcode, ','))
		{
			tokens.push_back(rawcode);
		}
         int reducer_num=calhash(tokens[0]);
         std::string filename=job_id+"/in/reducer/"+mapper_id+"/"+std::to_string((reducer_num));
         std::ofstream out;
         out.open(filename, std::fstream::out |std::fstream::app);
         out<<str<<"\n";
         out.close();
    }
    in.close();
    
 
    bzero(buffer,1024);
    std::strcpy(buffer,"completed");
    int n=send(new_socket_fd,buffer,strlen(buffer),0);
    if(n>0)
    {
        std::cout<<"message sent"<<std::endl;
    }
    else
    {
        std::cout<<"failed"<<std::endl;
    }
    std::cout<<mapper_id<<"closed "<<std::endl;
    std::cout<<"----------------------------------------------\n";
}
void init_mapper_connection(int *sockfd,std::string ip,int port)
{
    
    int new_socket_fd;
    socklen_t clilen;
    char buffer[1024]; 
    struct sockaddr_in self_addr; 
    if((*sockfd=socket(AF_INET, SOCK_STREAM,0))<0)
    { 
        std::cout<<"socket creation failed"<<"\n"; 
        close(*sockfd);
        std::exit(0);
    } 
    memset(&self_addr, 0, sizeof(self_addr)); 
    self_addr.sin_family=AF_INET;
    self_addr.sin_addr.s_addr = inet_addr(ip.c_str());
    self_addr.sin_port = htons(port); 
    if(bind(*sockfd, (struct sockaddr *)&self_addr,sizeof(self_addr))<0)
    { 
        std::cout<<"TCP socket bind failed"<<"\n";
        close(*sockfd);
        std::exit(0);
    }
}

int main(int argc, char  *argv[])
{
    int sockfd=0;
    socklen_t clilen;
    struct sockaddr_in client_addr;  
    char buffer[1024]; 
   
    if (argc !=3)
    {
      std::cout<<"enter all parameters"<<std::endl;
      exit(1);
    }
    
    init_mapper_connection(&sockfd,argv[1],std::stoi(argv[2]));
    while(1)
    {
        std::cout<<"mapper listening... "<<std::endl;
        listen(sockfd,10);
        int new_socket_fd= accept(sockfd,(struct sockaddr *) &client_addr,&clilen);
        bzero(buffer,1024);
        //read(new_socket_fd,buffer,1024);
    
	    int num;
   
        if ((num=recv(new_socket_fd, buffer,1024,0))== -1)
	    {
            perror("recv");
            exit(1);
        }  
        buffer[num] = '\0';
       
        std::string msg(buffer);
        std::cout<<msg;
        std::vector<std::string> tokens;
		std::stringstream check1(msg);
		std::string rawcode;
		while(getline(check1, rawcode, ','))
		{
			tokens.push_back(rawcode);
		}
        std::string job_id,mapper_id;
        job_id=tokens[0];
        mapper_id=tokens[1];

        std::thread mapper_task(mapper_task_thread,new_socket_fd,client_addr,job_id,mapper_id);            
        mapper_task.detach();
    }       
   std::cout<<"maapper exited,task completed.."<<std::endl; 
   std::exit(0);
}
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
#define MAPPER_COUNT 5
#define REDUCER_COUNT 3

std::string mapper_ips[5]={"127.0.0.2","127.0.0.3","127.0.0.4","127.0.0.5","127.0.0.6"};
std::string reducer_ips[3]={"127.0.0.7","127.0.0.8","127.0.0.9"};
int mapper_ports[5]={8081,8082,8083,8084,8085};
int reducer_ports[3]={8086,8087,8088};


int getWordsCount(std::string file_name)
{
    std::string str;
    int count=0;
    std::ifstream in;
    in.open(file_name);
    while(in>>str)
        count++;
    in.close();
    return count;
}
void init_file_partition(int chunk_size,std::string input_filename,int job_id)
{
    std::ifstream in;
    std::ofstream out;
    in.open(input_filename);
    std::string job_path=std::to_string(job_id)+"/in";
    std::string filename=job_path+"/mapper";
    mkdir(std::to_string(job_id).c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    mkdir(job_path.c_str(),S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    mkdir(filename.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    for(int i=1;i<=MAPPER_COUNT;i++)
    {
        std::string str;
        int count=0;
        out.open(filename+"/"+std::to_string(i));
        while(count!=chunk_size)
        {
            in>>str;
            out<<str<<"\n";
            count++;
        }
        out.close(); 
    }
    in.close();
    std::cout<<"-------------------------\n";
    std::cout<<"File partitoning done\n";
    std::cout<<"-------------------------\n";
}

void init_master_connection(int *sockfd,int port,int *reuse)
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
    if (setsockopt(*sockfd, SOL_SOCKET, SO_REUSEADDR,reuse, sizeof(int)) < 0)
        printf("setsockopt(SO_REUSEADDR) failed\n"); 
    memset(&self_addr, 0, sizeof(self_addr)); 
    self_addr.sin_family=AF_INET;
    self_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    self_addr.sin_port = htons(port); 
    if(bind(*sockfd, (struct sockaddr *)&self_addr,sizeof(self_addr))<0)
    { 
        std::cout<<"TCP socket bind failed"<<"\n";
        close(*sockfd);
        std::exit(0);
    }
}
void master_job(std::string file_name,int port,int job_id)
{
    int sockfd[MAPPER_COUNT]={0},sockfd_1[REDUCER_COUNT]={0},mapper_count=0,reducer_counter=0,red;
    struct sockaddr_in self_addr,mapper_addr,reducer_addr;  
    socklen_t clilen;
    struct sockaddr_in client_addr; 
    char buffer[1024];
    int word_count=getWordsCount(file_name);
    std::cout<<"------------------------\n";
    std::cout<<"words in file:"<<word_count<<std::endl;
    std::ifstream fin(job_id+"/FinalOutput.txt");
    if(fin) {
        remove(job_id+"/FinalOutput.txt");
        fin.close();
    }

    ////////// file partitioning//////////////

    int chunk_size=word_count/MAPPER_COUNT;
    init_file_partition(chunk_size,file_name,job_id);
    int i,reuse=1;
    std::cout<<"********************"<<std::endl;
    std::cout<<"Mapping Job"<<std::endl;
    std::cout<<"********************"<<std::endl;
    for(i=0;i<MAPPER_COUNT;i++)
    {
        init_master_connection(&sockfd[i],port,&reuse);        
        //sockfd[i]=socket(AF_INET, SOCK_STREAM, 0);
        mapper_addr.sin_family = AF_INET;
        mapper_addr.sin_addr.s_addr=inet_addr(mapper_ips[i].c_str());
        mapper_addr.sin_port = htons(mapper_ports[i]);
        if(connect(sockfd[i],(struct sockaddr *) &mapper_addr,sizeof(mapper_addr)) < 0) 
        {
            std::cout<<"Failed to connect to mapper_"<<i<<"\n";
            close(sockfd[i]);
            std::exit(0);
        }
        else
        {
            std::cout<<"connected to mapper_"<<i<<"\n";
        }
    }
    for(int i=0;i<MAPPER_COUNT;i++)
    {
        /* reading input string from client */
        bzero(buffer,1024);
        std::string msg=std::to_string(job_id)+","+std::to_string(i+1)+","+std::to_string(port);
        std::strcpy(buffer,msg.c_str());  
        send(sockfd[i],buffer,strlen(buffer),0);
    }
        /* receive message here*/
    for(int i=0;i<MAPPER_COUNT;i++)
    {
        bzero(buffer,1024);
        int num;
        if((num=recv(sockfd[i], buffer,1024,0))== -1)
	    {
            perror("recv");
            exit(1);
        }  
        buffer[num] = '\0';
        std::string msg(buffer);
        if(msg=="completed")
        {
            mapper_count++;
        }
        if(mapper_count==MAPPER_COUNT)
        {
            std::cout<<"------------------------"<<std::endl;
            std::cout<<"finished mapping"<<std::endl;
            std::cout<<"-------------------------"<<std::endl;
            std::cout<<"********************"<<std::endl;
            std::cout<<"Reducing Job"<<std::endl;
            std::cout<<"********************"<<std::endl;
            for(int i=0;i<REDUCER_COUNT;i++)
            {
                init_master_connection(&sockfd_1[i],port,&reuse);        
                reducer_addr.sin_family = AF_INET;
                reducer_addr.sin_addr.s_addr=inet_addr(reducer_ips[i].c_str());
                reducer_addr.sin_port = htons(reducer_ports[i]);
                if(connect(sockfd_1[i],(struct sockaddr *) &reducer_addr,sizeof(reducer_addr)) < 0) 
                {
                    std::cout<<"Failed to connect to reducer_"<<i<<"\n";
                    close(sockfd_1[i]);
                    std::exit(0);
                }
                else
                {
                    std::cout<<"connected to reducer_"<<i<<"\n";
                }
            }

            for(int i=0;i<REDUCER_COUNT;i++)
            {
                bzero(buffer,1024);
                std::string msg=std::to_string(job_id)+","+std::to_string(i)+","+std::to_string(port);
                std::strcpy(buffer,msg.c_str());  
                send(sockfd_1[i],buffer,strlen(buffer),0);
            }

            for(int i=0;i<REDUCER_COUNT;i++)
            {
                bzero(buffer,1024);
                int num;
                if((num=recv(sockfd_1[i], buffer,1024,0))== -1)
	            {
                    std::cout<<"error occured"<<std::endl;
                    perror("recv");
                    exit(1);
                }  
                buffer[num] = '\0';
                std::string msg(buffer);
                if(msg=="completed")
                {
                    reducer_counter++;
                }
                if(reducer_counter==REDUCER_COUNT)
                {
                    std::cout<<"----------------------"<<std::endl;
                    std::cout<<"finished reducing"<<std::endl;
                    std::cout<<"-----------------------"<<std::endl;
                    for(int j=0;j<REDUCER_COUNT;j++)
                    {
                        close(sockfd_1[j]);
                    }
                }
    }  
    std::cout<<"***********************************"<<std::endl;
    std::cout<<"master exited,Job "<<job_id<<"  completed.."<<std::endl; 
    std::cout<<"**********************************"<<std::endl;
    std::cout<<"============================================================="<<std::endl;
}
    }
}


int main()
{
    int job=1,available_port=8090;
    std::string str;
    while(true)
    {
        std::cout<<"============================================================="<<std::endl;
        std::cin>>str;
        if(str=="word_count")
        {
            std::thread job_thread(master_job,"input.txt",available_port+job,job);
            job_thread.detach();
            job++;
        }
        else if(str=="inverted_words")
        {
            std::thread job_thread(master_job,"input.txt",available_port+job,job);
            job_thread.detach();
            job++;
        }
        else
        {
            std::cout<<"Exited.."<<std::endl;
            break;
        }
    }
    return 0;
}
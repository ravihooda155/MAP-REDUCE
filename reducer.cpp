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
////////////////////////////////////////////

struct node
{
public:
	int file_no;
	std::string val;
		node(int _file_no,std::string _val)
		{
			file_no=_file_no;
			val=_val;
		}	
};
class min_heap
{
    std::vector<node> arr;
public:
    bool isEmpty();
    void pop();
    node heap_top();
    void insert_element(node);
    int size();
    int get_parent(int);
    int get_left_child(int);
    int get_right_child(int);
    void bubble_up(std::vector<node>&,int);
    void bubble_down(std::vector<node>&,int);
};
bool min_heap::isEmpty()
{
    if(arr.size()==0)
        return true;
    return false;
}
node min_heap::heap_top()
{
    return arr[0];
}
int min_heap::get_parent(int i)
{
    return ((i-1)/2);
}
int min_heap::get_left_child(int i)
{
    return (2*i+1);
}
int min_heap::get_right_child(int i)
{
    return (2*i+2);
}
int min_heap::size()
{
    return arr.size();
}
void min_heap::insert_element(node x)
{
    arr.push_back(x);
    /* call heapify here for each insertion */
    bubble_up(arr,arr.size()-1);
}
void min_heap::bubble_down(std::vector<node>& arr,int i)
{
    int left=get_left_child(i);
    int right=get_right_child(i);
    int smallest=i;
    if(left<arr.size() && arr[left].val<arr[i].val)
        smallest=left;
    else 
        smallest=i;
    if(right<arr.size() && arr[smallest].val>arr[right].val)
        smallest=right;
    if(smallest!=i)
    {
        std::swap(arr[i],arr[smallest]);
        bubble_down(arr,smallest);
    }
}
void min_heap::bubble_up(std::vector<node>&,int i)
{
    int parent=get_parent(i);
    if(i>0 && arr[i].val<arr[parent].val)
    {
        std::swap(arr[i],arr[parent]);
        bubble_up(arr,parent);
    }
}
void min_heap::pop()
{
    node minn=arr[0];
    arr[0]=arr[arr.size()-1];
    arr.pop_back();
    bubble_down(arr,0);
}
/////////////////////////////////
///////////////////////////////////
void sort_file(std::string file_name)
{
    std::fstream fileh;
    std::string word;
    std::vector<std::string> names;
    fileh.open(file_name,std::fstream::in);
    while(getline(fileh, word))
        names.push_back(word);
    fileh.close();
    sort(names.begin(), names.end());
    fileh.open(file_name,std::fstream::out);
    for(int i=0;i<names.size();i++)
    {           
        fileh<<names[i]<<"\n";
    }
    fileh.close();
}
void merge_files(std::ifstream *in,std::string f_name)
{
    min_heap heap;
	// make priority queue of first elements of k files
	for(int i=0;i<5;i++)
	{
		std::string str;
		in[i]>>str;
		heap.insert_element(node(i,str));
	}

    std::ofstream out;
    std::string tmp;
    int count=0;
    out.open(f_name,std::fstream::out |std::fstream::app);
    while(count!=5)
    {
   		node n=heap.heap_top();
        out<<n.val<<"\n";
       // std::cout<<"file selected:"<<n.file_no<<std::endl;
        if(in[n.file_no]>>tmp)
        {
            n.val=tmp;
        }
		else
		{
			n.val="zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";
			count++;
		}
		heap.pop();
		heap.insert_element(n);
    }
    out.close();
}




//////////////////////
pthread_mutex_t file_lock; 
void reduce(std::string file_name,std::string job_id)
{
    std::string filename= job_id+"/FinalOutput.txt";
    std::fstream out;
    std::ifstream in;
    std::string str;
    std::unordered_map<std::string,int> words;
    in.open(file_name);
    out.open(filename.c_str(), std::fstream::in | std::fstream::out | std::fstream::app);
    while(in >> str)
    {
        ++words[str];
    }

    for(auto& i:words)
    {
        out<<i.first<<","<<i.second<<"\n";
    }
}
void reducer_task_thread(int new_socket_fd,struct sockaddr_in client_addr,std::string job_id,std::string reducer_id,std::string master_port)
{
    
   
    std::cout<<"-------------------\n";
    mkdir((job_id+"/reduced").c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    struct sockaddr_in master_addr,self_addr;   
    char buffer[1024];
    
    /*int sockfd=socket(AF_INET, SOCK_STREAM, 0);
    bind(sockfd, (struct sockaddr *)&client_addr,sizeof(client_addr));
    
    if(sockfd<0) 
    {
        std::cout<<"failed to open socket"<<"\n";
        close(sockfd);
        std::exit(0);
    }
    bzero((char *) &master_addr, sizeof(master_addr));
    master_addr.sin_family = AF_INET;
    master_addr.sin_addr.s_addr=inet_addr("127.0.0.1");
    master_addr.sin_port = htons(atoi(master_port.c_str()));
/*
    if (connect(sockfd,(struct sockaddr *) &master_addr,sizeof(master_addr)) < 0) 
    {
        std::cout<<"Failed to connect to master"<<"\n";
        close(sockfd);
        std::exit(0);
    }*/
    //uncomment this code
   std::string f_name=job_id+"/reduced/reduced_"+reducer_id;

    for(int i=0;i<5;i++)
    {
        sort_file(job_id+"/in/reducer/"+std::to_string(i+1)+"/"+reducer_id);
    }

    std::ifstream in[5];

    for(int i=0;i<5;i++)
    {
        in[i].open(job_id+"/in/reducer/"+std::to_string(i+1)+"/"+reducer_id);
    }
    merge_files(in,f_name);
    
    std::cout<<"reducer finished:"<<reducer_id<<std::endl; 
    pthread_mutex_lock(&file_lock);
    reduce(f_name,job_id);
    pthread_mutex_unlock(&file_lock);
    std::strcpy(buffer,"completed");
    int n=send(new_socket_fd,buffer,std::strlen(buffer),0);
    std::cout<<"reducer "<<reducer_id<<" exiting"<<std::endl;
}
void init_reducer_connection(int *sockfd,std::string ip,int port)
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
    
    init_reducer_connection(&sockfd,argv[1],std::stoi(argv[2]));
    while(1)
    {
        std::cout<<"reducer listening... "<<std::endl;
        listen(sockfd,10);
        int new_socket_fd= accept(sockfd,(struct sockaddr *) &client_addr,&clilen);
        bzero(buffer,1024);
        int num;
   
        if ((num=recv(new_socket_fd, buffer,1024,0))== -1)
	    {
            perror("recv");
            exit(1);
        }  
        buffer[num] = '\0';
        
        std::string msg(buffer);
        std::vector<std::string> tokens;
		std::stringstream check1(msg);
		std::string rawcode;
		while(getline(check1, rawcode, ','))
		{
			tokens.push_back(rawcode);
		}
        std::string job_id,reducer_id,master_port;
        job_id=tokens[0];
        master_port=tokens[2];
        reducer_id=tokens[1];
        std::thread reducer_task(reducer_task_thread,new_socket_fd,client_addr,job_id,reducer_id,master_port);            
        reducer_task.detach();
    }       
   std::cout<<"reducer exited,task completed.."<<std::endl; 
   std::exit(0);
}

#include<bits/stdc++.h>
#include<fstream>
/*   
	100MB txt file : ~18 seconds (using STL priority_queue)
	2GB txt file: ~ CPU time:450.738 seconds (using STL priority_queue)
	100MB txt file : ~13 seconds  (min-heap using vectors)
	2GB	txt file CPU time: 293.168 seconds (min-heap using vectors)
*/
#define ll long long
using namespace std;
struct node
{
public:
	int file_no;
	string val;
		node(int _file_no,string _val)
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
void min_heap::bubble_down(vector<node>& arr,int i)
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
void min_heap::bubble_up(vector<node>&,int i)
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
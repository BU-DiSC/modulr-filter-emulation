// We can use stl container list as a double 
// ended queue to store the cache keys, with 
// the descending time of reference from front 
// to back and a set container to check presence 
// of a key. But to fetch the address of the key 
// in the list using find(), it takes O(N) time. 
// This can be optimized by storing a reference 
//     (iterator) to each key in a hash map. 
using namespace std; 
#include <bits/stdc++.h> 
#include "LRUcache.h"

// Declare the size 
LRUCache::LRUCache(int n) 
{ 
    capacity = n; 
    csize = 0;
} 

// add a block
void LRUCache::add(string x, vector <string> content) 
{ 
    int size = content.size() * content.at(0).size();
    if ( capacity < size ){
        cout << "error: content is larger than cache size" << endl;
        return;
    }
    // not present in cache 
    if (ma.find(x) == ma.end()) { 
        // cache is full 
        while ( csize+size > capacity) {
            // delete least recently used element 
            string last = dq.back(); 

            vector <string> last_content = buffer[last];
            int last_size = last_content.size() * last_content.at(0).size();
  
            // Pops the last elmeent 
            dq.pop_back(); 
  
            // Erase the last 
            ma.erase(last); 
            buffer.erase(last); 
            csize -= last_size;
        } 
    } 
    // present in cache 
    else
        dq.erase(ma[x]); 
  
    // update reference
    dq.push_front(x); 
    ma[x] = dq.begin(); 
    buffer[x] = content; 
    csize += size;
    //cout << csize << endl;
} 

// Refers key x with in the LRU cache 
vector<string> LRUCache::refer(string x) 
{ 
    vector <string> output;
    // not present in cache 
    if (ma.find(x) == ma.end()) { 
        return output;
    } 
    // present in cache 
    else {
        // update reference 
        dq.erase(ma[x]); 
        dq.push_front(x); 
        ma[x] = dq.begin(); 
        return buffer[x];
    }
} 

// Function to display contents of cache 
void LRUCache::display() 
{ 
    // Iterate in the deque and print 
    // all the elements in it 
    for (auto it = dq.begin(); it != dq.end(); 
         it++) 
        cout << (*it) << endl; 
}


void LRUCache::del(string x) 
{ 
    vector <string> output;
    // not present in cache 
    if (ma.find(x) == ma.end()) { 
        return;
    } 
    // present in cache 
    else {
        // update reference 
        dq.erase(ma[x]); 
        ma.erase(x);
        output = buffer[x]; 
        buffer.erase(x); 
        csize -= output.size()*output.at(0).size();
        return;
    }
} 

// pop least recently used element 
vector<string> LRUCache::pop( string & name ) 
{ 
    string last = dq.back(); 

    vector <string> last_content = buffer[last];
    int last_size = last_content.size() * last_content.at(0).size();
  
    // Pops the last elmeent 
    dq.pop_back(); 
  
    // Erase the last 
    ma.erase(last); 
    buffer.erase(last); 
    csize -= last_size;

    name = last;
    return last_content;
} 

// pop most recently used element 
vector<string> LRUCache::head( string & name ) 
{ 
    string head = dq.front(); 

    vector <string> head_content = buffer[head];
    int head_size = head_content.size() * head_content.at(0).size();
  
    // Pops the last elmeent 
    dq.pop_front(); 
  
    // Erase the last 
    ma.erase(head); 
    buffer.erase(head); 
    csize -= head_size;

    name = head;
    return head_content;
} 

int LRUCache::get_remain()
{ 
    return (capacity-csize);
}

// append a block
void LRUCache::append(string x, vector <string> content) 
{ 
//    int size = content.size() * content.at(0).size();
//    if ( capacity < size ){
//        cout << "error: content is larger than cache size" << endl;
//        return;
//    }
//    // not present in cache 
//    if (ma.find(x) == ma.end()) { 
//        // cache is full 
//        while ( csize+size > capacity) {
//            // delete least recently used element 
//            string last = dq.back(); 
//
//            vector <string> last_content = buffer[last];
//            int last_size = last_content.size() * last_content.at(0).size();
//  
//            // Pops the last elmeent 
//            dq.pop_back(); 
//  
//            // Erase the last 
//            ma.erase(last); 
//            buffer.erase(last); 
//            csize -= last_size;
//        } 
//    } 
//    // present in cache 
//    else
        dq.erase(ma[x]); 
  
    // update reference
    dq.push_back(x); 
    ma[x] = dq.end(); 
    buffer[x] = content; 
    //csize += size;
} 

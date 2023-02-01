#pragma once
#include <bits/stdc++.h> 
using namespace std; 

class LRUCache { 
    // store keys of cache 
    list<string> dq; 
  
    // store references of key in cache 
    unordered_map<string, list<string>::iterator> ma; 
    unordered_map<string, vector <string>> buffer; 

    int capacity; // maximum capacity of cache 
    int csize;    // bytes in the cache
  
public:
    LRUCache(int size); 
    vector<string> refer(string); 
    void add(string, vector<string>); 
    vector<string> pop(string &); 
    vector<string> head(string &); 
    void append(string, vector<string>); 
    void del(string);
    void display(); 
    int get_remain(); 
}; 

// HEADER FILES -----------------------------------------------
#ifndef HEAD
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <fstream>
#include <string.h>
#include <iostream>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <fstream>
#include <string.h>
#include <stdlib.h>
#include <netdb.h>
#include <unistd.h>
#include <signal.h>
#include <sstream>
#include <errno.h>
#include <fcntl.h>
#include <ctime>
#include <ifaddrs.h>
#include <netinet/in.h> 
#include <string.h> 
#include <stdio.h>
#include "md5.h"
#include <bits/stdc++.h>
using namespace std;

// DATA STRUCTURES --------------------------------------------

struct distributedHashTable {
	unordered_map<string, pair<string, string> > DHT;
};

struct routingTable {
	array<tuple<string, string, int>, 5> neighbour_set;
	array<tuple<string, string, int>, 5> leaf_set;
	array< array<tuple<string, string, int>, 16>, 4> routing_set;
	bool rtEntryFlag[4][16];
	bool putFlag[4][16];
	bool getFlag[4][16];
};

// struct routingTable {
// 	vector< pair<string,int> > neighbour_set;
// 	vector< pair<string,int> > leaf_set;
// 	map<string,vector<pair<string,int> > >routing_set;
// };

struct connectionInfo {
	string nodeID;
	string IP;
	int port;
};

struct pHash {
	map<char, int> hash;
};

static set<pair<string, int> > uniqueNSet;
extern connectionInfo connection;
extern routingTable node;
extern distributedHashTable dht;
extern pHash h;

// FUNCTION PROTOTYPES-----------------------------------------
void *startServer(void *connection);
void *handleJoinThread(void *remaining);
void handleJoin(string remaining);
void connectToNetwork(string ip, int port, int toDo, string msg);
void printHashTable();
void printNeighbourSet();
int hexToDec(string hexa);
string decToHex(int n);
// string md5(string a); 

extern string message;

enum ToDo {ADDME = 1, LEAF_SET = 2, ROUTING_TABLE = 3, NEIGHBOUR_SET = 4, FORWARDJOIN = 5, 
	PUT = 6, GET = 7, GETFORWARD = 8, RECVGET = 9, QUIT = 10, SHUTDOWN = 11, QUITDATA = 12};


#endif

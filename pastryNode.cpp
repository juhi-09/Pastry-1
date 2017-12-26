#include "pastry.h"
pthread_t newServerThread;
string delim = "$#$";

string extractPublicIP (void){
    struct ifaddrs * ifAddrStruct=NULL;
    struct ifaddrs * ifa=NULL;
    void * tmpAddrPtr=NULL;
	  string ret;

    getifaddrs(&ifAddrStruct);

    for (ifa = ifAddrStruct; ifa != NULL; ifa = ifa->ifa_next) {
        if (!ifa->ifa_addr)
            continue;
        if (ifa->ifa_addr->sa_family == AF_INET)
        { // IP4
            tmpAddrPtr=&((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
            char addressBuffer[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
      		if(ifa->ifa_name[0] == 'w')
            {
  				ret = string(addressBuffer);
  				if (ifAddrStruct!=NULL) freeifaddrs(ifAddrStruct);
  				return ret;
      			}
        }
    }
    return ret;
}

// Function to connect the node to pastry network
void connectToNetwork(string ip, int port, int toDo, string msg) {
	int client_socket;
    struct sockaddr_in node_addr;

    // Creating the client socket
    client_socket = socket(AF_INET, SOCK_STREAM, 0);

    // Error checking for creation of socket
    if (client_socket < 0) {
        cout << "\nError establishing socket..." << endl;
        close(client_socket);
        exit(1);
    }

    // ip = "10.2.132.129";
    // port = 5000;

    // Specifies that we are using ipv4 and assigning the port number for CRS.
    node_addr.sin_family = AF_INET;
    node_addr.sin_port = htons(port);

    // Converts the IP to binary format and puts it into the server address structure.
    // We use #include <arpa/inet.h> to use this function.
    if (inet_pton(AF_INET, ip.c_str(), &node_addr.sin_addr) != 1) {
        perror("inet_pton failed");
        exit(1);
    }

    // Establishing the connection
    if (connect(client_socket,(struct sockaddr *)&node_addr, sizeof(node_addr)) == 0) {
        cout << "\n------Connection Established with other node through Port: " << port << "------"<<endl;
    }
    else {
        cout << "Could not establish connection\n";
        close(client_socket);
        exit(1);
    }

    int bufSize = 1024;
    string buf;

    if(toDo == ADDME) {
        buf = "";
    	buf += "join";
    	buf += delim;
        buf += connection.nodeID;
        buf += delim;
        buf += connection.IP;
        buf += delim;
        buf += to_string(connection.port);
        buf += delim;
        buf += "connection";
     //    buf += delim;
    	// buf += message;

    	send(client_socket, buf.c_str(), bufSize, 0);
    	cout << "\nAdded To Pastry Network" << endl;
    	close(client_socket);
    }

    else if(toDo == ROUTING_TABLE) {
        buf = "";
        buf += "route";
        buf += delim;
        buf += msg;

        send(client_socket, buf.c_str(), bufSize, 0);
        close(client_socket);
    }

    else if (toDo == FORWARDJOIN) {
        buf = "";
        buf += "forward";
        buf += delim;
        buf += msg; 

        send(client_socket, buf.c_str(), bufSize, 0);
        close(client_socket);
    }

    else if(toDo == PUT) {
        buf = "";
        buf += "put";
        buf += delim;
        buf += msg; 

        send(client_socket, buf.c_str(), bufSize, 0);
        close(client_socket);
    }

    else if(toDo == GET) {
        buf = "";
        buf += "get";
        buf += delim;
        buf += msg; 
        buf += delim;
        buf += connection.IP;
        buf += delim;
        buf += to_string(connection.port);        

        send(client_socket, buf.c_str(), bufSize, 0);
        close(client_socket);
    }

    else if(toDo == RECVGET) {
        buf = "";
        buf += "recvget";
        buf += delim;
        buf += msg; 
        buf += delim;     

        send(client_socket, buf.c_str(), bufSize, 0);
        close(client_socket);
    }

    else if(toDo == GETFORWARD) {
        buf = "";
        buf += "getforward";
        buf += delim;
        buf += msg; 
        buf += delim;     

        send(client_socket, buf.c_str(), bufSize, 0);
        close(client_socket);
    }

    else if(toDo == QUIT) {
        buf = "";
        buf += "quit";
        buf += delim;
        buf += msg; 
        buf += delim;     

        send(client_socket, buf.c_str(), bufSize, 0);
        close(client_socket);
    }

    else if(toDo == SHUTDOWN) {
        buf = "";
        buf += "shutdown";
        buf += delim; 
        buf += msg; 
        buf += delim;   

        send(client_socket, buf.c_str(), bufSize, 0);
        close(client_socket);
    }

    else if(toDo == QUIT) {
        buf = "";
        buf += "quit";
        buf += delim; 
        buf += msg; 
        buf += delim;   

        send(client_socket, buf.c_str(), bufSize, 0);
        close(client_socket);
    }

    else if(toDo == QUITDATA) {
        buf = "";
        buf += "quitdata";
        buf += delim; 
        buf += msg; 
        buf += delim;   

        send(client_socket, buf.c_str(), bufSize, 0);
        close(client_socket);
    }
}

// Function to handle the join request
void handleJoin(string remaining) {
	string join_nodeID, join_IP;
    int join_port;

    int p = remaining.find_first_of(" ");
    join_IP = remaining.substr(0, p);
    join_port = stoi(remaining.substr(p+1));
    join_nodeID = md5(join_IP + to_string(join_port));
    join_nodeID = join_nodeID.substr(0, 4);

    node.neighbour_set[0] = make_tuple(join_nodeID, join_IP, join_port);

    if(join_nodeID < get<0>(node.leaf_set[2])) {
        for (int i = 0; i < 2; i++) {
            if(get<0>(node.leaf_set[i]) == "----") {
                node.leaf_set[i] = make_tuple(join_nodeID, join_IP, join_port);
                break;
            }
        }
    }
    else {
        for (int i = 3; i < 5; i++) {
            if(get<0>(node.leaf_set[i]) == "----") {
                node.leaf_set[i] = make_tuple(join_nodeID, join_IP, join_port);
                break;
            }
        }
    }

    for (int i = 3; i >= 0; i--) {
        if(connection.nodeID.substr(0, i) == join_nodeID.substr(0, i)) {
            if(get<0>(node.routing_set[i][h.hash[join_nodeID[i]]]) == "----") {
                node.routing_set[i][h.hash[join_nodeID[i]]] = make_tuple(join_nodeID, join_IP, join_port);
                break;
            }
            else {

            }
        }
    }

    int toDo = ADDME;
    
    connectToNetwork(join_IP, join_port, ADDME, "");

}

// Function to handle create
void handleCreate() {

    int returnSer;
    
    returnSer = pthread_create(&newServerThread, NULL, startServer, (void *)&connection);
    if(returnSer != 0) {
        perror("Server Thread Creation failed\n");
        exit(EXIT_FAILURE);
    }

    node.leaf_set.fill(make_tuple("----","----", 0));
    node.leaf_set[2] = make_tuple(connection.nodeID, connection.IP, connection.port);

    node.neighbour_set.fill(make_tuple("----","----", 0));
    node.neighbour_set[0] = make_tuple(connection.nodeID, connection.IP, connection.port);

    for(int i = 0 ; i < 4; i++) {
        for (int j = 0; j < 16; j++) {
            node.routing_set[i][j] = make_tuple("----","----", 0);
        }
    }

    for (int i = 0; i < 4; ++i) {
        node.routing_set[i][h.hash[connection.nodeID[i]]] = make_tuple(connection.nodeID, connection.IP, connection.port);
    }
} 

// // Function to handle the put request
void handlePut(string remaining) {

    message = remaining;

	int pos = remaining.find_first_of(" ");
	string key = remaining.substr(0, pos);
	string value = remaining.substr(pos + 1);

	string hashedKey = md5(key);
    hashedKey = hashedKey.substr(0, 6);
    string mapKey = hashedKey.substr(0, 4);

    string diff = decToHex(abs(hexToDec(connection.nodeID) - hexToDec(mapKey)));
    string minKey = diff;
    string diff1 = "ffff", diff2 = "ffff";

    // Handling leaf set
    if(mapKey < connection.nodeID) {
        if(get<0>(node.leaf_set[0]) != "----") {
            diff1 = decToHex(abs(hexToDec(get<0>(node.leaf_set[0])) - hexToDec(mapKey)));
        }
        if(get<0>(node.leaf_set[1]) != "----") {
            diff2 = decToHex(abs(hexToDec(get<0>(node.leaf_set[1])) - hexToDec(mapKey)));
        }
        if(diff1 < diff2) {
            if(diff1 < diff) {
                minKey = diff1;
                connectToNetwork(get<1>(node.leaf_set[0]), get<2>(node.leaf_set[0]), PUT, message);
            }
        }
        else {
            if(diff2 < diff) {
                minKey = diff2;
                connectToNetwork(get<1>(node.leaf_set[1]), get<2>(node.leaf_set[1]), PUT, message);
            }
        }
    }
    else {
        if(get<0>(node.leaf_set[3]) != "----") {
            diff1 = decToHex(abs(hexToDec(get<0>(node.leaf_set[3])) - hexToDec(mapKey)));
        }
        if(get<0>(node.leaf_set[4]) != "----") {
            diff2 = decToHex(abs(hexToDec(get<0>(node.leaf_set[4])) - hexToDec(mapKey)));
        }
        if(diff1 < diff2) {
            if(diff1 < diff) {
                minKey = diff1;
                connectToNetwork(get<1>(node.leaf_set[3]), get<2>(node.leaf_set[3]), PUT, message);
            }
        }
        else {
            if(diff2 < diff) {
                minKey = diff2;
                connectToNetwork(get<1>(node.leaf_set[4]), get<2>(node.leaf_set[4]), PUT, message);
            }
        }
    }

    if(minKey == diff) {
        int i;
        for(i = 3; i >= 1; i--) {
            if(connection.nodeID.substr(0, i) == mapKey.substr(0, i)) {
                vector<string> tempDiffs(16, "ffff");

                for(int j = 0; j < 16; j++) {
                    if(get<0>(node.routing_set[i][j]) != "----") {
                        tempDiffs[j] = decToHex(abs(hexToDec(get<0>(node.routing_set[i][j])) - hexToDec(mapKey)));
                    }
                }

                vector<string>::iterator m = min_element(tempDiffs.begin(), tempDiffs.end());
                if(*m < diff) {
                    minKey = *m;
                    connectToNetwork(get<1>(node.routing_set[i][m - tempDiffs.begin()]), get<2>(node.routing_set[i][m - tempDiffs.begin()]), PUT, message);
                    break;
                }
            }
        }

        if(i == 0) {
            vector<string> tempDiffs(16, "ffff");

            for(int j = 0; j < 16; j++) {
                if(get<0>(node.routing_set[i][j]) != "----") {
                    tempDiffs[j] = decToHex(abs(hexToDec(get<0>(node.routing_set[i][j])) - hexToDec(mapKey)));
                }
            }

            vector<string>::iterator m = min_element(tempDiffs.begin(), tempDiffs.end());
            if(*m < diff) {
                minKey = *m;
                connectToNetwork(get<1>(node.routing_set[i][m - tempDiffs.begin()]), get<2>(node.routing_set[i][m - tempDiffs.begin()]), PUT, message);
            }
        }
        if(minKey == diff) {
            dht.DHT.insert({hashedKey, {key, value}});
        } 
        printHashTable();

    }	
}

void handleGet(string remaining) {
    message = remaining;

    int pos = remaining.find_first_of(" ");
    string key = remaining.substr(0, pos);

    string keyId = md5(key);
    keyId = keyId.substr(0, 6);

    string mapKey = keyId.substr(0, 4);

    string diff = decToHex(abs(hexToDec(connection.nodeID) - hexToDec(mapKey)));
    string minKey = diff;
    string diff1 = "ffff", diff2 = "ffff";

    // Handling leaf set
    if(mapKey < connection.nodeID) {
        if(get<0>(node.leaf_set[0]) != "----") {
            diff1 = decToHex(abs(hexToDec(get<0>(node.leaf_set[0])) - hexToDec(mapKey)));
        }
        if(get<0>(node.leaf_set[1]) != "----") {
            diff2 = decToHex(abs(hexToDec(get<0>(node.leaf_set[1])) - hexToDec(mapKey)));
        }
        if(diff1 < diff2) {
            if(diff1 < diff) {
                minKey = diff1;
                connectToNetwork(get<1>(node.leaf_set[0]), get<2>(node.leaf_set[0]), GET, message);
            }
        }
        else {
            if(diff2 < diff) {
                minKey = diff2;
                connectToNetwork(get<1>(node.leaf_set[1]), get<2>(node.leaf_set[1]), GET, message);
            }
        }
    }
    else {
        if(get<0>(node.leaf_set[3]) != "----") {
            diff1 = decToHex(abs(hexToDec(get<0>(node.leaf_set[3])) - hexToDec(mapKey)));
        }
        if(get<0>(node.leaf_set[4]) != "----") {
            diff2 = decToHex(abs(hexToDec(get<0>(node.leaf_set[4])) - hexToDec(mapKey)));
        }
        if(diff1 < diff2) {
            if(diff1 < diff) {
                minKey = diff1;
                connectToNetwork(get<1>(node.leaf_set[3]), get<2>(node.leaf_set[3]), GET, message);
            }
        }
        else {
            if(diff2 < diff) {
                minKey = diff2;
                connectToNetwork(get<1>(node.leaf_set[4]), get<2>(node.leaf_set[4]), GET, message);
            }
        }
    }

    if(minKey == diff) {
        int i;
        for(i = 3; i >= 1; i--) {
            if(connection.nodeID.substr(0, i) == mapKey.substr(0, i)) {
                vector<string> tempDiffs(16, "ffff");

                for(int j = 0; j < 16; j++) {
                    if(get<0>(node.routing_set[i][j]) != "----") {
                        tempDiffs[j] = decToHex(abs(hexToDec(get<0>(node.routing_set[i][j])) - hexToDec(mapKey)));
                    }
                }

                vector<string>::iterator m = min_element(tempDiffs.begin(), tempDiffs.end());
                if(*m < diff) {
                    minKey = *m;
                    connectToNetwork(get<1>(node.routing_set[i][m - tempDiffs.begin()]), get<2>(node.routing_set[i][m - tempDiffs.begin()]), GET, message);
                    break;
                }
            }
        }

        if(i == 0) {
            vector<string> tempDiffs(16, "ffff");

            for(int j = 0; j < 16; j++) {
                if(get<0>(node.routing_set[i][j]) != "----") {
                    tempDiffs[j] = decToHex(abs(hexToDec(get<0>(node.routing_set[i][j])) - hexToDec(mapKey)));
                }
            }

            vector<string>::iterator m = min_element(tempDiffs.begin(), tempDiffs.end());
            if(*m < diff) {
                minKey = *m;
                connectToNetwork(get<1>(node.routing_set[i][m - tempDiffs.begin()]), get<2>(node.routing_set[i][m - tempDiffs.begin()]), GET, message);
            }
        }
        if(minKey == diff) {
            cout << "-----Key-Value Pair Found in Distributed Hash Table-----" << endl;
            cout << dht.DHT[keyId].first << " " << dht.DHT[keyId].second << endl;
        }

    }
}

// Function to handle quit
void handleQuit() {

    if(!dht.DHT.empty()) {
        int i, j = -1;
    for(i = 3; i >= 0; i--) {        
        j = h.hash[connection.nodeID[i]];

        for(j = j - 1; j >= 0; j--) {
            if(get<0>(node.routing_set[i][j]) == "----")
                continue;
            else {
                break;
            }
        }
        if(j == -1) {
            for(j = h.hash[connection.nodeID[i]]+1; j < 16; j++) {
                if(get<0>(node.routing_set[i][j]) == "----")
                    continue;
                else {
                    break;
                }
            }
        }

        if(j == 16) 
            continue;
        else
            break;
    } 
    if(j != -1 || j != 16) {

        string dhtData = "";
        for(auto k = dht.DHT.begin(); k != dht.DHT.end(); k++) {
            dhtData += (*k).first;
            dhtData += delim;
            dhtData += (*k).second.first;
            dhtData += delim;
            dhtData += (*k).second.second;
            dhtData += delim;
        }
        if(get<0>(node.routing_set[i][j]) == connection.nodeID) {
            ;
        }
        else {
            connectToNetwork(get<1>(node.routing_set[i][j]), get<2>(node.routing_set[i][j]), QUITDATA, dhtData); 
        }
        }
    }
    for(int i = 0; i < 4; i++) {
        for(int j = 0; j < 16; j++) {
            if(get<0>(node.routing_set[i][j]) == "----")
                continue;
            else {
                string msg = "";
                msg += connection.nodeID;
                msg += delim;
                if(get<0>(node.routing_set[i][j]) == connection.nodeID) {
                    continue;
                }
                connectToNetwork(get<1>(node.routing_set[i][j]), get<2>(node.routing_set[i][j]), QUIT, msg);
            }
        }
    }
}

// Function to shut down the pastry network
void handleShutDown() {
    string msg = "shutdown";

    for(int i = 0; i < 4; i++) {
        for(int j = 0; j < 16; j++) {
            if(get<0>(node.routing_set[i][j]) == "----")
                continue;
            else {
                connectToNetwork(get<1>(node.routing_set[i][j]), get<2>(node.routing_set[i][j]), SHUTDOWN, msg);
            }
        }
    }
    cout << "Closing the Pastry Network!" << endl;
    cout << "\nHave a nice day!! :)\n" << endl;
}

// Function to print the leaf set
void printLeafSet() {
	cout << "--------------Leaf Set Data------------------\n\n";
	for (int i = 0; i < 5; ++i) {
        cout << get<0>(node.leaf_set[i]) << " ";
    }
    cout << endl;
}

// Function to print the neighbour set
void printNeighbourSet() {
	cout << "--------------Neighbour Set Data------------------\n\n";
    for (int i = 0; i < 5; ++i) {
        cout << get<0>(node.neighbour_set[i]) << " ";
    }
    cout << endl;
}

// Function to print the routing set
void printRoutingSet() {
	cout << "--------------Routing Set Data------------------\n\n";
	for(int i = 0 ; i < 4; i++) {
        for (int j = 0; j < 16; j++) {
            cout << get<0>(node.routing_set[i][j]) << " ";
        }
        cout << endl;
    }
}

//  Function to print the routing set
void printHashTable() {
	cout << "--------------DHT Data------------------\n\n";
	for(auto i = dht.DHT.begin(); i != dht.DHT.end(); i++) {
		cout << i->first << " " << i->second.first << " " << i->second.second << endl;		
	}
}

// Function to handle inputs from user
void inputHandling() {
	
	string input;

	while(1) {
		getline(cin, input);
		int pos = input.find_first_of(" ");
		string command = input.substr(0, pos);
		string remaining = input.substr(pos+1);

		if(command == "put") {
            if(remaining != "")
			    handlePut(remaining);
		}
		else if(command == "get") {
            if(remaining != "")
			    handleGet(remaining);
		}
		else if(command == "join") {
            if(remaining != "")
	      	    handleJoin(remaining);

		}
        else if(command == "create") {
            handleCreate();
        }
		else if(command == "lset") {
	      	printLeafSet();
		}
		else if(command == "nset") {
	      	printNeighbourSet();
		}
		else if(command == "routetable") {
	      	printRoutingSet();
		}
		else if(command == "hashtable") {
	      	printHashTable();
		}
        else if(command == "quit") {
            handleQuit();
            break;
        }
        else if(command == "shutdown") {
            handleShutDown();
            cout << "Closing the Pastry Network!" << endl;
            cout << "\nHave a nice day!! :)\n" << endl;
            break;
        }
	}
}

int main(int argc, char* argv[]) {

    for(int i = 0; i < 4; i++) {
        for(int j = 0; j < 16; j++) {
            node.rtEntryFlag[i][j] = 0;
        }
    }
     for(int i = 0; i < 4; i++) {
        for(int j = 0; j < 16; j++) {
            node.putFlag[i][j] = 0;
        }
    }
     for(int i = 0; i < 4; i++) {
        for(int j = 0; j < 16; j++) {
            node.getFlag[i][j] = 0;
        }
    }

    h.hash.insert({'0', 0});
    h.hash.insert({'1', 1});
    h.hash.insert({'2', 2});
    h.hash.insert({'3', 3});
    h.hash.insert({'4', 4});
    h.hash.insert({'5', 5});
    h.hash.insert({'6', 6});
    h.hash.insert({'7', 7});
    h.hash.insert({'8', 8});
    h.hash.insert({'9', 9});
    h.hash.insert({'a', 10});
    h.hash.insert({'b', 11});
    h.hash.insert({'c', 12});
    h.hash.insert({'d', 13});
    h.hash.insert({'e', 14});
    h.hash.insert({'f', 15});

	string input;
	getline(cin, input);
	connection.IP = extractPublicIP();

	cout << "My IP: " << connection.IP << endl;

	int pos = input.find_first_of(" ");
	string command = input.substr(0, pos);
	string remaining = input.substr(pos+1);
	string temp,port;

	if(command == "port") {
		connection.port = stoi(remaining);
		temp += connection.IP;
		temp += remaining;
		connection.nodeID = md5(temp);
		connection.nodeID = connection.nodeID.substr(0, 4);
		cout << "-------------NodeID generated after MD5 Hashing----------------------\n\n";
		cout << "Node ID " << connection.nodeID << endl;
	}

	inputHandling();

	return 0;
}

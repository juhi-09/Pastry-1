#include "pastry.h"

distributedHashTable dht;
pHash h;
routingTable node;
string message;
connectionInfo connection;
connectionInfo recvDetails;
string sendIP;
string delim1 = "$#$";

int hexToDec(string hexa) {
    int ans = 0;
    for(int i = hexa.length()-1; i >= 0; i--) {     
        ans += h.hash[hexa[i]] * pow(16, hexa.length()-1-i);
    }
    return ans;
}

string decToHex(int n) {
    char hexaDeciNum[100];

    int i = 0;
    while(n!=0)
    {   
        int temp  = 0;        
        temp = n % 16;        
        if(temp < 10) {
            hexaDeciNum[i] = temp + 48;
            i++;
        }
        else {
            hexaDeciNum[i] = temp + 55;
            i++;
        }        
        n = n/16;
    }

    string ans = hexaDeciNum;
    ans[4] = '\0';
    return ans;    
}

vector<string> split(string s){
    vector<string> list;
    size_t pos;
    string token;
    while ((pos = s.find(delim1)) != string::npos) {
        token = s.substr(0, pos);
        list.push_back(token);
        s.erase(0, pos + delim1.length());
    }

    if(s.find('\0') != string::npos){
        token = s.substr(0, s.find('\0'));
        list.push_back(token);
    }
    else if(s.find('\0') == string::npos){
        list.push_back(s);
    }
    return list;
}

// Thread handler function for join
void *handleJoinThread(void *recvDetails) {
 
    connectionInfo ci = *(struct connectionInfo *)recvDetails;
    
    string routeData; 

    for (int i = 0; i < 5; i++) {
        if(get<0>(node.neighbour_set[i]) == "----") {
            node.neighbour_set[i] = make_tuple(ci.nodeID, ci.IP, ci.port);
            break;
        }
    }

    int rowToSend = 0;
    int flag = 0;
    
    for (int i = 3; i >= 0; i--) {
        if(connection.nodeID.substr(0, i) == ci.nodeID.substr(0, i)) {
            if(get<0>(node.routing_set[i][h.hash[ci.nodeID[i]]]) == "----") {
                node.routing_set[i][h.hash[ci.nodeID[i]]] = make_tuple(ci.nodeID, ci.IP, ci.port);
                rowToSend = i;
                routeData = ""; 
    
                for(int k = 0; k <= rowToSend; k++) {
                    for(int j = 0; j < 16; j++) {
                        routeData += (get<0>(node.routing_set[k][j]) + delim1 + get<1>(node.routing_set[k][j]) + delim1 + to_string(get<2>(node.routing_set[k][j])) + delim1);
                    } 
                }

                connectToNetwork(ci.IP, ci.port, ROUTING_TABLE, routeData);
                break;
            }
            else {
                // Send join message to node.routing_set[i][h.hash[ci.nodeID[i]]] 
                routeData = "";
                routeData += ci.nodeID;
                routeData += delim1;
                routeData += ci.IP;
                routeData += delim1;
                routeData += to_string(ci.port);
                routeData += delim1;
                routeData += "connection";
                flag = 1;
                if(get<0>(node.routing_set[i][h.hash[ci.nodeID[i]]]) != "----" && get<0>(node.routing_set[i][h.hash[ci.nodeID[i]]]) != ci.nodeID && get<0>(node.routing_set[i][h.hash[ci.nodeID[i]]]) != connection.nodeID)
                    connectToNetwork(get<1>(node.routing_set[i][h.hash[ci.nodeID[i]]]), get<2>(node.routing_set[i][h.hash[ci.nodeID[i]]]), FORWARDJOIN, routeData);

                break;
            }
        }
    } 

    if(!flag) {
        routeData = "";
        routeData += ci.nodeID;
        routeData += delim1;
        routeData += ci.IP;
        routeData += delim1;
        routeData += to_string(ci.port);
        routeData += delim1;
        routeData += "connection";
    }
    for(int j = 0; j < 16; j++) {
        if(node.rtEntryFlag[rowToSend][j] == false && get<0>(node.routing_set[rowToSend][j]) != "----" && get<0>(node.routing_set[rowToSend][j]) != ci.nodeID && get<0>(node.routing_set[rowToSend][j]) != connection.nodeID) {
            node.rtEntryFlag[rowToSend][j] = true;
            connectToNetwork(get<1>(node.routing_set[rowToSend][j]), get<2>(node.routing_set[rowToSend][j]), FORWARDJOIN, routeData);
        }
    }  

   /* vector<tuple< string, string, int> > nodes, uniqueNodes;

    for(int i = 0; i < 4; i++) {
        for(int j = 0; j < 16; j++) {
            if(get<0>(node.routing_set[i][j]) == "----")
                continue;
            nodes.push_back(node.routing_set[i][j]);
        }
    }

    sort(nodes.begin(), nodes.end());
    vector<tuple< string, string, int> >::iterator it = unique(nodes.begin(), nodes.end());

    cout << "In Handle Join Thread" << endl;

    for(auto i = nodes.begin(); i != it; i++) {
        uniqueNodes.push_back(*i);
    }

    int k;
    for(auto it = uniqueNodes.begin(); it != uniqueNodes.end(); it++) {
        if(get<0>(*it) == connection.nodeID) {
            k = it - uniqueNodes.begin();
            break;
        }
    }

    for(int i = 0; i < 5; i++) {
        if(i == 2) {
            node.leaf_set[i] = make_tuple(connection.nodeID, connection.IP, connection.port);
        }
        else {
            node.leaf_set[i] = make_tuple("----","----", 0); 
        }
    }

    cout << "In Handle Join Thread after for" << endl;

    if(k-1>=0)
       node.leaf_set[1] = uniqueNodes[k-1];
    if(k-2>=0)
       node.leaf_set[0] = uniqueNodes[k-2];
    if(k+1<=uniqueNodes.size()-1)
       node.leaf_set[3] = uniqueNodes[k+1];
    if(k+2<=uniqueNodes.size()-1)
       node.leaf_set[4] = uniqueNodes[k+2];
    */

	pthread_exit(0);
}

// Thread handler function for put
void *handlePutThread(void *remain) {

    string remaining = *(string *)remain;
    message = remaining;
    int pos = remaining.find_first_of(" ");
    string key = remaining.substr(0,pos);
    remaining = remaining.substr(pos + 1);
    string value = remaining;

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

	pthread_exit(0); 
}

// Thread handler function for get
void *handleGetThread(void *remain) {

    string remaining = *(string *)remain;
    
	message = remaining;
	int pos = remaining.find_first_of("$");
	string key = remaining.substr(0, pos);
	string connectionDetails = remaining.substr(pos + delim1.length());
	
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
                connectToNetwork(get<1>(node.leaf_set[0]), get<2>(node.leaf_set[0]), GETFORWARD, message);
            }
        }
        else {
            if(diff2 < diff) {
                minKey = diff2;
                connectToNetwork(get<1>(node.leaf_set[1]), get<2>(node.leaf_set[1]), GETFORWARD, message);
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
                connectToNetwork(get<1>(node.leaf_set[3]), get<2>(node.leaf_set[3]), GETFORWARD, message);
            }
        }
        else {
            if(diff2 < diff) {
                minKey = diff2;
                connectToNetwork(get<1>(node.leaf_set[4]), get<2>(node.leaf_set[4]), GETFORWARD, message);
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
                    connectToNetwork(get<1>(node.routing_set[i][m - tempDiffs.begin()]), get<2>(node.routing_set[i][m - tempDiffs.begin()]), GETFORWARD, message);
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
                connectToNetwork(get<1>(node.routing_set[i][m - tempDiffs.begin()]), get<2>(node.routing_set[i][m - tempDiffs.begin()]), GETFORWARD, message);
            }
        }
        if(minKey == diff) {
            int pos = connectionDetails.find_first_of('$');
            string forwardIP = connectionDetails.substr(0, pos);
            string forwardPort = connectionDetails.substr(pos+delim1.length());
            message = dht.DHT[hashedKey].first + " " + dht.DHT[hashedKey].second;
            connectToNetwork(forwardIP, stoi(forwardPort), RECVGET, message);  
        } 
    }   
    
    pthread_exit(0);
}

// Thread handler function
void *startServer(void *connection) {

	connectionInfo con = *(struct connectionInfo *)connection;

	// Identifier of the socket getting created.
	int server_socket;

    struct sockaddr_in server_addr;
    socklen_t size;

    // Creating the socket
    server_socket = socket(AF_INET, SOCK_STREAM, 0);

    // Error checking for creation of socket
    if (server_socket < 0) {
        perror("\nError establishing socket...\n");
        exit(1);
    }

    
    // Initializing the server address data structure.
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = inet_addr(con.IP.c_str());
    server_addr.sin_port = htons(con.port);
    size = sizeof(server_addr);

    // Binding the port to socket
    if ((bind(server_socket, (struct sockaddr*)&server_addr, sizeof(server_addr))) < 0) {
        perror("Error Binding connection in Server\n");
        exit(1);
    }

    // Listening for nodes
    cout << "Listening for Nodes on port " << con.port << endl;
    listen(server_socket, 5);

	int returnVal;

	while(1) {    	
    	
    	// Accept the connection from other nodes 
	    returnVal = accept(server_socket,(struct sockaddr *)&server_addr, &size);

	    // Error checking for accept() call
	    if (returnVal < 0) 
	        perror("Error on accepting the connection");

	    int bufSize = 1024;
	    char buffer[bufSize];
	    string buf;

    	bzero(buffer, bufSize);	   
    	recv(returnVal, buffer, bufSize, 0);
    	
        buf = buffer;
    	string token;

    	int pos = buf.find_first_of(delim1);
    	token = buf.substr(0, pos);
    	string remaining = buf.substr(pos+delim1.length());
        // Receive a Join Message
    	if(token == "join") {
            string conn = "connection";
            int posCon = remaining.find_first_of('c');
            string details = remaining.substr(0, posCon);

            int pos1 = details.find_first_of('$');
            string recvNodeId = details.substr(0, pos1);
            details = details.substr(pos1+delim1.length());
            pos1 = details.find_first_of('$');
            string receivedIP = details.substr(0, pos1);
            details = details.substr(pos1+delim1.length());
            pos1 = details.find_first_of('$');
            int receivedPort = stoi(details.substr(0, pos1));

            connectionInfo recvDetails;
            recvDetails.nodeID = recvNodeId;
            recvDetails.IP = receivedIP;
            recvDetails.port = receivedPort;

    		// get the remaining string from the buffer and pass it to handleJoin.
    		pthread_t newJoinThread;
    		int threadJoin;

            for(int i = 0; i < 4; i++) {
                for(int j = 0; j < 16; j++) {
                    node.rtEntryFlag[i][j] = 0;
                }
            }

    		threadJoin = pthread_create(&newJoinThread, NULL, handleJoinThread, (void *)&recvDetails);
    		if(threadJoin != 0) {
				perror("Thread Join Creation failed\n");
				exit(EXIT_FAILURE);
			}

			void* thread_result;
			threadJoin = pthread_join(newJoinThread, &thread_result);
			if (threadJoin != 0) {
				perror("Thread join failed");
				exit(EXIT_FAILURE);
			}
			cout << "Added to Pastry Network" << endl;
            close(returnVal);
    	}
        else if(token == "route") {

            vector<string> data = split(remaining);
            int rows = data.size() / 48;
            for(int i = 0; i < rows; i++) {
                for(int j = 0; j < 48; j+=3) {
                    node.routing_set[i][j/3] = make_tuple(data[j], data[j+1], stoi(data[j+2]));
                }
            }

            vector<tuple< string, string, int> > nodes, uniqueNodes;

            for(int i = 0; i < 4; i++) {
                for(int j = 0; j < 16; j++) {
                    if(get<0>(node.routing_set[i][j]) == "----")
                        continue;
                    nodes.push_back(node.routing_set[i][j]);
                }
            }

            sort(nodes.begin(), nodes.end());
            vector<tuple< string, string, int> >::iterator it = unique(nodes.begin(), nodes.end());

            for(auto i = nodes.begin(); i != it; i++) {
                uniqueNodes.push_back(*i);
            }

            int k;
            for(auto it = uniqueNodes.begin(); it != uniqueNodes.end(); it++) {
                if(get<0>(*it) == con.nodeID) {
                    k = it - uniqueNodes.begin();
                    break;
                }
            }

            for(int i = 0; i < 5; i++) {
                if(i == 2) {
                    node.leaf_set[i] = make_tuple(con.nodeID, con.IP, con.port);
                }
                else {
                    node.leaf_set[i] = make_tuple("----","----", 0); 
                }
            }
            
            if(k-1>=0)
               node.leaf_set[1] = uniqueNodes[k-1];
            if(k-2>=0)
               node.leaf_set[0] = uniqueNodes[k-2];
            if(k+1<=uniqueNodes.size()-2)
               node.leaf_set[3] = uniqueNodes[k+1];
            if(k+2<=uniqueNodes.size()-1)
               node.leaf_set[4] = uniqueNodes[k+2];

            close(returnVal);
        }
        else if(token == "forward") {
            string conn = "connection";
            int posCon = remaining.find_first_of('c');
            string details = remaining.substr(0, posCon);

            // remaining = remaining.substr(posCon + conn.length() + delim1.length());
            
            int pos1 = details.find_first_of('$');
            string recvNodeId = details.substr(0, pos1);
            details = details.substr(pos1+delim1.length());
            pos1 = details.find_first_of('$');
            string receivedIP = details.substr(0, pos1);
            details = details.substr(pos1+delim1.length());
            pos1 = details.find_first_of('$');
            int receivedPort = stoi(details.substr(0, pos1));

            connectionInfo recvDetails;
            recvDetails.nodeID = recvNodeId;
            recvDetails.IP = receivedIP;
            recvDetails.port = receivedPort;

            // get the remaining string from the buffer and pass it to handleJoin.
            pthread_t newForwardThread;
            int threadForward;
            
            threadForward = pthread_create(&newForwardThread, NULL, handleJoinThread, (void *)&recvDetails);
            if(threadForward != 0) {
                perror("Thread Join Creation failed\n");
                exit(EXIT_FAILURE);
            }

            void* thread_forward;
            threadForward = pthread_join(newForwardThread, &thread_forward);
            if (threadForward != 0) {
                perror("Thread join failed");
                exit(EXIT_FAILURE);
            }
            close(returnVal);
        }

        else if(token == "put") {
            message = remaining;
            pthread_t putThread;
            int threadPUT;
            
            threadPUT = pthread_create(&putThread, NULL, handlePutThread, (void*)&remaining);
            if(threadPUT != 0) {
                perror("Thread Put Creation failed\n");
                exit(EXIT_FAILURE);
            }

            void* thread_put;
            threadPUT = pthread_join(putThread, &thread_put);
            if (threadPUT != 0) {
                perror("Thread Put failed");
                exit(EXIT_FAILURE);
            }
            close(returnVal);
        }

        else if(token == "get") {
            message = remaining;
            pthread_t getThread;
            int threadGET;
            
            threadGET = pthread_create(&getThread, NULL, handleGetThread, (void*)&remaining);
            if(threadGET != 0) {
                perror("Thread Get Creation failed\n");
                exit(EXIT_FAILURE);
            }

            void* thread_get;
            threadGET = pthread_join(getThread, &thread_get);
            if (threadGET != 0) {
                perror("Thread Get failed");
                exit(EXIT_FAILURE);
            }
            close(returnVal);
        }

        else if(token == "recvget") {
            cout << "-----Key-Value Pair Found in Distributed Hash Table-----" << endl;
            int pos = remaining.find_first_of('$');
            remaining = remaining.substr(0, pos);
            cout << remaining << endl;
            close(returnVal);
        }

        else if(token == "getforward") {
            message = remaining;
            pthread_t getForwardThread;
            int threadGETForward;
            
            threadGETForward = pthread_create(&getForwardThread, NULL, handleGetThread, (void*)&remaining);
            if(threadGETForward != 0) {
                perror("Thread Get Forward Creation failed\n");
                exit(EXIT_FAILURE);
            }

            void* thread_get_Forward;
            threadGETForward = pthread_join(getForwardThread, &thread_get_Forward);
            if (threadGETForward != 0) {
                perror("Thread Get Forward failed");
                exit(EXIT_FAILURE);
            }
            close(returnVal);
        }

        else if(token == "shutdown") {
            dht.DHT.clear();
            cout << "Closing the Pastry Network!" << endl;
            cout << "\nHave a nice day!! :)\n" << endl;
            close(returnVal);
            exit(0);
        }

        else if(token == "quitdata") {
            vector<string> data = split(remaining);
            int values = data.size() / 3;
            for(int i = 0; i < data.size(); i+=3) {
                dht.DHT.insert({data[i], {data[i+1], data[i+2]}});
            }
            cout << "Handled Quit Data" << endl;
            close(returnVal);
        }

        else if(token == "quit") {
            int pos = remaining.find_first_of("$");
            string key = remaining.substr(0, pos);

            for(int i = 0; i < 5; i++) {
                if(get<0>(node.leaf_set[i]) == key) {
                    node.leaf_set[i] = make_tuple("----", "----", 0);
                }
            }

            for(int i = 0; i < 5; i++) {
                if(get<0>(node.neighbour_set[i]) == key) {
                    node.neighbour_set[i] = make_tuple("----", "----", 0);
                }
            }

            for (int i = 0; i < 4; i++) {
                for (int j = 0; j < 16; j++) {
                    if(get<0>(node.routing_set[i][j]) == key) {
                        node.routing_set[i][j] = make_tuple("----", "----", 0);
                    }
                }
            }
            cout << "Handled Quit" << endl;
            close(returnVal);
        }
	}
	pthread_exit(0);
}

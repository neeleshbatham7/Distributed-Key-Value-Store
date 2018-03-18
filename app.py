# ----------------------------------LIBRARIES--------------------------------- #

from flask import Flask, request, jsonify
import os, socket, time, re, sys, requests, hashlib, time, threading, math, json

# ----------------------------------APP_NAME--------------------------------- #

app = Flask(__name__)

# ----------------------------------VARIABLES--------------------------------- #

replicas = {} # this node's partition replicas
data = {}      # this node's data
down_nodes = [] # this node's down_nodes
IP_PORT = os.environ.get('ip_port') # this nodes network address

#--------------------------------HELPER_FUNCTIONS------------------------------#

def get_view():
    return os.environ.get('VIEW')

def get_partition_replica_size():
    return int(os.environ.get('K')) # partion replica size K

def get_node_addresses():
    return get_view().split(',')

def get_number_of_nodes():
    return len(get_node_addresses())

def get_partition_id(node_address):
    partition_size = get_partition_replica_size()
    node_addresses = get_node_addresses()
    partition_id = node_addresses.index(node_address) / partition_size
    return int(partition_id)

def get_node_id(node_address):
    return int(get_node_addresses().index(node_address))

def get_partition_members(address):
    partition_members = []
    partition_id = get_partition_id(address)
    for node_address in get_node_addresses():
        if partition_id == get_partition_id(node_address):
            partition_members.append(str(node_address))
    return partition_members

def get_node_index_relative_to_partition(address):
    return int(get_partition_members(address).index(address))

def get_number_of_partitions():
    return int(math.ceil(float(get_number_of_nodes())/float(get_partition_replica_size())))

def get_number_of_keys():
    return len(data)

def get_all_partition_ids():
    partition_ids = []
    for i in range(int(get_number_of_partitions())):
        partition_ids.append(int(i))
    return partition_ids

def get_partition_members_from_index(partition_index):
    for node_address in get_node_addresses():
        if get_partition_id(node_address)==partition_index:
            return get_partition_members(node_address)
    return "error"

def get_fellow_partition_members(address):
    fellow_members = get_partition_members(address)
    fellow_members.remove(address)
    return fellow_members

def vector_to_string(cp):
    cp_string = ""
    for i in cp[:-1]:
        cp_string += str(i) + '.'
    cp_string += str(cp[-1])
    return cp_string

def compare_causal_payloads(a, b):
    sys.stdout.flush()

    smaller_payload = None
    if (len(a) != len(b)):
        return 'error'
    for x in range(len(a)):
        if (a[x] == b[x]):
            continue
        elif (a[x] < b[x]):
            if (smaller_payload == None):
                smaller_payload = 'a'
            elif (smaller_payload == 'a'):
                continue
            else:
                return "concurrent"
        elif (a[x] > b[x]):
            if (smaller_payload == None):
                smaller_payload = 'b'
            elif (smaller_payload == 'b'):
                continue
            else:
                return "concurrent"
    if (smaller_payload == 'a'):
        return b
    elif (smaller_payload == 'b'):
        return a
    else:
        return 'concurrent'

def compare_timestamps(a,b):
    if a > b:
        return a
    elif b > a:
        return b
    else:
        return "concurrent"

def compare_ids(a,b):
    if (a == b):
        return "concurrent"
    if a > b:
        return a
    else:
        return b

# a = [[x.y.z],timestamp,id_number]
# b = [[x.y.z],timestamp,id_number]
def compare_causal_order(a,b):
    a_causal_payload = a[0]
    a_timestamp = a[1]
    a_id = a[2]
    b_causal_payload = b[0]
    b_timestamp = b[1]
    b_id = b[2]

    a_causal_payload = [int(x) for x in a_causal_payload]
    b_causal_payload = [int(x) for x in b_causal_payload]

    # Compare causal payloads first
    response = compare_causal_payloads(a_causal_payload, b_causal_payload)
    if response == a_causal_payload:
        return a
    elif response == b_causal_payload:
        return b
    # Compare timestamps if causal payloads are concurrent
    elif response == 'concurrent':
        response_time = compare_timestamps(a_timestamp, b_timestamp)
        if response_time == a_timestamp:
            return a
        elif response_time == b_timestamp:
            return b
        # Compare ids if timestamps are conccurent
        elif response_time == 'concurrent':
                response_id = compare_ids(a_id,b_id)
                if response_id == a_id:
                    return a
                elif response_id == b_id:
                    return b
                else:
                    return 'concurrent'

    return 'error'


def update_causal_payload(address, vector_payload):
    index = int(get_node_index_relative_to_partition(address))
    ret_payload = []
    for i in range(len(vector_payload)):
        x = vector_payload[i]
        if i == index:
            x += 1
        ret_payload.append(x)
    return ret_payload

@app.before_first_request
def begin_pinging():
    def ping_replicas():
        ping_count = 0
        while True:
            time.sleep(0.33)
            ping_count += 1
            for node_address in get_partition_members(IP_PORT):
                if (node_address == IP_PORT):
                    continue
                else:
                    try:
                         # ping memeber
                         url = 'http://' + node_address + '/ping'
                         response = None
                         response = requests.get(url, timeout=1)
                         replicas[node_address] = True
                    except:
                        replicas[node_address] = False
            time.sleep(0.33)
            up_nodes = []
            print "HEALING", str(ping_count)
            print "CONTENTS OF down_nodes: ", down_nodes
            sys.stdout.flush()
            for node_address in down_nodes:
                if replicas[node_address]:
                    payload = {"data": json.dumps(data)}
                    print payload
                    url = 'http://' + node_address + '/mega_backup'
                    response = requests.put(url, data=payload)
                    print "CONTENT", response.content
                    if response.status_code == 432:
                        up_nodes.append(node_address)
                    sys.stdout.flush()

            for node_address in up_nodes:
                if node_address in down_nodes:
                    down_nodes.remove(node_address)

            print "REPLICAS", replicas, str(ping_count)
            sys.stdout.flush()
            time.sleep(0.33)

    thread = threading.Thread(target=ping_replicas)
    thread.start()

# ------------------------------------HASH----------------------------------- #

# returns a partition's index based on hash value of key
# key->[hash]->node_index
def hash(key, partition_number):

    hash_object = hashlib.sha256(key) # sha256
    hex_value = hash_object.hexdigest()
    key_value = int(hex_value, 16)

    # returns a partition id index
    return key_value % partition_number

# ----------------------------------KVS_ROUTE--------------------------------- #

@app.route('/kvs', methods=['GET', 'PUT', 'DELETE'])
def check():

    if request.method == 'GET':

        key = str(request.args.get('key')) # get the key
        causal_payload = str(request.args.get('causal_payload'))
        my_partition_index = get_partition_id(IP_PORT) # local partition
        key_index = hash(key, get_number_of_partitions()) # where the key belongs

        # if the partition_index the same as my local partition index
        if my_partition_index == key_index:

            if not (re.match('^[A-Za-z0-9_]*$', key)):
                temp = {'msg':'error',
                        'error':'incorrect charset given',
                        'owner':IP_PORT
                        }
                return jsonify(temp), 404
            if (len(key) > 250):
                temp = {'msg':'error', 'error':'key too long', 'owner':IP_PORT}
                return jsonify(temp), 404

            if (key in data):
                data_payload = data[key]
                # if(causal_payload == vector_to_string(data_payload[1])):
                temp = {'msg':'success',
                        'value':data_payload[0],
                        'partition_id':int(data_payload[3]),
                        'causal_payload':vector_to_string(data_payload[1]),
                        'timestamp':data_payload[2]}
                return jsonify(temp), 200


                # else:
                #     temp = {'msg':'error',
                #             'error':'incorrect causal payload ' + causal_payload,
                #             'my_causal_payload': vector_to_string(data_payload[1]),
                #             'partition_id':get_partition_id(IP_PORT),
                #             }
                #
                #     return jsonify(temp), 404
            else:
                temp = {'msg':'error',
                        'error':'key does not exist',
                        'partition_id':get_partition_id(IP_PORT),
                        }

                return jsonify(temp), 404

        else:
            node_addresses = get_partition_members_from_index(key_index)
            print "NODE ADDRESSES " + str(node_addresses)
            sys.stdout.flush()
            for node_address in node_addresses:
                url = 'http://' + node_address + '/kvs?key=' + key + '&causal_payload=' + causal_payload
                try:
                    response = requests.get(url, timeout=1)
                    return response.content, 200
                except:
                    continue

            response = {'msg':'error','error':'could not send key to partition ' + str(key_index)}
            return jsonify(response), 999

    elif request.method == 'PUT':

        timestamp = time.time()
        key = str(request.form['key']) # get the key
        my_partition_index = get_partition_id(IP_PORT) # this nodes partition index
        key_index = hash(key, get_number_of_partitions()) # keys hash index

        if (key_index == my_partition_index):
            print "IN HERE"
            sys.stdout.flush()
            if not ('value' in request.form and 'causal_payload' in request.form):
                temp = {'msg':'error',
                        'error':'service is not available',
                        'owner':IP_PORT
                        }
                return jsonify(temp), 404

            value = str(request.form['value'])

            if not (re.match('^[A-Za-z0-9_]*$', key)):
                temp = {'msg':'error',
                        'error':'incorrect charset given',
                        'owner':IP_PORT
                        }
                return jsonify(temp), 404

            if (len(key) > 250):
                temp = {'msg':'error', 'error':'key too long', 'owner':IP_PORT}
                return jsonify(temp), 404

            causal_payload = str(request.form['causal_payload'])

            id_number = int(get_partition_id(IP_PORT))
            # if supplied with empty causal payload
            if (causal_payload == ''):

                # if (key in data):
                #         my_causal_payload = data[key][1]
                #         response_payload = {'msg' : 'error',
                #                             'error' : 'key already exists with existing causal_payload ' + vector_to_string(my_causal_payload),
                #                             'partition_id' : get_partition_id(IP_PORT),
                #                            }
                #
                #         return jsonify(response_payload), 436

                causal_payload = [0 for x in range(len(get_partition_members(IP_PORT)))]
                my_causal_payload = update_causal_payload(IP_PORT, causal_payload)

                key_payload = { 'key' : key,
                                'value' : value,
                                'causal_payload' : vector_to_string(my_causal_payload),
                                'timestamp' : timestamp,
                                'id' : id_number,
                                'source_address': IP_PORT
                               }

                for node_address in get_fellow_partition_members(IP_PORT):
                    if replicas[node_address] == True:
                        url = 'http://' + node_address + '/backup'
                        response = requests.put(url, data=key_payload)
                    else:
                        # add to the queue
                        if not node_address in down_nodes:
                            down_nodes.append(node_address)
                        continue

                # Updating local information
                data_payload = []
                data_payload.append(value)
                data_payload.append(my_causal_payload)
                data_payload.append(timestamp)
                data_payload.append(id_number)
                data[key] = data_payload

                response_payload = {'msg' : 'success',
                                    'partition_id' : get_partition_id(IP_PORT),
                                    'causal_payload' : vector_to_string(my_causal_payload),
                                    'timestamp' : timestamp,
                                    'source_address' : str(IP_PORT)
                                   }

                return jsonify(response_payload), 200

            else:
                # getting non-empty causal payload
                my_causal_payload = data[key][1]
                print my_causal_payload
                sys.stdout.flush()

                if vector_to_string(my_causal_payload) == causal_payload:
                    causal_payload  = [int(x) for x in causal_payload.split('.')]
                    my_causal_payload = update_causal_payload(IP_PORT, causal_payload)

                    key_payload = { 'key' : key,
                                    'value' : value,
                                    'causal_payload' : vector_to_string(my_causal_payload),
                                    'timestamp' : timestamp,
                                    'id' : id_number,
                                    'source_address': IP_PORT
                                   }
                    # updating other replicas info
                    for node_address in get_fellow_partition_members(IP_PORT):
                        if replicas[node_address] == True:
                            url = 'http://' + node_address + '/backup'
                            response = requests.put(url, data=key_payload)
                        else:
                            # add to the queue
                            if not node_address in down_nodes:
                                down_nodes.append(node_address)
                            continue
                    # Updating local information
                    data_payload = []
                    data_payload.append(value)
                    data_payload.append(my_causal_payload)
                    data_payload.append(timestamp)
                    data_payload.append(id_number)
                    data[key] = data_payload

                    response_payload = {'msg' : 'success',
                                        'partition_id' : get_partition_id(IP_PORT),
                                        'causal_payload' : vector_to_string(my_causal_payload),
                                        'timestamp' : timestamp,
                                        'source_address': str(IP_PORT)
                                       }

                    return jsonify(response_payload), 200

                else:
                    response_payload = {'msg': 'causal_payload does not exist on this node',
                                        'causal_payload':vector_to_string(my_causal_payload)}
                    return jsonify(response_payload), 888

        else:

            forwarding_payload = request.form
            node_addresses = get_partition_members_from_index(key_index)
            print "NODE ADDRESSES " + str(node_addresses)
            sys.stdout.flush()
            for node_address in node_addresses:
                try:
                    url = 'http://' + node_address + '/kvs'
                    response = requests.put(url, data=forwarding_payload, timeout=1)
                    return response.content, response.status_code
                    break
                except:
                    continue

            response = {'msg':'error','error':'could not send key to partition ' + str(key_index)}
            return jsonify(response), 223

# --------------------------------PING_ROUTE---------------------------------- #

@app.route('/ping', methods=['GET'])
def ping():

    if request.method == 'GET':

        payload = {'msg':'success', 'owner':IP_PORT}
        status = 200

        return jsonify(payload), status

# --------------------------------MEGA_BACKUP_ROUTE--------------------------- #

@app.route('/mega_backup', methods=['PUT'])
def mega_backup():

    if request.method == 'PUT':
        payload = request.form['data']
        global data
        data = json.loads(payload)
        print "DATA", data
        sys.stdout.flush()

        response = {"msg":"I have been updaed via MEGABACKUP"}
        return jsonify(response), 432


# --------------------------------BACKUP_ROUTE-------------------------------- #

@app.route('/backup', methods=['PUT','GET'])
def backup():

    if request.method == 'PUT':
        key = request.form['key']
        value = request.form['value']
        causal_payload = request.form['causal_payload'].split('.')
        timestamp = request.form['timestamp']
        id_number = request.form['id']
        source_address = request.form['source_address']
        if key in data:

            causal_bundle = [causal_payload, timestamp, id_number]
            print causal_bundle
            sys.stdout.flush()
            my_key_payload = data[key]
            my_causal_payload = my_key_payload[1]
            my_timestamp = my_key_payload[2]
            my_id = my_key_payload[3]
            my_causal_bundle = [my_causal_payload, my_timestamp, my_id]
            winning_casual_bundle = compare_causal_order(my_causal_bundle, causal_bundle)

            if (my_causal_bundle == causal_bundle):
                response = {'msg':'I am already updated!'}
                return jsonify(response), 333
            elif (winning_casual_bundle == my_causal_bundle):
                print 'winning', winning_casual_bundle, my_causal_bundle
                sys.stdout.flush()
                response = {'msg':'You need to be updated!'}
                # add to the queue
                key_payload = { 'key' : key,
                                'value' : my_key_payload[0],
                                'causal_payload' : vector_to_string(my_causal_payload),
                                'timestamp' : my_timestamp,
                                'id' : my_id,
                                'source_address': IP_PORT
                               }

                url = 'http://' + node_address + '/backup'
                response = requests.put(url, data=key_payload)
                print "THIS PART OF THE CODE"
                print response.content
                sys.stdout.flush()
                return jsonify(response), 444
            else:
                print 'winning', winning_casual_bundle, my_causal_bundle
                sys.stdout.flush()
                data_payload = [value, causal_payload, timestamp, id_number]
                data[key] = data_payload
                response = {'msg':'I am now updated!'}
                return jsonify(response), 555
        else:
            #write it
            data_payload = [value, causal_payload, timestamp, id_number]
            data[key] = data_payload
            response = {'msg':'I am initialized and updated!'}
            return jsonify(response), 777

@app.route('/kvs/get_partition_id', methods=['GET'])
def kvs_get_partition_id():
    if request.method == 'GET':

        payload = {'msg':'success', 'partition_id':get_partition_id(IP_PORT)}
        status = 200

        return jsonify(payload), status

@app.route('/kvs/get_all_partition_ids', methods=['GET'])
def kvs_get_partition_ids():
    if request.method == 'GET':

        payload = {'msg':'success', 'partition_id_list':get_all_partition_ids()}
        status = 200

        return jsonify(payload), status

@app.route('/kvs/get_partition_members', methods=['GET'])
def kvs_get_partition_members():
    if request.method == 'GET':

        partion_id = int(request.args.get('partition_id')) # get the id

        payload = {'msg':'success', 'partition_members':get_partition_members_from_index(partion_id)}
        status = 200

        return jsonify(payload), status

@app.route('/kvs/get_number_of_keys', methods=['GET'])
def kvs_get_number_of_keys(): # on a replica
    if request.method == 'GET':

        payload = {'count' : get_number_of_keys()}
        status = 200

        return jsonify(payload), status

# ------------------------------REDISTRIBUTE_KEYS----------------------------- #

# redistributes this nodes keys using key->[hash]->node_index
def redistribute_my_keys():

    VIEW = str(os.environ.get('VIEW')) # the VIEW
    node_addresses = VIEW.split(',') # all nodes currently running in list form
    node_number = len(node_addresses) # number of nodes in the network

    # if this node's address is in the list, then get
    if (str(IP_PORT) in node_addresses):

        my_hash_index = node_addresses.index(IP_PORT) # this nodes hash index

    else: # this case is called if this node is being asked to leave the network

        my_hash_index = None

    # determines if all keys on this node were redistributed
    redistributed = True

    for key in data.keys(): # for each key in this node's data

        index = hash(str(key), node_number) # find the key's index

        # if the key belongs on this node and this node is not being asked to
        # leave the network
        if (index == my_hash_index and my_hash_index != None):

            continue # do nothing

        else: # if key does not belong to this node, send the key to the owner

            node_address = str(node_addresses[index]) # key's new node address
            url = 'http://' + node_address + '/kvs'
            payload = {'key':key, 'value':data[str(key)]}
            response = requests.put(url, data=payload) # send message
            status = response.status_code

            if not (status == 200 or status == 201): # if key was not delivered

                redistributed = False # mission failed
                break

            else: # if key was delievered

                del data[key] # remove key from this nodes data

    # returns True if all keys on this node were redistributed
    return redistributed

# ---------------------------NOTIFY_REDISTRIBUTION---------------------------- #

# notfies all nodes in the network to redistribute their keys
def notify_redistribution_of_keys(VIEW):

    temp = {'msg':'error', 'owner':IP_PORT} # fail payload
    node_addresses = VIEW.split(',') # addresses to notify
    notified = True # flag to check if all nodes responded

    # if this fails it is because this node is being asked to leave the network
    if (IP_PORT in node_addresses):

        # remove self from list of nodes to notify
        node_addresses.remove(IP_PORT)

    # tell each node to redistribute its keys
    for address in node_addresses:

        url = 'http://' + str(address) + '/kvs/redistribute_keys'
        payload = {'source_address':str(IP_PORT)}
        response = requests.put(url, data=payload) # send message

        # if node did not respond as expected
        # mission failed
        if not (response.status_code == 200):

            notified = False # set notified to false and return error to client
            break

    if (notified is True): # if all nodes responded as expected

        temp = {'msg':'success', 'owner':IP_PORT} # respond with success

    # returns success when all nodes in the network are notified
    return temp

# --------------------------------NOTIFY_NODES-------------------------------- #

# notifies all nodes in the network to update their VIEW
def notify_nodes_of_view_change(VIEW):

    notified = True # determines if all nodes responded to VIEW update
    node_addresses = VIEW.split(',') # all nodes in the network

    # if this fails it is because this node is being asked to leave the network
    if (IP_PORT in node_addresses):
        node_addresses.remove(IP_PORT) # remove self from list of nodes

    # get the VIEW here because the VIEW may not contain this nodes addresses
    # if this node is in the process of being asked to leave the network
    VIEW = str(os.environ.get('VIEW'))

    # update the VIEW ENV variable of all nodes
    # including this node if it were being asked to leave the network
    for address in node_addresses:

        url = 'http://' + str(address) + '/kvs/view_notification'
        payload = {'VIEW':VIEW}
        response = requests.put(url, data=payload) # send message

        # if node did not respond as expected
        # mission failed
        if not (response.status_code == 200):

            notified = False # set notified to false and return error to client
            break

    # returns True if all nodes were notified of the VIEW update
    return notified

# --------------------------------UPDATE_VIEW-------------------------------- #

# updates this node's VIEW depending on action ('add', 'remove')
# and then notifies all nodes in the network
def update_view(ip_port, action):

    OLD_VIEW = get_view() # get the old VIEW
    node_addresses = get_node_addresses() # addresses of all nodes

    if (action == 'add'): # if add node is being requested

        # add new node address to VIEW if it is not already there
        if (ip_port not in node_addresses):

            NEW_VIEW = OLD_VIEW + ',' + ip_port # add address to VIEW
            os.environ['VIEW'] = NEW_VIEW # set VIEW ENV

        NEW_VIEW = str(os.environ.get('VIEW')) # get new VIEW

        # the NEW_VIEW is being passed in because the we want to
        # notify the whole network about the new node that is being added
        # to the network
        notified = notify_nodes_of_view_change(NEW_VIEW)

        # returns true if all nodes in the network are notified
        # of a VIEW change with adding a node
        return notified

    elif (action == 'remove'): # if remove node is being requested

        # remove old node address from VIEW if it is still there
        if (ip_port in node_addresses):

            # removes both possiblities of address and comma in VIEW
            NEW_VIEW = OLD_VIEW.replace(',' + str(ip_port), '')
            NEW_VIEW = NEW_VIEW.replace(str(ip_port) + ',', '')
            os.environ['VIEW'] = NEW_VIEW # set VIEW ENV

        # the OLD_VIEW is being passed in because we want to
        # notify the whole network about the node that is being removed
        # by the network, including the node being removed
        notified = notify_nodes_of_view_change(OLD_VIEW)

        # returns true if all nodes in the network are notified
        # of a VIEW change with removing a node
        return notified

    else: # error

        return False

# -----------------------------------ADD_NODE--------------------------------- #

# adds node to network by:
# notifying all nodes of VIEW update
# redistributing this node's keys
# notifying all nodes to redistribute their keys
def add_node_to_network(ip_port):

    temp = {'msg':'error', 'owner':IP_PORT} # fail payload
    VIEW = get_view() # get VIEW

    # returns True if all nodes in the network update their VIEW
    notified = update_view(ip_port, 'add')

    # if all nodes responded as expected
    # redistribute the keys to all nodes using redistribute_keys()
    if (notified is True):

        # returns True if all this node's keys were redistributed
        redistributed = redistribute_my_keys()

        # if this node's keys were properly redistributed
        if (redistributed is True):

            # returns success if all nodes were properly notified
            # to redistribute their keys
            temp = notify_redistribution_of_keys(VIEW)

    return temp # returns True if node was added to the network

# --------------------------------REMOVE_NODE--------------------------------- #

# removes node in network by:
# notifying all nodes of VIEW update
# redistributing this node's keys
# notifying all nodes to redistribute their keys
def remove_node_from_network(ip_port):

    temp = {'msg':'error', 'owner':IP_PORT} # fail payload
    OLD_VIEW = get_view() # get old VIEW

    # returns True if all nodes in the network update their VIEW
    notified = update_view(ip_port, 'remove')

    # if all nodes responded as expected
    # redistribute the keys to all nodes using redistribute_keys()
    if (notified is True):

        # returns True if all this node's keys were redistributed
        redistributed = redistribute_my_keys()

        # if this node's keys were properly redistributed
        if (redistributed is True):

            # returns success if all nodes were properly notified
            # to redistribute their keys
            temp = notify_redistribution_of_keys(OLD_VIEW)

    return temp # returns True is node was removed from the network


@app.route('/kvs/view_update', methods=['PUT'])
def request_view_update():

# -------------------------------------PUT------------------------------------ #

    if request.method == 'PUT':

        temp = {'msg':'error', 'owner':IP_PORT}

        if ('type' in request.form): # request to change network

            action = str(request.form['type']) # action type being requested
            ip_port = str(request.form['ip_port']) # node address from client

            if (action == 'add'): # if client requests to add a node

                # returns success if node was added to the network
                temp = add_node_to_network(ip_port)
                temp['partition_id'] = get_partition_id(ip_port)
                temp['number_of_partitions'] = get_number_of_partitions()

            if (action == 'remove'): # if client requests to remove a node

                # returns success if node was removed from the network
                temp = remove_node_from_network(ip_port)
                temp['number_of_partitions'] = get_number_of_partitions()

        # returns success if action type was properly executed
        return jsonify(temp)

@app.route('/kvs/view_notification', methods=['PUT'])
def request_change_view():

    if request.method == 'PUT':

        temp = {'msg':'error', 'owner':IP_PORT}
        status = 404

        if ('VIEW' in request.form):

            VIEW = request.form['VIEW']
            os.environ['VIEW'] = VIEW
            temp = {'msg':'success', 'owner':IP_PORT}
            status = 200

        return jsonify(temp), status

# ------------------------------------ROUTE----------------------------------- #

@app.route('/kvs/redistribute_keys', methods=['PUT'])
def request_redistibute_keys():

    if request.method == 'PUT':

        temp = {'msg':'error', 'owner':IP_PORT}
        status = 404

        source_address = request.form['source_address'] # this is not used

        redistributed = redistribute_my_keys()

        if (redistributed is True):

            temp = {'msg':'success', 'owner':IP_PORT}
            status = 200

        return jsonify(temp), status

# -------------------------------------MAIN------------------------------------ #

if __name__ == '__main__':

    begin_pinging()
    app.run(host='0.0.0.0', port=8080, threaded=True)

# ---------------------------------------------------------------------------- #

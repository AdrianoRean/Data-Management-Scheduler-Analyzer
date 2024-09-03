from copy import deepcopy
import json

from schedules import schedules

schedule_to_analyze = 1

blind_write = False
info = (0, [])
elements_touched_transactions = {}
'''
Nella forma di  (per due transazioni e due elementi):
{
    "element_1" : [
        (transaction_0, "W")
    ],
    "element_2" : [
        (transaction_1, "R"), (transaction_1, "W"), (transaction_0, "R")
    ]
}
'''
transaction_graph = []
'''
Nella forma di (per due transazioni e due elementi):
[
    [False, False],
    [True, False]
]
'''
read_from = {}
'''
Nella forma di (per due transazioni e due elementi):
{
    "transaction_0" : {
        "element_1" : "",
        "element_2" : trasaction_2
    },
    "transaction_1" : {
        "element_1" : "",
        "element_2" : ""
    }
}
'''
final_write = {}
'''
Nella forma di  (per due transazioni e due elementi):
{
    "element_1" : transaction_0,
    "element_2" : transaction_1
}
'''

to_serial = {}
'''
Nella forma di (per due transazioni e due elementi):
{
    "transaction_0" : [
        ["R", "transaction_0", "element_2"}, ["W", "transaction_0", "element_1"}
    ],
    "transaction_1" : [
        ["R", "transaction_1", "element_2"}, ["W", "transaction_1", "element_2"}
    ]
}
'''
serial_schedules = {}
'''
Nella forma di (per due transazioni e due elementi):
{
    "01" : [
        ["R", "transaction_0", "element_2"], 
        ["W", "transaction_0", "element_1"], 
        ["R", "transaction_1", "element_2"], 
        ["W", "transaction_1", "element_2"]
    ],
    "10" : [
        ["R", "transaction_1", "element_2"], 
        ["W", "transaction_1", "element_2"], 
        ["R", "transaction_0", "element_2"],
        ["W", "transaction_0", "element_1"]
    ]
}
'''

def parse(or_schedule, n_transactions, elements):
    
    #Initialization
    schedule = deepcopy(or_schedule)
    for i in range(0, n_transactions):
        transaction_graph.append([False for j in range (0, n_transactions)])
        read_from[str(i)] = {}
        to_serial[str(i)] = []
    for element in elements:
        elements_touched_transactions[element] = set([])
        final_write[element] = None
        for i in range(0, n_transactions):
            read_from[str(i)][element] = None
        
    
    #Actual Parsing
    for action in schedule:
        
        type_action = action[0]
        transaction = action[1]
        element = action[2]
        
        if element != "":
            
            for prior_action in elements_touched_transactions[element]:
                other_transaction = prior_action[0]
                other_type = prior_action[1]
                if other_transaction != transaction and transaction_graph[int(other_transaction)][int(transaction)] == False and (other_type == "W" or type_action == "W"):
                    transaction_graph[int(other_transaction)][int(transaction)] = True
                    
            elements_touched_transactions[element].add((transaction, element))
            if type_action == "W":
                if read_from[transaction][element] == "":
                    blind_write = True
                final_write[element] = transaction
            else:
                read_from[transaction][element] = final_write[element]
                
            if check_conflict_serializability(transaction_graph) and not blind_write:
                return False
                
        to_serial[transaction].append(action)

def parse_serial(schedule_name, n_transactions, elements):
    #Initialization
    schedule = deepcopy(serial_schedules[schedule_name]["schedule"])
    serial_read_from = serial_schedules[schedule_name]["read_from"]
    serial_final_write = serial_schedules[schedule_name]["final_write"]
    
    for i in range(0, n_transactions):
        serial_read_from[str(i)] = {}
    for element in elements:
        serial_final_write[element] = None
        for i in range(0, n_transactions):
            serial_read_from[str(i)][element] = None
        
    #Actual Parsing
    for action in schedule:
        
        type_action = action[0]
        transaction = action[1]
        element = action[2]
        
        if element != "":
            if type_action == "W":
                serial_final_write[element] = transaction
            else:
                serial_read_from[transaction][element] = serial_final_write[element]
        
def generate_serial(remaining, order, n_transactions, elements):
    if remaining == []:
        #crea schedule
        new_serial = []
        for transaction in order:
            new_serial += to_serial[transaction]
        name = ''.join(order)
        serial_schedules[name] = {}
        serial_schedules[name]["schedule"] = new_serial
        serial_schedules[name]["read_from"] = {}
        serial_schedules[name]["final_write"] = {}
        parse_serial(name, n_transactions, elements)
    else:
        for transaction in remaining:
            new_remaining = deepcopy(remaining)
            new_remaining.remove(transaction)
            new_order = deepcopy(order)
            new_order.append(transaction)
            generate_serial(new_remaining, new_order, n_transactions, elements)
                
def parse_and_create(schedule):
    info = schedule.pop(0)
    parse(schedule, info[0], info[1])
    generate_serial([str(transaction) for transaction in range (0, info[0])],[], info[0], info[1])
    
    with open(str(schedule_to_analyze) + '.json', 'w') as f:
        data = {
                "blind_write" : blind_write,
                "graph" : transaction_graph,
                "read_from" : read_from,
                "final_write" : final_write,
        }
        json.dump(data, f, indent=4)
        
    with open(str(schedule_to_analyze) + '_serials.json', 'w') as f:
        json.dump(serial_schedules, f, indent=4)
    
parse_and_create(schedules[schedule_to_analyze])
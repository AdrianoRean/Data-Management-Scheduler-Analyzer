from copy import deepcopy
import json

from schedules import schedules

conflict_serializable = True
info = (0, [])
resources_touched_transactions = {}
'''
Nella forma di  (per due transazioni e due elementi):
{
    "element_1" : {
        "Reads" : [],
        "Writes" [transaction_0]
    }
    "element_2" : {
        "Reads" : [transaction_1, transaction_0],
        "Writes" [transaction_1]
    }
}
'''
read_from = {}
'''
Nella forma di (per due transazioni e due elementi):
{
    transaction_0 : {
        "element_1" : "",
        "element_2" : trasaction_2
    },
    transaction_1 : {
        "element_1" : "",
        "element_2" : ""
    }
}
'''
remaining_conflicts = {}
'''
Nella forma di (per due transazioni e due elementi):
{
    transaction_0 : []
    transaction_1 : [transaction_0]
}
'''
conflicts = {}
'''
Nella forma di (per due transazioni e due elementi):
{
    transaction_0 : [transaction_1]
    transaction_1 : []
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
is_blind = {}
'''
Nella forma di  (per due transazioni e due elementi):
{
    transaction_0 : 
        "element_1" : True, 
        "element_0" : None
    transaction_1 : 
        "element_2" : False
}
'''

#Si potrebbe cercare di legare i cicli a se sono blind o meno, così si fa il check di view in maniera piu mirata.

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
            
def parse(or_schedule, n_transactions, resources):
    
    #Initialization
    schedule = deepcopy(or_schedule)
    for i in range(0, n_transactions):
        read_from[i] = {}
        to_serial[i] = []
        remaining_conflicts[i] = [j for j in range (0, n_transactions)]
        remaining_conflicts[i].remove(i)
        conflicts[i] = []
        is_blind[i] = {}
        
    for resource in resources:
        resources_touched_transactions[resource] = {}
        final_write[resource] = None
        for i in range(0, n_transactions):
            read_from[i][resource] = None
            resources_touched_transactions[resource]['Reads'] = set([])
            resources_touched_transactions[resource]['Writes'] = set([])
    
    #Actual Parsing
    for operation in schedule:
        
        action,transaction, resource = operation
        
        if resource != "":
            
            for other_transaction in remaining_conflicts[transaction]:
                if other_transaction in resources_touched_transactions[resource]["Writes"]:
                    conflicts[transaction].append(other_transaction)
                    remaining_conflicts[transaction].remove(other_transaction)
            
            if action == "W":
                resources_touched_transactions[resource]["Writes"].add(transaction)
                
                if transaction in resources_touched_transactions[resource]["Reads"]:
                    is_blind[transaction][resource] = False
                else:
                    is_blind[transaction][resource] = True
                
                final_write[resource] = transaction
                for other_transaction in remaining_conflicts[transaction]:
                    if other_transaction in resources_touched_transactions[resource]["Reads"]:
                        conflicts[transaction].append(other_transaction)
                        remaining_conflicts[transaction].remove(other_transaction)
            else:
                resources_touched_transactions[resource]["Reads"].add(transaction)
                read_from[transaction][resource] = final_write[resource]
                is_blind[transaction][resource] = None
                
        to_serial[transaction].append(operation)

def create_conflict_list(list_conflict_inverted, n_transactions):
    
    conflict_list = {}
    
    for i in range(0, n_transactions):
        conflict_list[i] = []
        
    for i in range(0, n_transactions):
        c_list = list_conflict_inverted[i]
        
        for other_transaction in c_list:
            conflict_list[other_transaction].append(i)
            
    return conflict_list

def parse_serial(schedule_name, n_transactions, resources):
    #Initialization
    schedule = deepcopy(serial_schedules[schedule_name]["schedule"])
    serial_read_from = serial_schedules[schedule_name]["read_from"]
    serial_final_write = serial_schedules[schedule_name]["final_write"]
    
    for i in range(0, n_transactions):
        serial_read_from[i] = {}
    for resource in resources:
        serial_final_write[resource] = None
        for i in range(0, n_transactions):
            serial_read_from[i][resource] = None
        
    #Actual Parsing
    for operation in schedule:
        
        action,transaction,resource = operation

        if resource != "":
            if action == "W":
                serial_final_write[resource] = transaction
            else:
                serial_read_from[transaction][resource] = serial_final_write[resource]
        
def generate_serial(remaining, order, n_transactions, resources):
    if remaining == []:
        #crea schedule
        new_serial = []
        name = ""
        for transaction in order:
            new_serial += to_serial[transaction]
            name = name + str(transaction)
        serial_schedules[name] = {}
        serial_schedules[name]["schedule"] = new_serial
        serial_schedules[name]["read_from"] = {}
        serial_schedules[name]["final_write"] = {}
        parse_serial(name, n_transactions, resources)
    else:
        for transaction in remaining:
            new_remaining = deepcopy(remaining)
            new_remaining.remove(transaction)
            new_order = deepcopy(order)
            new_order.append(transaction)
            generate_serial(new_remaining, new_order, n_transactions, resources)
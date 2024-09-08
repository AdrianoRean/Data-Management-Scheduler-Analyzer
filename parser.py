from copy import deepcopy
import json

from schedules import schedules

#Assunzione che nessuna transazione legge o scrive due volte stesso elemento, nè legge dopo aver scritto.
#Anche, se un grafo orientato ha un ciclo, anche il grafo inversamente orientato ha lo stesso ciclo.

blind_write = True
conflict_serializable = True
info = (0, [])
elements_touched_transactions = {}
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
remaining_conflicts = {}
'''
Nella forma di (per due transazioni e due elementi):
{
    "transaction_0" : []
    "transaction_1" : [transaction_0]
}
'''
conflicts = {}
'''
Nella forma di (per due transazioni e due elementi):
{
    "transaction_0" : [transaction_1]
    "transaction_1" : []
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
            
def parse(or_schedule, n_transactions, elements):
    
    #Initialization
    schedule = deepcopy(or_schedule)
    for i in range(0, n_transactions):
        read_from[i] = {}
        to_serial[i] = []
        remaining_conflicts[i] = [j for j in range (0, n_transactions)]
        remaining_conflicts[i].remove(i)
        conflicts[i] = []
        
    for element in elements:
        elements_touched_transactions[element] = {}
        final_write[element] = None
        for i in range(0, n_transactions):
            read_from[i][element] = None
            elements_touched_transactions[element]['Reads'] = set([])
            elements_touched_transactions[element]['Writes'] = set([])
    
    #Actual Parsing
    for action in schedule:
        
        type_action = action[0]
        transaction = action[1]
        element = action[2]
        
        if element != "":
            
            for other_transaction in remaining_conflicts[transaction]:
                if other_transaction in elements_touched_transactions[element]["Writes"]:
                    conflicts[transaction].append(other_transaction)
                    remaining_conflicts[transaction].remove(other_transaction)
                
            
            if type_action == "W":
                elements_touched_transactions[element]["Writes"].add(transaction)
                final_write[element] = transaction
                for other_transaction in remaining_conflicts[transaction]:
                    if other_transaction in elements_touched_transactions[element]["Reads"]:
                        conflicts[transaction].append(other_transaction)
                        remaining_conflicts[transaction].remove(other_transaction)
            else:
                elements_touched_transactions[element]["Reads"].add(transaction)
                read_from[transaction][element] = final_write[element]
                
        to_serial[transaction].append(action)

def create_conflict_list(list_conflict_inverted, n_transactions):
    
    conflict_list = {}
    
    for i in range(0, n_transactions):
        conflict_list[i] = []
        
    for i in range(0, n_transactions):
        c_list = list_conflict_inverted[i]
        
        for other_transaction in c_list:
            conflict_list[other_transaction].append(i)
            
    return conflict_list

def parse_serial(schedule_name, n_transactions, elements):
    #Initialization
    schedule = deepcopy(serial_schedules[schedule_name]["schedule"])
    serial_read_from = serial_schedules[schedule_name]["read_from"]
    serial_final_write = serial_schedules[schedule_name]["final_write"]
    
    for i in range(0, n_transactions):
        serial_read_from[i] = {}
    for element in elements:
        serial_final_write[element] = None
        for i in range(0, n_transactions):
            serial_read_from[i][element] = None
        
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
        name = ""
        for transaction in order:
            new_serial += to_serial[transaction]
            name = name + str(transaction)
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
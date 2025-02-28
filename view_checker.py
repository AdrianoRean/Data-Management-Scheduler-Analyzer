from copy import deepcopy
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
read_from = {}
final_write = {}
resources_touched_transactions = {}
serial_schedules = {}

def parse(or_schedule, n_transactions, resources):
    
    #Initialization
    schedule = deepcopy(or_schedule)
    for i in range(0, n_transactions):
        read_from[i] = {}
        to_serial[i] = []
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
            
            if action == "W":
                resources_touched_transactions[resource]["Writes"].add(transaction)
                
                if transaction in resources_touched_transactions[resource]["Reads"]:
                    is_blind[transaction][resource] = False
                else:
                    is_blind[transaction][resource] = True
                
                final_write[resource] = transaction
            else:
                resources_touched_transactions[resource]["Reads"].add(transaction)
                read_from[transaction][resource] = final_write[resource]
                is_blind[transaction][resource] = None
                
        to_serial[transaction].append(operation)

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

def check_view_serializabilty(read_from, final_write, serial_schedules):
    for schedule in serial_schedules:
        if read_from == serial_schedules[schedule]["read_from"] and final_write == serial_schedules[schedule]["final_write"]:
            return True
    else:
        return False

def check_if_cycles_are_blind(circuits, is_blind, n_transactions):
    for t in range(0, n_transactions):
        pass
        
    for circuit in circuits:
        elements_in_common = set([])
        for t in circuit:
            elements_in_common = set.intersection(elements_in_common, set(is_blind[t].keys()))
        for e in elements_in_common:
            for t in circuit:
                if is_blind[t][e] == False: #Controlla se può andare bene conflitto only read e blind write
                    return False
    
    return True
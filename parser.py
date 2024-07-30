from copy import deepcopy
import json

from schedules import schedules

schedule_to_analyze = 1

blind_write = False
elements_touched_transactions = {}
graph = []
read_from = {}
final_write = {}

to_serial = {}
serial_schedules = {}

def parse(or_schedule):
    
    #Initialization
    schedule = deepcopy(or_schedule)
    info = schedule.pop(0)
    for i in range(0, info[0]):
        graph.append([False for j in range (0, info[0])])
        read_from[str(i)] = {}
        to_serial[str(i)] = []
    for element in info[1]:
        elements_touched_transactions[element] = set([])
        final_write[element] = None
        for i in range(0, info[0]):
            read_from[str(i)][element] = (None, None)
        
    
    #Actual Parsing
    for action in schedule:
        
        type_action = action[0]
        transaction = action[1]
        element = action[2]
        
        if element != "":
            
            for prior_action in elements_touched_transactions[element]:
                other_transaction = prior_action[0]
                other_type = prior_action[1]
                if other_transaction != transaction and (other_type == "W" or type_action == "W"):
                    graph[int(other_transaction)][int(transaction)] = True
                    break
                    
            elements_touched_transactions[element].add((transaction, element))
            if type_action == "W":
                if read_from[transaction][element] == (None,None):
                    blind_write = True
                final_write[element] = transaction
            else:
                read_from[transaction][element] == (transaction, final_write[element])
                
        to_serial[transaction].append((type_action, element))
        
def generate_serial(start, transactions):
    for i in range (start):
        pass

def parse_serial(schedule):
    pass
        
parse(schedules[0])
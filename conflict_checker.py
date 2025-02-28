from copy import deepcopy

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
            
def parse(or_schedule, n_transactions, resources):
    
    #Initialization
    schedule = deepcopy(or_schedule)
    for i in range(0, n_transactions):
        read_from[i] = {}
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
    
    return conflicts
                

def create_conflict_list(list_conflict_inverted, n_transactions):
    
    conflict_list = {}
    
    for i in range(0, n_transactions):
        conflict_list[i] = []
        
    for i in range(0, n_transactions):
        c_list = list_conflict_inverted[i]
        
        for other_transaction in c_list:
            conflict_list[other_transaction].append(i)
            
    return conflict_list

def check_conflict_serializability(graph, n_transaction):
    remaining = [i for i in range(0, n_transaction)]
    
    actual_node = remaining.pop()
    visited = [False for i in range(0, n_transaction)]
    visited[actual_node] = True
    
    while True:
        if check_cycle(graph, n_transaction, actual_node, remaining, visited):
            return False
        else:
            if remaining != []:
                actual_node = remaining.pop()
                visited = [False for i in range(0, n_transaction)]
                visited[actual_node] = True
            else:
                return True
            
def check_cycle(graph, n_transaction, actual_node, remaining, visited):
    for next_node in graph[actual_node]:
        if visited[next_node]:
            return True
        elif visited[next_node] in remaining:
            visited[next_node] = True
            remaining.remove(next_node)
            return check_cycle(graph, n_transaction, next_node, remaining, visited)
    return False
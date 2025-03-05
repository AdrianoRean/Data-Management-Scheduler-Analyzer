from copy import deepcopy
#self.resources_touched_transactions = {}
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
        #self.read_from = {}
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
        #self.remaining_conflicts = {}
'''
        Nella forma di (per due transazioni e due elementi):
        {
            transaction_0 : []
            transaction_1 : [transaction_0]
        }
'''
        #self.conflicts = {}
'''
        Nella forma di (per due transazioni e due elementi):
        {
            transaction_0 : [transaction_1]
            transaction_1 : []
        }
'''
        #self.final_write = {}
'''
        Nella forma di  (per due transazioni e due elementi):
        {
            "element_1" : transaction_0,
            "element_2" : transaction_1
        }
'''
        #self.is_blind = {}
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
        #self.conflict_list = {}
        
class ConflictChecker:
    
    def __init__(self,  schedule, resources, n_transactions, remaining_conflicts = {}, conflicts={}, resources_touched_transactions = {}):
        self.conflict_serializable = True
        self.info = (0, [])
        self.resources = resources
        self.n_transactions = n_transactions
        self.schedule = schedule
        
        self.remaining_conflicts = remaining_conflicts
        self.conflicts = conflicts
        self.resources_touched_transactions = resources_touched_transactions
             
    def parse(self):
        
        #Initialization
        schedule = deepcopy(self.schedule)
        for i in range(0, self.n_transactions):
            self.remaining_conflicts[i] = [j for j in range (0, self.n_transactions)]
            self.remaining_conflicts[i].remove(i)
            self.conflicts[i] = []
            
        for resource in self.resources:
            self.resources_touched_transactions[resource] = {}
            for i in range(0, self.n_transactions):
                self.resources_touched_transactions[resource]['Reads'] = set([])
                self.resources_touched_transactions[resource]['Writes'] = set([])
        
        #Actual Parsing
        for operation in schedule:
            
            action,transaction, resource = operation
            
            if resource != "":
                #to avoid for inconsistencies
                rm = self.remaining_conflicts[transaction].copy()
                for other_transaction in rm:
                    if other_transaction in self.resources_touched_transactions[resource]["Writes"]:
                        self.conflicts[transaction].append(other_transaction)
                        self.remaining_conflicts[transaction].remove(other_transaction)
                
                if action == "W":
                    self.resources_touched_transactions[resource]["Writes"].add(transaction)
                    #to avoid for inconsistencies
                    rm = self.remaining_conflicts[transaction].copy()
                    for other_transaction in rm:
                        if other_transaction in self.resources_touched_transactions[resource]["Reads"]:
                            self.conflicts[transaction].append(other_transaction)
                            self.remaining_conflicts[transaction].remove(other_transaction)
                else:
                    self.resources_touched_transactions[resource]["Reads"].add(transaction)
                
    def check_conflict_serializability(self):
        remaining = [i for i in range(0, self.n_transactions)]
        
        actual_node = remaining.pop()
        visited = [False for i in range(0, self.n_transactions)]
        visited[actual_node] = True
        
        while True:
            if self.check_cycle(self.conflicts, self.n_transactions, actual_node, remaining, visited):
                return False
            else:
                if remaining != []:
                    actual_node = remaining.pop()
                    visited = [False for i in range(0, self.n_transactions)]
                    visited[actual_node] = True
                else:
                    return True
                
    def check_cycle(self, graph, n_transaction, actual_node, remaining, visited):
        for next_node in graph[actual_node]:
            if visited[next_node]:
                return True
            elif next_node in remaining:
                new_visited = visited.copy()
                new_visited[next_node] = True
                remaining.remove(next_node)
                if self.check_cycle(graph, n_transaction, next_node, remaining, new_visited):
                    return True
        return False
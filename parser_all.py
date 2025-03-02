from copy import deepcopy

class Parser:
    
    def __init__(self, or_schedule, n_transactions, resources):
        self.schedule = or_schedule
        self.n_transactions = n_transactions
        self.resources = resources
        
        #TwoPL
        self.resources_needed = {} #lista di risorse richieste
        self.transactions_involved = {} # lista di transazioni che richiedono la risorsa
        
        #Conflict-Serializable variables
        self.remaining_conflicts = {}
        '''
        Nella forma di (per due transazioni e due elementi):
        {
            transaction_0 : []
            transaction_1 : [transaction_0]
        }
        '''
        self.conflicts = {}
        '''
        Nella forma di (per due transazioni e due elementi):
        {
            transaction_0 : [transaction_1]
            transaction_1 : []
        }
        '''
        self.resources_touched_transactions = {}
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
        
        #View-serializable variables
        self.read_from = {}
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
        self.final_write = {}
        '''
        Nella forma di  (per due transazioni e due elementi):
        {
            "element_1" : transaction_0,
            "element_2" : transaction_1
        }
        '''
        self.is_blind = False
        #Si potrebbe cercare di legare i cicli a se sono blind o meno, così si fa il check di view in maniera piu mirata.
        self.to_serial = {}
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

    def parse(self):
        
        #Initialization
        schedule = deepcopy(self.schedule)
        for i in range(0, self.n_transactions):
            self.read_from[i] = {}
            self.to_serial[i] = []
            self.remaining_conflicts[i] = [j for j in range (0, self.n_transactions)]
            self.remaining_conflicts[i].remove(i)
            self.conflicts[i] = []
            
        for resource in self.resources:
            self.resources_touched_transactions[resource] = {}
            self.final_write[resource] = None
            for i in range(0, self.n_transactions):
                self.read_from[i][resource] = None
                self.resources_touched_transactions[resource]['Reads'] = set([])
                self.resources_touched_transactions[resource]['Writes'] = set([])
        
        #Actual Parsing
        for operation in schedule:
            
            action,transaction, resource = operation

            # RESOURCE_NEEDED
            if str(transaction) not in self.resources_needed.keys():
                self.resources_needed[str(transaction)]=[(resource,action)]
            else:
                self.resources_needed[str(transaction)].append((resource,action))

            # TRANSACTIONS_INVOLVED     
            if resource not in self.transactions_involved.keys():
                self.transactions_involved[resource]=[(transaction,action)]
            else:
                self.transactions_involved[resource].append((transaction,action))
            
            if resource != "":
                
                for other_transaction in self.remaining_conflicts[transaction]:
                    if other_transaction in self.resources_touched_transactions[resource]["Writes"]:
                        self.conflicts[transaction].append(other_transaction)
                        self.remaining_conflicts[transaction].remove(other_transaction)
                
                if action == "W":
                    self.resources_touched_transactions[resource]["Writes"].add(transaction)
                    
                    if transaction not in self.resources_touched_transactions[resource]["Reads"]:
                        self.is_blind = True
                    
                    self.final_write[resource] = transaction
                    for other_transaction in self.remaining_conflicts[transaction]:
                        if other_transaction in self.resources_touched_transactions[resource]["Reads"]:
                            self.conflicts[transaction].append(other_transaction)
                            self.remaining_conflicts[transaction].remove(other_transaction)
                else:
                    self.resources_touched_transactions[resource]["Reads"].add(transaction)
                    self.read_from[transaction][resource] = self.final_write[resource]
                    
            self.to_serial[transaction].append(operation)
        
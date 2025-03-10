from copy import deepcopy

class ViewChecker:
    
    def __init__(self, 
            schedule,
            n_transactions,
            resources,
            is_blind = False,
            to_serial = {},
            read_from = {},
            final_write = {}):
        self.schedule = schedule
        self.n_transactions = n_transactions
        self.resources = resources
        self.is_blind = is_blind
        self.to_serial = to_serial
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
        self.read_from = read_from
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
        self.final_write = final_write
        '''
            Nella forma di  (per due transazioni e due elementi):
            {
                "element_1" : transaction_0,
                "element_2" : transaction_1
            }
            '''

    def parse(self):
        
        #Initialization
        schedule = deepcopy(self.schedule)
        for i in range(0, self.n_transactions):
            self.read_from[i] = {}
            self.to_serial[i] = []
            
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
            
            if resource != "":
                
                if action == "W":
                    
                    if transaction not in self.resources_touched_transactions[resource]["Reads"]:
                        self.is_blind = True
                    
                    self.final_write[resource] = transaction
                else:
                    self.resources_touched_transactions[resource]["Reads"].add(transaction)
                    self.read_from[transaction][resource] = self.final_write[resource]
                    
            self.to_serial[transaction].append(operation)

    def parse_and_check_serial(self, serial_schedule, n_transactions, resources):
        #Initialization
        schedule = deepcopy(serial_schedule["schedule"])
        serial_read_from = serial_schedule["read_from"]
        serial_final_write = serial_schedule["final_write"]
        
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
        
        return self.check_view_serializabilty(serial_schedule)
        

    def generate_and_check_serial(self, remaining, order = []):
        if remaining == []:
            #crea schedule
            new_serial = []
            for transaction in order:
                new_serial += self.to_serial[transaction]
            serial_schedule = {}
            serial_schedule["schedule"] = new_serial
            serial_schedule["read_from"] = {}
            serial_schedule["final_write"] = {}
            if self.parse_and_check_serial(serial_schedule, self.n_transactions, self.resources):
                return True
        else:
            for transaction in remaining:
                new_remaining = deepcopy(remaining)
                new_remaining.remove(transaction)
                new_order = deepcopy(order)
                new_order.append(transaction)
                if self.generate_and_check_serial(new_remaining, new_order):
                    return True
        
        return False
        
    def check_view_serializabilty(self, serial_schedule):
        if self.read_from == serial_schedule["read_from"] and self.final_write == serial_schedule["final_write"]:
            return True
        else:
            return False
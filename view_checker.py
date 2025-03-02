from copy import deepcopy

class ViewChecker:
    
    def __init__(self, 
            schedule,
            n_transactions,
            resources):
        self.schedule = schedule
        self.n_transactions = n_transactions
        self.resources = resources
        
        self.is_blind = False
        self.serial_schedules = {}
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
        #self.serial_schedules = {}
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
                    self.resources_touched_transactions[resource]["Writes"].add(transaction)
                    
                    if transaction not in self.resources_touched_transactions[resource]["Reads"]:
                        self.is_blind = True
                    
                    self.final_write[resource] = transaction
                else:
                    self.resources_touched_transactions[resource]["Reads"].add(transaction)
                    self.read_from[transaction][resource] = self.final_write[resource]
                    
            self.to_serial[transaction].append(operation)

    def parse_serial(self, schedule_name, n_transactions, resources):
        #Initialization
        schedule = deepcopy(self.serial_schedules[schedule_name]["schedule"])
        serial_read_from = self.serial_schedules[schedule_name]["read_from"]
        serial_final_write = self.serial_schedules[schedule_name]["final_write"]
        
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
        

    def generate_serial(self, remaining, order, n_transactions, resources):
        if remaining == []:
            #crea schedule
            new_serial = []
            name = ""
            for transaction in order:
                new_serial += self.to_serial[transaction]
                name = name + str(transaction)
            self.serial_schedules[name] = {}
            self.serial_schedules[name]["schedule"] = new_serial
            self.serial_schedules[name]["read_from"] = {}
            self.serial_schedules[name]["final_write"] = {}
            self.parse_serial(name, self.n_transactions, self.resources)
        else:
            for transaction in remaining:
                new_remaining = deepcopy(remaining)
                new_remaining.remove(transaction)
                new_order = deepcopy(order)
                new_order.append(transaction)
                self.generate_serial(new_remaining, new_order, n_transactions, resources)

    def check_view_serializabilty(self):
        for schedule in self.serial_schedules:
            if self.read_from == self.serial_schedules[schedule]["read_from"] and self.final_write == self.serial_schedules[schedule]["final_write"]:
                return True
        else:
            return False
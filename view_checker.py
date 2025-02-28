from copy import deepcopy

class ViewChecker:
    
    def __init__(self, n_transactions):
        self.n_transactions = n_transactions
        self.johnson = self.Johnson(n_transactions)
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
        self.is_blind = {}
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
        
    class Johnson:
    
        def __init__(self, n_transactions):
            self.circuits = []
            self.is_blind = False
            self.n_transactions = n_transactions

        def get_component_adiacency_list(self, connected_lists, s, n_transaction):
            new_lists = deepcopy(connected_lists)
            print(f"New list for s {s}: {new_lists}")
            for i in range(0,s):
                new_lists.pop(i)
                for j in range(s, n_transaction):
                    try:
                        new_lists[j].remove(i)
                    except:
                        pass
            print(f"After: {new_lists}")
            return new_lists

        def unblock(self, v, blocked, blocked_stack):
            blocked[v] = False
            to_del = []
            for w in blocked_stack[v]:
                to_del.append(w)
                self.unblock(w, blocked, blocked_stack)
            for w in to_del:
                blocked_stack[v].pop(w)
                
        def circuit(self, v, s, stack, blocked, blocked_stack, adiancency_list):
            #print("v: " + str(v) + ", s: " + str(s) + ", stack: " + str(stack) + ", blocked: " + str(blocked) + ", b_stack: " + str(blocked_stack) + ", adiacency_list: " + str(adiancency_list))
            f = False
            stack.append(v)
            blocked[v] = True
            for w in adiancency_list[v]:
                if w == s:
                    self.circuits.append(deepcopy(stack))
                    f = True
                elif not blocked[w]:
                    f = self.circuit(w, s, stack, blocked, blocked_stack, adiancency_list)
                if f:
                    self.unblock(v, blocked, blocked_stack)
                else:
                    for w in adiancency_list[v]:
                        if v not in blocked_stack[w]:
                            blocked_stack[w].append(v)
                
                stack.remove(v)
                
                return f

        def johnson(self, connected_lists):
            
            for s in range (0, self.n_transactions - 1):
                adiancency_list = self.get_component_adiacency_list(connected_lists, s, self.n_transactions)
                stack = []
                blocked = [False for i in range(0, self.n_transactions)]
                blocked_stack = {}
                for i in range (s, self.n_transactions):
                    blocked_stack[i] = []
                
                self.circuit(s, s, stack, blocked, blocked_stack, adiancency_list)        

    def parse(self, or_schedule, n_transactions, resources):
        
        #Initialization
        schedule = deepcopy(or_schedule)
        for i in range(0, n_transactions):
            self.read_from[i] = {}
            self.to_serial[i] = []
            self.is_blind[i] = {}
            
        for resource in resources:
            self.resources_touched_transactions[resource] = {}
            self.final_write[resource] = None
            for i in range(0, n_transactions):
                self.read_from[i][resource] = None
                self.resources_touched_transactions[resource]['Reads'] = set([])
                self.resources_touched_transactions[resource]['Writes'] = set([])
        
        #Actual Parsing
        for operation in schedule:
            
            action,transaction, resource = operation
            
            if resource != "":
                
                if action == "W":
                    self.resources_touched_transactions[resource]["Writes"].add(transaction)
                    
                    if transaction in self.resources_touched_transactions[resource]["Reads"]:
                        self.is_blind[transaction][resource] = False
                    else:
                        self.is_blind[transaction][resource] = True
                    
                    self.final_write[resource] = transaction
                else:
                    self.resources_touched_transactions[resource]["Reads"].add(transaction)
                    self.read_from[transaction][resource] = self.final_write[resource]
                    self.is_blind[transaction][resource] = None
                    
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
            self.parse_serial(name, n_transactions, resources)
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

    def check_if_cycles_are_blind(self):
        for t in range(0, self.n_transactions):
            pass
            
        for circuit in self.johnson.circuits:
            elements_in_common = set([])
            for t in circuit:
                elements_in_common = set.intersection(elements_in_common, set(self.is_blind[t].keys()))
            for e in elements_in_common:
                for t in circuit:
                    if self.is_blind[t][e] == False: #Controlla se può andare bene conflitto only read e blind write
                        return False
        
        return True
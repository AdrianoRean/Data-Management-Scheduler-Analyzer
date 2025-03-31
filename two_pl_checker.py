from copy import deepcopy
from schedules import schedules

class TwoPLChecker:
    
    def __init__(self, n_transactions, schedule, resources, resources_needed={}, resources_to_use={}):
   
        self.n_transactions = n_transactions
        self.schedule = schedule
        self.resources = resources
        self.resources_needed = resources_needed
        self.resources_to_use = resources_to_use

    def clean_transaction_involved(self, transaction):

        for key in self.transactions_involved.keys():
            self.transactions_involved[key] = [(other_tr, other_act) for (other_tr, other_act) in self.transactions_involved[key] if other_tr != transaction]

    def need_same(self,loop):
        resources = []
        transaction = []
        for t,r in loop:
            if r in resources or t in transaction:
                return True
            else:
                resources.append(r)
                transaction.append(t)
        return False
    
    def anticipate_locks(self, asking_transaction, resource, loop = []):
        
        #Doppio ciclo per evitare ricorsione se posso subito verificare inutilizzabilità
        
        #Devo ancora usare la risorsa chiesta?
        for other_transaction in self.lock[str(resource)][1]:
            #Se sono me stesso, non devo controllare!
            if other_transaction == asking_transaction:
                continue
            else:
                for t in self.resources_to_use[str(other_transaction)]:
                    if t[0] == resource: 
                        return False
    
        #controlla che possono anticipare i loro lock (aka termino growing phase e inizio shriking)
        for other_transaction in self.lock[str(resource)][1]:
            #Se sono me stesso, non devo anticipare!
            if other_transaction == asking_transaction:
                continue
            else:
                #Evita casini con modifiche durante i loop
                r_n = deepcopy(self.resources_needed[str(other_transaction)])
                for (other_resource, other_action) in r_n:
                    other_tuple = (other_action, other_transaction, other_resource)
                    if other_tuple in loop:
                        return False
                    loop.append(other_tuple)
                    result = self.check_if_lock_available(other_transaction, other_action, other_resource, True, loop.copy())
                    if not result:
                        return False
                
        #Sono in shriking phase e ho già utilizzato la risorsa
        return True
        
    def check_if_lock_available(self, transaction, action, resource, anticipating = False, loop = []):        
        #No locks
        if self.lock[resource] == None and self.phase[str(transaction)]:
            self.lock[resource] = (action, [transaction])
            self.resources_needed[str(transaction)].remove((resource, action))
            if not anticipating:
                self.resources_to_use[str(transaction)].remove((resource, action))
                
                if len(self.resources_to_use[str(transaction)]) == 0:
                    self.phase[str(transaction)] = False
                    self.lock[resource] = None
                    
            return True
        #Lock already acquired
        elif not anticipating and ((self.lock[resource] == ("W", [transaction]) or (action == "R" and self.lock[resource][0] == "R" and transaction in self.lock[resource][1]))):
            self.resources_to_use[str(transaction)].remove((resource, action))
            if len(self.resources_to_use[str(transaction)]) == 0:
                self.phase[str(transaction)] = False
                if action == "W":
                    self.lock[resource] = None
                else:
                    self.lock[resource][1].remove(transaction)
                    if len(self.lock[resource][1]) == 0:
                        self.lock[resource] = None
            return True
        #Other's lock
        elif action == "R":
            # Shared lock?
            if self.phase[str(transaction)]:
                if self.lock[resource][0] == "R":
                    self.lock[resource][1].append(transaction)
                    self.resources_needed[str(transaction)].remove((resource, action))
                    if not anticipating:
                        self.resources_to_use[str(transaction)].remove((resource, action))
                        if len(self.resources_to_use[str(transaction)]) == 0:
                            self.phase[str(transaction)] = False
                            self.lock[resource][1].remove(transaction)
                            if len(self.lock[resource][1]) == 0:
                                self.lock[resource] = None
                    return True
                #Can other transactions anticipate locks?
                elif self.anticipate_locks(transaction, resource, loop):
                    self.lock[resource] = (action, [transaction])
                    self.resources_needed[str(transaction)].remove((resource, action))
                    if not anticipating:
                        self.resources_to_use[str(transaction)].remove((resource, action))
                        if len(self.resources_to_use[str(transaction)]) == 0:
                            self.phase[str(transaction)] = False
                            self.lock[resource][1].remove(transaction)
                            if len(self.lock[resource][1]) == 0:
                                self.lock[resource] = None    
                    return True
                else:
                    return False
            else:
                return False
        #Upgrade lock
        else:
            if self.phase[str(transaction)] and (self.lock[resource] == ("R", [transaction]) or self.anticipate_locks(transaction, resource, loop)):
                self.lock[resource] = (action, [transaction])
                self.resources_needed[str(transaction)].remove((resource, action))
                if not anticipating:
                    self.resources_to_use[str(transaction)].remove((resource, action))
                    if len(self.resources_to_use[str(transaction)]) == 0:
                        self.phase[str(transaction)] = False
                        self.lock[resource] = None
                return True
            
        return False
    
    '''#Upgrade lock
        elif action == "W" and self.lock[resource] == ("R", [transaction]):
            self.lock[resource] = (action, [transaction])
            self.resources_needed[str(transaction)].remove((resource, action))
            if not anticipating:
                self.resources_to_use[str(transaction)].remove((resource, action))
                
                if len(self.resources_to_use[str(transaction)]) == 0:
                    self.lock[resource] = None
            
            return True'''
        
    def parse(self):
        # 𝑂(𝑛)
        for operation in (self.schedule):
            action,tr,resource = operation
                    
            # RESOURCES_NEEDED
            if str(tr) not in self.resources_needed.keys():
                self.resources_needed[str(tr)]=[(resource, action)]
                self.resources_to_use[str(tr)]=[(resource, action)]
            else:
                self.resources_needed[str(tr)].append((resource, action))
                self.resources_to_use[str(tr)].append((resource, action))
         
    def parse_lock(self):    
        self.lock = {}   
        self.phase = {}
        
        for resource in self.resources:
            self.lock[resource] = None
            
        for i in range(0, self.n_transactions):
            self.phase[str(i)] = True


    def two_pl_checker(self):
        ''' L'idea del checker è di lockare tutto il prima possibile '''

        # Step 2: Check and lock
        # invece di usare who_is_locking basarsi solo sul transaction_involved
        #print(f'\n OUT \nresources_needed: {resources_needed}, transaction_involved: {transactions_involved}')
        
        for operation in (self.schedule):
            action,tr,resource = operation

            # se posso eseguire l'azione -> nessuno ha il lock oppure la transazione che lo 
            #                               detiene può lasciarlo perché non ha alcuna altra
            #                               risorsa da lockare
            # per ogni transazione che richiede la risorsa prima di lui controlla se ha qualcosa
            result = self.check_if_lock_available(tr, action, resource, False, [])
            if not result:
                '''print("Step")
                print(f"Transaction: {str(tr)}, Action: {action}, Resource: {resource}")
                print("----------")
                print("Resource Needed")
                print(self.resources_needed)
                print("----------")
                print("Resource to use")
                print(self.resources_to_use)
                print("----------")
                print("Locks")
                print(self.lock)'''
                return False
                        
            #print(f"Tr: {str(tr)}, Action: {action}, Resource: {resource}, R_Needed: {resources_needed[str(tr)]}")

        return True

if __name__ == "main":
    schedule = schedules[1]
    info = schedule.pop(0)
    pl = TwoPLChecker()
    #print(f'Lo schedule: {schedule} è 2pl? \n {two_pl_checker(schedule)}')
        
    for s in schedules:
        info = s.pop(0)
        print(f'Lo schedule: {s} è 2pl? \n {pl.two_pl_checker(s)}')

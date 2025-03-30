from schedules import schedules

class TwoPLChecker:
    
    def __init__(self, schedule, resources_needed={}, transactions_involved={}):
   
        self.schedule = schedule
        self.resources_needed = resources_needed
        self.transactions_involved = transactions_involved

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
    
    def check_if_lock_available(self, transaction, action, resource, index, loop_resources={}):
        
        # Sono il primo?
        if index != 0:
            previous_transaction, previous_action = self.transactions_involved[resource][index - 1]
        else:
            return True
        
        # Shared lock?
        if action == "R" and previous_action == "R":
            actions = self.transactions_involved[resource][0:index]
            for _,a in actions:
                if a != "R":
                    return False
            return True 
        
        # Upgrading lock or there was shared lock?
        elif transaction==previous_transaction:
            
            for (transaction_involved, _) in self.transactions_involved[resource]:
                if transaction_involved == transaction:
                    continue

                for (other_resource, other_action) in self.resources_needed[str(transaction_involved)]:
                    other_index = self.transactions_involved[other_resource].index((transaction_involved,other_action))
                    
                    if other_resource in loop_resources:
                        if transaction_involved != loop_resources[other_resource]:
                            return False
                        else:
                            loop_resources[other_resource] = -1
                    else:
                        loop_resources[other_resource] = transaction_involved
                    
                    result = self.check_if_lock_available(transaction_involved, other_action, other_resource, other_index, loop_resources.copy())
                    if not result:
                        return False
            return True
               

        # L'azione precedente può anticipare i lock?
        else:
            
            for (other_resource, other_action) in self.resources_needed[str(previous_transaction)]:
                other_index = self.transactions_involved[other_resource].index((previous_transaction,other_action))
               
                if other_resource in loop_resources:
                    if previous_transaction != loop_resources[other_resource]:
                        return False
                    else:
                        loop_resources[other_resource] = -1
                else:
                    loop_resources[other_resource] = previous_transaction
                    
                result = self.check_if_lock_available(previous_transaction, other_action, other_resource, other_index, loop_resources.copy())
                if not result:
                    return False
            return True
        
    def parse(self):
        # 𝑂(𝑛)
        for operation in (self.schedule):
            action,tr,resource = operation
                    
            # RESOURCES_NEEDED
            if str(tr) not in self.resources_needed.keys():
                self.resources_needed[str(tr)]=[(resource,action)]
            else:
                self.resources_needed[str(tr)].append((resource,action))

            # TRANSACTIONS_INVOLVED     
            if resource not in self.transactions_involved.keys():
                self.transactions_involved[resource]=[(tr,action)]
            else:
                self.transactions_involved[resource].append((tr,action))


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
            index = self.transactions_involved[resource].index((tr,action))
            # per ogni transazione che richiede la risorsa prima di lui controlla se ha qualcosa
            if action != "C":
                result = self.check_if_lock_available(tr, action, resource, index,loop_resources={resource:tr})
                if not result:
                    return False
                #print(f"Tr: {str(tr)}, Action: {action}, Resource: {resource}, R_Needed: {resources_needed[str(tr)]}")
                self.resources_needed[str(tr)].remove((resource, action))
            
            # Ho fatto tutto?
            if len(self.resources_needed[str(tr)]) == 0:
                self.clean_transaction_involved(tr)

        return True

if __name__ == "main":
    schedule = schedules[1]
    info = schedule.pop(0)
    pl = TwoPLChecker()
    #print(f'Lo schedule: {schedule} è 2pl? \n {two_pl_checker(schedule)}')
        
    for s in schedules:
        info = s.pop(0)
        print(f'Lo schedule: {s} è 2pl? \n {pl.two_pl_checker(s)}')

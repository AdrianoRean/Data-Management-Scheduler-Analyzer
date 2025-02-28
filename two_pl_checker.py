from schedules import schedules

resources_needed = {} #lista di risorse richieste
transactions_involved = {} # lista di transazioni che richiedono la risorsa

def clean_transaction_involved(transaction):
    global transactions_involved

    for key in transactions_involved.keys():
        transactions_involved[key] = [(other_tr, other_act) for (other_tr, other_act) in transactions_involved[key] if other_tr != transaction]

def check_if_lock_available(transaction, action, resource, index, loop=[]):
    global has_something_to_lock
    global resources_needed
    global transactions_involved
    global who_is_locking

    # Sono il primo?
    if index != 0:
        previous_transaction, previous_action = transactions_involved[resource][index - 1]
    else:
        return True

    # Shared lock?
    if action == "R" and previous_action == "R":
        return True
    # Sono sempre io?
    elif transaction == previous_transaction: 
        return True
    # L'azione precedente può anticipare i lock?
    else:
        
        for (other_resource,other_action) in resources_needed[str(previous_transaction)]:
            other_index = transactions_involved[other_resource].index((previous_transaction,other_action))
            if previous_transaction in loop:
                return False
            else:
                loop.append(previous_transaction)
            result = check_if_lock_available(previous_transaction, other_action, other_resource, other_index,loop.copy())
            if not result:
                return False
        return True

def two_pl_checker(schedule):
    ''' L'idea del checker è di lockare tutto il prima possibile '''
    global has_something_to_lock
    global resources_needed
    global transactions_involved
    global who_is_locking

    # Step 1: Inizializzare le mappe
    for operation in (schedule):
        action,tr,resource = operation
                
        # RESOURCES_NEEDED
        if str(tr) not in resources_needed.keys():
            resources_needed[str(tr)]=[(resource,action)]
        else:
            resources_needed[str(tr)].append((resource,action))

        # TRANSACTIONS_INVOLVED     
        if resource not in transactions_involved.keys():
            transactions_involved[resource]=[(tr,action)]
        else:
            transactions_involved[resource].append((tr,action))
        
    # Step 2: Check and lock
    # invece di usare who_is_locking basarsi solo sul transaction_involved
    #print(f'\n OUT \nresources_needed: {resources_needed}, transaction_involved: {transactions_involved}')
    
    for operation in (schedule):
        action,tr,resource = operation

        # se posso eseguire l'azione -> nessuno ha il lock oppure la transazione che lo 
        #                               detiene può lasciarlo perché non ha alcuna altra
        #                               risorsa da lockare
        index = transactions_involved[resource].index((tr,action))
        # per ogni transazione che richiede la risorsa prima di lui controlla se ha qualcosa
        result = check_if_lock_available(tr, action, resource, index,loop=[tr])
        if not result:
            return False
        #print(f"Tr: {str(tr)}, Action: {action}, Resource: {resource}, R_Needed: {resources_needed[str(tr)]}")
        resources_needed[str(tr)].remove((resource, action))
        
        # Ho fatto tutto?
        if len(resources_needed[str(tr)]) == 0:
            clean_transaction_involved(tr)

    return True

schedule = schedules[1]
info = schedule.pop(0)
#print(f'Lo schedule: {schedule} è 2pl? \n {two_pl_checker(schedule)}')
       
for s in schedules:
    info = s.pop(0)
    print(f'Lo schedule: {s} è 2pl? \n {two_pl_checker(s)}')

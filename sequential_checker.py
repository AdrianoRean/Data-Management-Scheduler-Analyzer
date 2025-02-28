from two_pl_checker import *
import time
import conflict_checker as cc
import johnson as johnson
import view_checker as vc

#Assunzione che nessuna transazione legge o scrive due volte stesso elemento, nè legge dopo aver scritto.

schedule_to_analyze = 5
                
def sequential_checker(schedule):
    # the sequential checker doesn't do any preprocessing in parallel 
    # it checks the property one by one
    blind_write = None
    circuits = []
    info = schedule.pop(0)
    n_transactions = info[0]
    resources = info[1]

    # P2L checker
    if two_pl_checker(schedule):
        return "two_pl"
    # Conflict-equivalent checker
    conflicts = cc.parse(schedule, n_transactions, resources)
    conflict_lists = cc.create_conflict_list(conflicts, n_transactions)
    conflict_serializable = cc.check_conflict_serializability(conflict_lists, info[0])
    if conflict_serializable:
        return "conflict"
    is_blind = vc.parse(schedule,n_transactions,resources)
    vc.generate_serial([transaction for transaction in range (0, n_transactions)],[], n_transactions, resources)
    
    circuits = johnson.johnson(conflict_lists, n_transactions)
    blind_write = vc.check_if_cycles_are_blind(circuits, is_blind, n_transactions)
    if blind_write:
        view_serializability = vc.check_view_serializabilty(schedule)
    # View-serializability checker
    if view_serializability:
        return "view"

    return "None"

if __name__ == "__main__":
    time_results = {}
    for s in schedules:
        start_time = time.perf_counter() # Avvia il timer
        result = sequential_checker(s)
        end_time = time.perf_counter()  # Ferma il timer 
        delta = (end_time-start_time)
        if result in time_results:
            time_results[result]+= (delta)
        else:
            time_results[result] = (delta)
        print(f"Schedule: {s}\nResult: {result}\nElapsed time: {delta}\n*****************")




  

from parser import *
from checker import *
from johnson import johnson
from two_pl_checker import *
import time

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
    conflict_lists = create_conflict_list(conflicts, n_transactions)
    conflict_serializable = check_conflict_serializability(conflict_lists, info[0])
    if conflict_serializable:
        return "conflict"
    circuits = johnson(conflict_lists, n_transactions)
    blind_write = check_if_cycles_are_blind(circuits, is_blind, n_transactions)
    if blind_write:
        view_serializability = check_view_serializabilty(schedule)
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




  

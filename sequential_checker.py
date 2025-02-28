from two_pl_checker import TwoPLChecker
import time
from conflict_checker import ConflictChecker
from view_checker import ViewChecker
from schedules import *

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
    pl = TwoPLChecker()
    if pl.two_pl_checker(schedule):
        return "two_pl"
    # Conflict-equivalent checker
    cc = ConflictChecker()
    cc.parse(schedule, n_transactions, resources)
    cc.create_conflict_list(n_transactions)
    conflict_serializable = cc.check_conflict_serializability(info[0])
    if conflict_serializable:
        return "conflict"
    vc = ViewChecker(n_transactions)
    vc.parse(schedule,n_transactions,resources)
    vc.generate_serial([transaction for transaction in range (0, n_transactions)],[], n_transactions, resources)
    vc.johnson.johnson(cc.conflict_list)
    blind_write = vc.check_if_cycles_are_blind()
    if blind_write:
        view_serializability = vc.check_view_serializabilty()
    # View-serializability checker
    if view_serializability:
        return "view"

    return "None"

if __name__ == "__main__":
    time_results = {}
    #schedules = schedules[-3:-2]
    for s in conflict_schedules:
        start_time = time.perf_counter() # Avvia il timer
        result = sequential_checker(s)
        end_time = time.perf_counter()  # Ferma il timer 
        delta = (end_time-start_time)
        if result in time_results:
            time_results[result]+= (delta)
        else:
            time_results[result] = (delta)
        print(f"Schedule: {s}\nResult: {result}\nElapsed time: {delta}\n*****************")




  

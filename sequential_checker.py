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
    info = schedule.pop(0)
    n_transactions = info[0]
    resources = info[1]

    # P2L checker
    pl = TwoPLChecker(schedule)
    pl.parse()
    if pl.two_pl_checker():
        return "two_pl"
    # Conflict-equivalent checker
    cc = ConflictChecker(schedule,resources,n_transactions)
    cc.parse()
    cc.create_conflict_list()
    conflict_serializable = cc.check_conflict_serializability()
    if conflict_serializable:
        return "conflict"
    vc = ViewChecker(schedule, n_transactions, resources)
    vc.parse()
    if vc.is_blind:
        vc.generate_serial([transaction for transaction in range (0, n_transactions)])
        view_serializability = vc.check_view_serializabilty()
        if view_serializability:
            return "view"

    return "None"

if __name__ == "__main__":
    time_results = {}
    #schedules = schedules[-3:-2]
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




  

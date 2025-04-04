from modular_two_pl_checker import Modular_TwoPLChecker
import time
from conflict_checker import ConflictChecker
from view_checker import ViewChecker
from schedules import *
from parser_all import *

#Assunzione che nessuna transazione legge o scrive due volte stesso elemento, nè legge dopo aver scritto.

schedule_to_analyze = 5
                
def parallel_checker(schedule):
    # the sequential checker doesn't do any preprocessing in parallel 
    # it checks the property one by one
    info = schedule.pop(0)
    n_transactions = info[0]
    resources = info[1]

    parser = Parser(or_schedule=schedule, n_transactions=n_transactions, resources=resources)
    parser.parse()
    # P2L checker
    pl = Modular_TwoPLChecker(n_transactions, schedule, resources, {}, {})
    pl.parse()
    pl.parse_lock()
    if pl.two_pl_checker():
        return "two_pl"
    # Conflict-equivalent checker
    cc = ConflictChecker(schedule=schedule, resources=resources, n_transactions=n_transactions, remaining_conflicts=parser.remaining_conflicts, conflicts=parser.conflicts, resources_touched_transactions=parser.resources_touched_transactions)
    conflict_serializable = cc.check_conflict_serializability()
    if conflict_serializable:
        return "conflict"
    if parser.is_blind:
        vc = ViewChecker(schedule=schedule, n_transactions=n_transactions, resources=resources, is_blind=parser.is_blind, to_serial=parser.to_serial, read_from=parser.read_from, final_write=parser.final_write)
        view_serializability = vc.generate_and_check_serial([transaction for transaction in range (0, n_transactions)])
        if view_serializability:
            return "view"

    return "none"

if __name__ == "__main__":
    time_results = {}
    #schedules = schedules[-3:-2]
    for s in none_schedule[2:3]:
        start_time = time.perf_counter() # Avvia il timer
        result = parallel_checker(s)
        end_time = time.perf_counter()  # Ferma il timer 
        delta = (end_time-start_time)
        if result in time_results:
            time_results[result]+= (delta)
        else:
            time_results[result] = (delta)
        print(f"Schedule: {s}\nResult: {result}\nElapsed time: {delta}\n*****************")




  

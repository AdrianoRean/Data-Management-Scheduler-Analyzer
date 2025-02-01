from parser import *
from checker import *
from johnson import johnson

#Assunzione che nessuna transazione legge o scrive due volte stesso elemento, nè legge dopo aver scritto.

schedule_to_analyze = 5
                
def parse_check_and_create(schedule):
    blind_write = None
    circuits = []
    info = schedule.pop(0)
    n_transactions = info[0]
    resources = info[1]
    parse(schedule, n_transactions, resources)
    generate_serial([transaction for transaction in range (0, n_transactions)],[], n_transactions, resources)
    conflict_lists = create_conflict_list(conflicts, n_transactions)
    
    conflict_serializable = check_conflict_serializability(conflict_lists, info[0])
    if conflict_serializable:
        view_serializability = True
    else:
        circuits = johnson(conflict_lists, n_transactions)
        blind_write = check_if_cycles_are_blind(circuits, is_blind, n_transactions)
        if blind_write:
            view_serializability = check_view_serializabilty(read_from, final_write, serial_schedules)
        else:
            view_serializability = False
    
    with open(str(schedule_to_analyze) + '.json', 'w') as f:
        data = {
                "conflict_serializable" : conflict_serializable,
                "view_serializable" : view_serializability,
                "blind_write" : blind_write,
                "graph" : conflict_lists,
                "circuits" : circuits,
                "read_from" : read_from,
                "final_write" : final_write,
        }
        json.dump(data, f, indent=4)
        
    with open(str(schedule_to_analyze) + '_serials.json', 'w') as f:
        json.dump(serial_schedules, f, indent=4)
    
parse_check_and_create(schedules[schedule_to_analyze])
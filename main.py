from parser import *
from checker import *

schedule_to_analyze = 0
                
def parse_check_and_create(schedule):
    info = schedule.pop(0)
    n_transactions = info[0]
    elements = info[1]
    parse(schedule, n_transactions, elements)
    generate_serial([transaction for transaction in range (0, n_transactions)],[], n_transactions, elements)
    conflict_lists = create_conflict_list(conflicts, n_transactions)
    
    conflict_serializable = check_conflict_serializability(conflict_lists, info[0])
    if conflict_serializable:
        view_serializability = True
    else:
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
                "read_from" : read_from,
                "final_write" : final_write,
        }
        json.dump(data, f, indent=4)
        
    with open(str(schedule_to_analyze) + '_serials.json', 'w') as f:
        json.dump(serial_schedules, f, indent=4)
    
parse_check_and_create(schedules[schedule_to_analyze])
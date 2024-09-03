from parser import *
from checker import *

schedule_to_analyze = 3
                
def parse_check_and_create(schedule):
    info = schedule.pop(0)
    parse(schedule, info[0], info[1])
    generate_serial([str(transaction) for transaction in range (0, info[0])],[], info[0], info[1])
    
    conflict_serializable = check_conflict_serializability(transaction_graph,info[0])
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
                "graph" : transaction_graph,
                "read_from" : read_from,
                "final_write" : final_write,
        }
        json.dump(data, f, indent=4)
        
    with open(str(schedule_to_analyze) + '_serials.json', 'w') as f:
        json.dump(serial_schedules, f, indent=4)
    
parse_check_and_create(schedules[schedule_to_analyze])
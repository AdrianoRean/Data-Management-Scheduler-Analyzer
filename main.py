from parser import *
from checker import *
                
def parse_and_create(schedule):
    info = schedule.pop(0)
    parse(schedule, info[0], info[1])
    conflict_serializable = check_conflict_serializability(transaction_graph,info[0])
    generate_serial([str(transaction) for transaction in range (0, info[0])],[], info[0], info[1])
    
    with open(str(schedule_to_analyze) + '.json', 'w') as f:
        data = {
                "conflict_serializable" : conflict_serializable,
                "blind_write" : blind_write,
                "graph" : transaction_graph,
                "read_from" : read_from,
                "final_write" : final_write,
        }
        json.dump(data, f, indent=4)
        
    with open(str(schedule_to_analyze) + '_serials.json', 'w') as f:
        json.dump(serial_schedules, f, indent=4)
    
parse_and_create(schedules[schedule_to_analyze])